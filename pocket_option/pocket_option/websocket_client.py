import asyncio
import json
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Awaitable, Callable
import aiohttp
class PocketOptionWebSocketError(Exception):
    """Ошибка Pocket Option WebSocket."""
TickCallback = Callable[
    [str, float, float],
    Awaitable[None],
] | None
@dataclass(frozen=True)
class PocketOptionTick:
    symbol: str
    timestamp: float
    price: float
@dataclass(frozen=True)
class PocketOptionCandle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
class PocketOptionWebSocketClient:
    """
    Асинхронный клиент Pocket Option WebSocket.
    Архитектура:
        Pocket Option WebSocket
                ↓
        Engine.IO / Socket.IO
                ↓
        authentication
                ↓
        historical data
                ↓
        realtime ticks
                ↓
        candle aggregation
                ↓
        MarketData / ForexService
    ВАЖНО:
    - клиент предназначен только для аналитики;
    - торговые операции отсутствуют;
    - нет buy/sell/order/TradeExecutor;
    - единственный reader WebSocket находится внутри run();
    - обработчики НЕ выполняют второй ws.receive()/ws.recv();
    - бинарные Socket.IO attachment'ы обрабатываются через основной reader.
    """
    DEFAULT_WS_URL = (
        "wss://api-spb.po.market/socket.io/"
        "?EIO=4&transport=websocket"
    )
    DEFAULT_LANG = "ru"
    DEFAULT_CURRENT_URL = "cabinet/quick-high-low/USD"
    ENGINE_PING_INTERVAL = 25.0
    ENGINE_PING_TIMEOUT = 20.0
    RECONNECT_MIN = 1.0
    RECONNECT_MAX = 30.0
    DEFAULT_TICK_BUFFER = 5000
    DEFAULT_HISTORY_BUFFER = 5000
    def __init__(
        self,
        ws_url: str | None = None,
        ssid: str | None = None,
        uid: str | None = None,
        tick_callback: TickCallback = None,
    ) -> None:
        self.ws_url = (
            ws_url
            or os.getenv("POCKET_OPTION_WS_URL")
            or self.DEFAULT_WS_URL
        )
        self.ssid = (
            ssid
            or os.getenv("POCKET_OPTION_SSID")
            or ""
        )
        self.uid = (
            str(uid)
            if uid is not None
            else os.getenv("POCKET_OPTION_UID", "")
        )
        self.lang = (
            os.getenv("POCKET_OPTION_LANG")
            or self.DEFAULT_LANG
        )
        self.current_url = (
            os.getenv("POCKET_OPTION_CURRENT_URL")
            or self.DEFAULT_CURRENT_URL
        )
        self.is_chart = self._env_bool(
            os.getenv("POCKET_OPTION_IS_CHART", "1")
        )
        self.tick_callback = tick_callback
        self.session: aiohttp.ClientSession | None = None
        self.ws: aiohttp.ClientWebSocketResponse | None = None
        self._runner_task: asyncio.Task[Any] | None = None
        self._stop_requested = False
        self._connected = asyncio.Event()
        self._authenticated = asyncio.Event()
        # Защита от одновременного connect() из run_forever()
        # и внешнего request_history()/subscribe().
        self._connection_lock = asyncio.Lock()
        # Исторические запросы выполняются последовательно.
        # Это важно для ответов, в которых сервер не возвращает
        # явный request-id / symbol в верхнем уровне payload.
        self._history_request_lock = asyncio.Lock()
        self._last_error: str | None = None
        self._last_message_at: float | None = None
        self._last_tick_at: float | None = None
        self._ticks: dict[
            str,
            deque[PocketOptionTick],
        ] = defaultdict(
            lambda: deque(
                maxlen=self.DEFAULT_TICK_BUFFER
            )
        )
        self._history: dict[
            tuple[str, int],
            deque[PocketOptionCandle],
        ] = defaultdict(
            lambda: deque(
                maxlen=self.DEFAULT_HISTORY_BUFFER
            )
        )
        # Binary Socket.IO state.
        self._pending_binary_event: dict[str, Any] | None = None
        self._binary_attachments: list[bytes] = []
        # History waiters.
        self._history_waiters: dict[
            tuple[str, int],
            list[asyncio.Future[list[PocketOptionCandle]]],
        ] = defaultdict(list)
        self._history_requests: dict[
            tuple[str, int],
            float,
        ] = {}
        # Контекст активного loadHistoryPeriod.
        # Нужен для raw-list history payload без asset/period.
        self._active_history_key: tuple[str, int] | None = None
        self._subscriptions: dict[
            str,
            set[int],
        ] = defaultdict(set)
        self._known_symbols: set[str] = set()
        # Пока Pocket Option не предоставил нам надёжный отдельный
        # server-time event, используем локальное время без смещения.
        self._server_time_offset = 0.0
    # ==========================================================================
    # ENV / CONFIG
    # ==========================================================================
    @staticmethod
    def _env_bool(value: str) -> bool:
        return str(value).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
    @property
    def is_connected(self) -> bool:
        return (
            self.ws is not None
            and not self.ws.closed
            and self._connected.is_set()
        )
    @property
    def is_authenticated(self) -> bool:
        return self._authenticated.is_set()
    @property
    def last_error(self) -> str | None:
        return self._last_error
    # ==========================================================================
    # SOCKET CONNECTION
    # ==========================================================================
    async def connect(self) -> None:
        """
        Установить WebSocket-соединение и выполнить Socket.IO auth.
        connect() НЕ ждёт auth/success.
        Авторизацию принимает основной reader run().
        """
        async with self._connection_lock:
            if self.is_connected:
                return
            if not self.ssid:
                raise PocketOptionWebSocketError(
                    "POCKET_OPTION_SSID is not configured."
                )
            if not self.uid:
                raise PocketOptionWebSocketError(
                    "POCKET_OPTION_UID is not configured."
                )
            self._stop_requested = False
            self._authenticated.clear()
            self._last_error = None
            if (
                self.session is None
                or self.session.closed
            ):
                self.session = aiohttp.ClientSession(
                    headers={
                        "Origin": "https://pocketoption.com",
                        "User-Agent": (
                            "Mozilla/5.0 (iPhone; CPU iPhone OS 18_3_1 "
                            "like Mac OS X) AppleWebKit/605.1.15 "
                            "(KHTML, like Gecko) Version/18.3 "
                            "Mobile/15E148 Safari/604.1"
                        ),
                        "Cache-Control": "no-cache",
                        "Pragma": "no-cache",
                    }
                )
            try:
                self.ws = await self.session.ws_connect(
                    self.ws_url,
                    heartbeat=None,
                    autoping=False,
                    receive_timeout=None,
                )
            except Exception as exc:
                self._last_error = (
                    "WebSocket connection failed: "
                    f"{type(exc).__name__}: {exc}"
                )
                raise PocketOptionWebSocketError(
                    self._last_error
                ) from exc
            self._connected.set()
            try:
                await self._perform_engineio_handshake()
                await self.send_auth()
            except Exception:
                await self._close_socket()
                raise
    async def _perform_engineio_handshake(self) -> None:
        """
        Engine.IO EIO=4 handshake.
        Ожидаем:
            0{"sid":...,"pingInterval":25000,...}
        Затем отправляем:
            40
        """
        if self.ws is None:
            raise PocketOptionWebSocketError(
                "WebSocket is not connected."
            )
        message = await self.ws.receive()
        if message.type != aiohttp.WSMsgType.TEXT:
            raise PocketOptionWebSocketError(
                "Invalid Engine.IO handshake frame."
            )
        text = str(message.data)
        if not text.startswith("0"):
            raise PocketOptionWebSocketError(
                "Unexpected Engine.IO handshake: "
                f"{text[:200]}"
            )
        try:
            payload = json.loads(text[1:])
        except json.JSONDecodeError as exc:
            raise PocketOptionWebSocketError(
                "Invalid Engine.IO handshake JSON."
            ) from exc
        if isinstance(payload, dict):
            ping_interval = payload.get("pingInterval")
            ping_timeout = payload.get("pingTimeout")
            if ping_interval:
                self.ENGINE_PING_INTERVAL = (
                    float(ping_interval) / 1000.0
                )
            if ping_timeout:
                self.ENGINE_PING_TIMEOUT = (
                    float(ping_timeout) / 1000.0
                )
        await self._send_text("40")
    # ==========================================================================
    # AUTH
    # ==========================================================================
    async def send_auth(self) -> None:
        """
        Browser-compatible Pocket Option auth.
        Наблюдаемый формат:
        42["auth",{
            "sessionToken":"...",
            "uid":"...",
            "lang":"ru",
            "currentUrl":"cabinet/quick-high-low/USD",
            "isChart":1
        }]
        """
        payload = {
            "sessionToken": self.ssid,
            "uid": self.uid,
            "lang": self.lang,
            "currentUrl": self.current_url,
            "isChart": 1 if self.is_chart else 0,
        }
        await self._send_socketio(
            "auth",
            payload,
        )
    # ==========================================================================
    # LOW LEVEL SEND
    # ==========================================================================
    async def _send_text(
        self,
        text: str,
    ) -> None:
        if (
            self.ws is None
            or self.ws.closed
        ):
            raise PocketOptionWebSocketError(
                "WebSocket is not connected."
            )
        await self.ws.send_str(text)
    async def _send_socketio(
        self,
        event: str,
        payload: Any = None,
    ) -> None:
        if payload is None:
            message = f'42["{event}"]'
        else:
            message = (
                "42"
                + json.dumps(
                    [event, payload],
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
        await self._send_text(message)
    # ==========================================================================
    # SUBSCRIPTION
    # ==========================================================================
    async def subscribe(
        self,
        symbol: str,
        period: int = 60,
    ) -> None:
        """
        Подписка на realtime поток.
        Pocket Option использует технический asset id,
        например:
            EUR/USD OTC -> EURUSD_otc
        """
        symbol = self.normalize_symbol(symbol)
        period = int(period)
        if period <= 0:
            raise ValueError(
                "period must be greater than zero."
            )
        await self._wait_until_authenticated()
        await self._send_socketio(
            "changeSymbol",
            {
                "asset": symbol,
                "period": period,
            },
        )
        await self._send_socketio(
            "subfor",
            symbol,
        )
        self._subscriptions[symbol].add(period)
        self._known_symbols.add(symbol)
    async def _restore_subscriptions(self) -> None:
        """
        Восстановить все известные подписки после reconnect.
        Используем snapshot, чтобы не изменять dictionary
        во время итерации.
        """
        subscriptions = [
            (
                symbol,
                period,
            )
            for symbol, periods
            in self._subscriptions.items()
            for period in sorted(periods)
        ]
        for symbol, period in subscriptions:
            if self._stop_requested:
                return
            try:
                await self._send_socketio(
                    "changeSymbol",
                    {
                        "asset": symbol,
                        "period": period,
                    },
                )
                await self._send_socketio(
                    "subfor",
                    symbol,
                )
                self._known_symbols.add(symbol)
            except Exception as exc:
                self._last_error = (
                    "Subscription restore failed for "
                    f"{symbol}/{period}: "
                    f"{type(exc).__name__}: {exc}"
                )
    # ==========================================================================
    # HISTORY REQUEST
    # ==========================================================================
    async def request_history(
        self,
        symbol: str,
        period: int = 60,
        *,
        offset: int = 500,
        timestamp: int | None = None,
        timeout: float = 15.0,
    ) -> list[PocketOptionCandle]:
        """
        Запрос исторических данных через loadHistoryPeriod.
        Наблюдаемый формат:
        42["loadHistoryPeriod",{
            "asset":"EURUSD_otc",
            "index":...,
            "time":...,
            "offset":500,
            "period":60
        }]
        Ответ может прийти:
        - обычным Socket.IO JSON;
        - binary Socket.IO attachment;
        - history в формате ticks;
        - candles;
        - raw list без symbol/period.
        Последний случай привязывается к активному
        loadHistoryPeriod request context.
        """
        symbol = self.normalize_symbol(symbol)
        period = int(period)
        if period <= 0:
            raise ValueError(
                "period must be greater than zero."
            )
        if offset <= 0:
            raise ValueError(
                "offset must be greater than zero."
            )
        if timeout <= 0:
            raise ValueError(
                "timeout must be greater than zero."
            )
        # Не допускаем несколько неизвестно-контекстных
        # loadHistoryPeriod одновременно.
        async with self._history_request_lock:
            await self._wait_until_authenticated()
            # Сохраняем subscription независимо от того,
            # была ли она создана ранее.
            await self.subscribe(
                symbol,
                period,
            )
            if timestamp is None:
                timestamp = int(
                    self.server_timestamp()
                )
            request = {
                "asset": symbol,
                "index": int(timestamp),
                "time": int(timestamp),
                "offset": int(offset),
                "period": period,
            }
            key = (
                symbol,
                period,
            )
            future: asyncio.Future[
                list[PocketOptionCandle]
            ] = (
                asyncio.get_running_loop().create_future()
            )
            self._history_waiters[key].append(
                future
            )
            self._history_requests[key] = (
                time.monotonic()
            )
            self._active_history_key = key
            try:
                await self._send_socketio(
                    "loadHistoryPeriod",
                    request,
                )
                try:
                    candles = await asyncio.wait_for(
                        future,
                        timeout=timeout,
                    )
                except asyncio.TimeoutError:
                    candles = self.get_cached_candles(
                        symbol,
                        period,
                        limit=offset,
                    )
                return candles
            finally:
                waiters = self._history_waiters.get(
                    key
                )
                if waiters and future in waiters:
                    waiters.remove(future)
                if not waiters:
                    self._history_waiters.pop(
                        key,
                        None,
                    )
                self._history_requests.pop(
                    key,
                    None,
                )
                if self._active_history_key == key:
                    self._active_history_key = None
    # ==========================================================================
    # MAIN READER
    # ==========================================================================
    async def run(self) -> None:
        """
        Единственный reader WebSocket.
        НИКАКИХ дополнительных ws.receive() внутри
        обработчиков сообщений.
        """
        if self.ws is None:
            raise PocketOptionWebSocketError(
                "WebSocket is not connected."
            )
        try:
            async for message in self.ws:
                self._last_message_at = time.time()
                if message.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_text(
                        str(message.data)
                    )
                elif message.type == aiohttp.WSMsgType.BINARY:
                    await self._handle_binary(
                        message.data
                    )
                elif message.type == aiohttp.WSMsgType.CLOSED:
                    break
                elif message.type == aiohttp.WSMsgType.ERROR:
                    raise PocketOptionWebSocketError(
                        "WebSocket error frame received."
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._last_error = (
                f"{type(exc).__name__}: {exc}"
            )
            raise
        finally:
            self._connected.clear()
            self._authenticated.clear()
    # ==========================================================================
    # TEXT FRAME HANDLING
    # ==========================================================================
    async def _handle_text(
        self,
        text: str,
    ) -> None:
        if not text:
            return
        # Engine.IO ping.
        if text == "2":
            try:
                await self._send_text("3")
            except Exception:
                pass
            return
        # Engine.IO pong.
        if text == "3":
            return
        # Socket.IO connection.
        # Это НЕ означает, что Pocket Option auth уже успешна.
        if text == "40":
            return
        # Иногда Socket.IO connection содержит sid:
        #
        # 40{"sid":"..."}
        #
        # Это тоже НЕ auth success.
        if text.startswith("40"):
            return
        # Socket.IO event.
        if text.startswith("42"):
            await self._handle_socketio_event(
                text[2:]
            )
            return
        # Socket.IO binary event header.
        if text.startswith("45"):
            await self._handle_binary_header(
                text
            )
            return
        # Engine.IO open/reconnect frame.
        if text.startswith("0"):
            return
    async def _handle_socketio_event(
        self,
        payload_text: str,
    ) -> None:
        try:
            payload = json.loads(
                payload_text
            )
        except json.JSONDecodeError:
            return
        if not isinstance(payload, list):
            return
        if not payload:
            return
        event = payload[0]
        if not isinstance(event, str):
            return
        data = (
            payload[1]
            if len(payload) > 1
            else None
        )
        # --------------------------------------------------------------
        # AUTH SUCCESS
        # --------------------------------------------------------------
        if event in {
            "auth/success",
            "successauth",
            "successAuth",
        }:
            self._authenticated.set()
            self._last_error = None
            return
        # --------------------------------------------------------------
        # AUTH FAILURE
        # --------------------------------------------------------------
        if event in {
            "auth/fail",
            "auth/error",
            "NotAuthorized",
        }:
            self._authenticated.clear()
            self._last_error = (
                "Pocket Option authentication failed: "
                f"{data}"
            )
            return
        # --------------------------------------------------------------
        # REALTIME STREAM
        # --------------------------------------------------------------
        if event in {
            "updateStream",
            "updateCloseValue",
        }:
            await self._handle_stream_payload(
                data
            )
            return
        # --------------------------------------------------------------
        # HISTORY
        # --------------------------------------------------------------
        if event in {
            "updateHistoryNewFast",
            "loadHistoryPeriod",
            "loadHistoryPeriodFast",
            "history",
        }:
            await self._handle_history_payload(
                data,
                event_name=event,
            )
            return
        # --------------------------------------------------------------
        # DISCONNECT
        # --------------------------------------------------------------
        if event == "disconnect":
            self._connected.clear()
            self._authenticated.clear()
            return
    # ==========================================================================
    # BINARY SOCKET.IO
    # ==========================================================================
    async def _handle_binary_header(
        self,
        text: str,
    ) -> None:
        """
        Пример:
            451-[...]
        где:
            45 = Engine.IO binary packet
            1  = Socket.IO attachment count
        """
        if not text.startswith("45"):
            return
        separator = text.find("-")
        if separator < 0:
            return
        try:
            attachment_count = int(
                text[2:separator]
            )
        except ValueError:
            return
        header_text = text[
            separator + 1:
        ]
        try:
            payload = json.loads(
                header_text
            )
        except json.JSONDecodeError:
            return
        event_name: str | None = None
        if (
            isinstance(payload, list)
            and payload
            and isinstance(payload[0], str)
        ):
            event_name = payload[0]
        self._pending_binary_event = {
            "payload": payload,
            "event": event_name,
            "attachments": attachment_count,
        }
        self._binary_attachments = []
        if attachment_count == 0:
            await self._process_binary_event()
    async def _handle_binary(
        self,
        data: bytes,
    ) -> None:
        if self._pending_binary_event is None:
            decoded = (
                self._decode_possible_binary_json(
                    data
                )
            )
            if decoded is not None:
                await self._handle_decoded_binary(
                    decoded
                )
            return
        self._binary_attachments.append(data)
        expected = int(
            self._pending_binary_event[
                "attachments"
            ]
        )
        if len(self._binary_attachments) >= expected:
            await self._process_binary_event()
    async def _process_binary_event(
        self,
    ) -> None:
        if self._pending_binary_event is None:
            return
        payload = self._pending_binary_event[
            "payload"
        ]
        event_name = self._pending_binary_event.get(
            "event"
        )
        attachments = self._binary_attachments
        self._pending_binary_event = None
        self._binary_attachments = []
        payload = self._replace_binary_placeholders(
            payload,
            attachments,
        )
        payload = self._decode_nested_bytes(
            payload
        )
        await self._handle_decoded_binary(
            payload,
            event_name=event_name,
        )
    @classmethod
    def _replace_binary_placeholders(
        cls,
        value: Any,
        attachments: list[bytes],
    ) -> Any:
        if isinstance(value, dict):
            if value.get("_placeholder") is True:
                number = value.get("num")
                if (
                    isinstance(number, int)
                    and 0 <= number < len(attachments)
                ):
                    return attachments[number]
                return value
            return {
                key: cls._replace_binary_placeholders(
                    item,
                    attachments,
                )
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [
                cls._replace_binary_placeholders(
                    item,
                    attachments,
                )
                for item in value
            ]
        return value
    @classmethod
    def _decode_nested_bytes(
        cls,
        value: Any,
    ) -> Any:
        """
        Рекурсивно декодирует attachment, если его содержимое
        является UTF-8 JSON.
        Если attachment не является JSON, исходные bytes
        сохраняются без изменений.
        """
        if isinstance(value, bytes):
            decoded = cls._decode_possible_binary_json(
                value
            )
            if decoded is not None:
                return cls._decode_nested_bytes(
                    decoded
                )
            return value
        if isinstance(value, dict):
            return {
                key: cls._decode_nested_bytes(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [
                cls._decode_nested_bytes(item)
                for item in value
            ]
        return value
    @staticmethod
    def _decode_possible_binary_json(
        data: bytes,
    ) -> Any | None:
        if not data:
            return None
        try:
            text = data.decode(
                "utf-8"
            )
        except UnicodeDecodeError:
            return None
        text = text.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return None
    async def _handle_decoded_binary(
        self,
        payload: Any,
        event_name: str | None = None,
    ) -> None:
        payload = self._decode_nested_bytes(
            payload
        )
        if isinstance(payload, list):
            if (
                len(payload) >= 2
                and isinstance(payload[0], str)
            ):
                event = payload[0]
                data = payload[1]
                if event in {
                    "updateStream",
                    "updateCloseValue",
                }:
                    await self._handle_stream_payload(
                        data
                    )
                    return
                if event in {
                    "updateHistoryNewFast",
                    "loadHistoryPeriod",
                    "loadHistoryPeriodFast",
                    "history",
                }:
                    await self._handle_history_payload(
                        data,
                        event_name=event,
                    )
                    return
        if event_name in {
            "updateStream",
            "updateCloseValue",
        }:
            await self._handle_stream_payload(
                payload
            )
            return
        await self._handle_history_payload(
            payload,
            event_name=event_name,
        )
    # ==========================================================================
    # STREAM PARSING
    # ==========================================================================
    async def _handle_stream_payload(
        self,
        payload: Any,
    ) -> None:
        payload = self._decode_nested_bytes(
            payload
        )
        ticks = self._extract_stream_ticks(
            payload
        )
        for tick in ticks:
            await self._store_tick(
                tick
            )
    def _extract_stream_ticks(
        self,
        payload: Any,
    ) -> list[PocketOptionTick]:
        result: list[PocketOptionTick] = []
        def add_item(item: Any) -> None:
            if (
                isinstance(item, list)
                and len(item) >= 3
                and isinstance(item[0], str)
            ):
                symbol = self.normalize_symbol(
                    item[0]
                )
                try:
                    timestamp = float(item[1])
                    price = float(item[2])
                except (
                    TypeError,
                    ValueError,
                ):
                    return
                if timestamp <= 0 or price <= 0:
                    return
                result.append(
                    PocketOptionTick(
                        symbol=symbol,
                        timestamp=timestamp,
                        price=price,
                    )
                )
        if isinstance(payload, list):
            for item in payload:
                if (
                    isinstance(item, list)
                    and len(item) >= 3
                    and isinstance(item[0], str)
                ):
                    add_item(item)
                elif isinstance(item, list):
                    for nested in item:
                        add_item(nested)
        elif isinstance(payload, dict):
            symbol = (
                payload.get("asset")
                or payload.get("symbol")
            )
            history = payload.get(
                "history"
            )
            if (
                isinstance(symbol, str)
                and isinstance(history, list)
            ):
                normalized_symbol = (
                    self.normalize_symbol(symbol)
                )
                for item in history:
                    if (
                        isinstance(item, list)
                        and len(item) >= 2
                    ):
                        try:
                            timestamp = float(
                                item[0]
                            )
                            price = float(
                                item[1]
                            )
                        except (
                            TypeError,
                            ValueError,
                        ):
                            continue
                        if (
                            timestamp > 0
                            and price > 0
                        ):
                            result.append(
                                PocketOptionTick(
                                    symbol=normalized_symbol,
                                    timestamp=timestamp,
                                    price=price,
                                )
                            )
        return result
    async def _store_tick(
        self,
        tick: PocketOptionTick,
    ) -> None:
        self._ticks[tick.symbol].append(
            tick
        )
        self._known_symbols.add(
            tick.symbol
        )
        self._last_tick_at = time.time()
        self._update_candles_from_tick(
            tick
        )
        if self.tick_callback is not None:
            try:
                await self.tick_callback(
                    tick.symbol,
                    tick.timestamp,
                    tick.price,
                )
            except Exception:
                # Callback не должен останавливать reader.
                pass
    # ==========================================================================
    # HISTORY PARSING
    # ==========================================================================
    async def _handle_history_payload(
        self,
        payload: Any,
        event_name: str | None = None,
    ) -> None:
        payload = self._decode_nested_bytes(
            payload
        )
        candles: list[PocketOptionCandle] = []
        symbol: str | None = None
        period: int | None = None
        # ------------------------------------------------------------------
        # DICT PAYLOAD
        # ------------------------------------------------------------------
        if isinstance(payload, dict):
            raw_symbol = (
                payload.get("asset")
                or payload.get("symbol")
            )
            if isinstance(raw_symbol, str):
                symbol = self.normalize_symbol(
                    raw_symbol
                )
            raw_period = (
                payload.get("period")
                or payload.get("timeframe")
            )
            if raw_period is not None:
                try:
                    period = int(
                        raw_period
                    )
                except (
                    TypeError,
                    ValueError,
                ):
                    period = None
            raw_candles = payload.get(
                "candles"
            )
            if isinstance(
                raw_candles,
                list,
            ):
                candles.extend(
                    self._parse_candle_array(
                        raw_candles,
                        period,
                    )
                )
            raw_history = payload.get(
                "history"
            )
            if isinstance(
                raw_history,
                list,
            ):
                candles.extend(
                    self._ticks_to_candles(
                        symbol,
                        raw_history,
                        period,
                    )
                )
        # ------------------------------------------------------------------
        # RAW LIST PAYLOAD
        # ------------------------------------------------------------------
        elif isinstance(payload, list):
            candles.extend(
                self._parse_candle_array(
                    payload,
                    period,
                )
            )
        candles = self._deduplicate_candles(
            candles
        )
        if not candles:
            return
        # ------------------------------------------------------------------
        # RESOLVE RAW-LIST CONTEXT
        # ------------------------------------------------------------------
        if symbol is None:
            if self._active_history_key is not None:
                symbol = self._active_history_key[0]
            elif event_name == "updateHistoryNewFast":
                # updateHistoryNewFast должен по возможности
                # содержать asset. Если его нет, не угадываем.
                return
        if symbol is None:
            return
        if period is None:
            if self._active_history_key is not None:
                period = self._active_history_key[1]
            else:
                period = self._infer_period(
                    symbol
                )
        if period <= 0:
            return
        key = (
            symbol,
            int(period),
        )
        # ------------------------------------------------------------------
        # MERGE INTO CACHE
        # ------------------------------------------------------------------
        existing = self._history[key]
        merged = {
            candle.timestamp: candle
            for candle in existing
        }
        for candle in candles:
            merged[candle.timestamp] = candle
        ordered = sorted(
            merged.values(),
            key=lambda candle: candle.timestamp,
        )
        existing.clear()
        existing.extend(
            ordered[
                -self.DEFAULT_HISTORY_BUFFER:
            ]
        )
        self._known_symbols.add(
            symbol
        )
        self._resolve_history_waiters(
            key,
            list(existing),
        )
    def _parse_candle_array(
        self,
        values: list[Any],
        period: int | None,
    ) -> list[PocketOptionCandle]:
        result: list[PocketOptionCandle] = []
        for item in values:
            if not isinstance(item, list):
                continue
            if len(item) < 5:
                continue
            try:
                timestamp = int(
                    float(item[0])
                )
                open_price = float(
                    item[1]
                )
                close_price = float(
                    item[2]
                )
                high_price = float(
                    item[3]
                )
                low_price = float(
                    item[4]
                )
                volume = (
                    float(item[5])
                    if len(item) > 5
                    else 0.0
                )
            except (
                TypeError,
                ValueError,
            ):
                continue
            if timestamp <= 0:
                continue
            if min(
                open_price,
                high_price,
                low_price,
                close_price,
            ) <= 0:
                continue
            high_price = max(
                high_price,
                open_price,
                close_price,
            )
            low_price = min(
                low_price,
                open_price,
                close_price,
            )
            result.append(
                PocketOptionCandle(
                    timestamp=timestamp,
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    volume=max(
                        volume,
                        0.0,
                    ),
                )
            )
        return result
    def _ticks_to_candles(
        self,
        symbol: str | None,
        values: list[Any],
        period: int | None,
    ) -> list[PocketOptionCandle]:
        if not symbol:
            return []
        if not period or period <= 0:
            period = 60
        ticks: list[
            PocketOptionTick
        ] = []
        for item in values:
            if (
                not isinstance(item, list)
                or len(item) < 2
            ):
                continue
            try:
                timestamp = float(
                    item[0]
                )
                price = float(
                    item[1]
                )
            except (
                TypeError,
                ValueError,
            ):
                continue
            if (
                timestamp <= 0
                or price <= 0
            ):
                continue
            ticks.append(
                PocketOptionTick(
                    symbol=symbol,
                    timestamp=timestamp,
                    price=price,
                )
            )
        return self._aggregate_ticks(
            ticks,
            period,
        )
    # ==========================================================================
    # CANDLE AGGREGATION
    # ==========================================================================
    def _update_candles_from_tick(
        self,
        tick: PocketOptionTick,
    ) -> None:
        periods = list(
            self._subscriptions.get(
                tick.symbol,
                set(),
            )
        )
        for period in periods:
            candle = self._aggregate_single_tick(
                tick,
                period,
            )
            if candle is None:
                continue
            key = (
                tick.symbol,
                period,
            )
            existing = self._history[key]
            # --------------------------------------------------------------
            # ВАЖНО:
            # Не заменяем текущую свечу новой свечой из одного тика.
            # Иначе high/low/open будут потеряны.
            # --------------------------------------------------------------
            if (
                existing
                and existing[-1].timestamp
                == candle.timestamp
            ):
                previous = existing[-1]
                existing[-1] = PocketOptionCandle(
                    timestamp=previous.timestamp,
                    open=previous.open,
                    high=max(
                        previous.high,
                        tick.price,
                    ),
                    low=min(
                        previous.low,
                        tick.price,
                    ),
                    close=tick.price,
                    volume=previous.volume,
                )
            else:
                existing.append(
                    candle
                )
    @staticmethod
    def _aggregate_single_tick(
        tick: PocketOptionTick,
        period: int,
    ) -> PocketOptionCandle | None:
        if period <= 0:
            return None
        bucket = (
            int(tick.timestamp)
            // period
        ) * period
        return PocketOptionCandle(
            timestamp=bucket,
            open=tick.price,
            high=tick.price,
            low=tick.price,
            close=tick.price,
            volume=0.0,
        )
    def _aggregate_ticks(
        self,
        ticks: list[PocketOptionTick],
        period: int,
    ) -> list[PocketOptionCandle]:
        if period <= 0:
            return []
        grouped: dict[
            int,
            list[PocketOptionTick],
        ] = defaultdict(list)
        for tick in sorted(
            ticks,
            key=lambda item: item.timestamp,
        ):
            bucket = (
                int(tick.timestamp)
                // period
            ) * period
            grouped[bucket].append(
                tick
            )
        candles: list[
            PocketOptionCandle
        ] = []
        for timestamp in sorted(
            grouped
        ):
            group = grouped[timestamp]
            if not group:
                continue
            prices = [
                tick.price
                for tick in group
            ]
            candles.append(
                PocketOptionCandle(
                    timestamp=timestamp,
                    open=prices[0],
                    high=max(prices),
                    low=min(prices),
                    close=prices[-1],
                    volume=0.0,
                )
            )
        return candles
    # ==========================================================================
    # HISTORY ACCESS
    # ==========================================================================
    def get_cached_candles(
        self,
        symbol: str,
        period: int = 60,
        limit: int = 500,
    ) -> list[PocketOptionCandle]:
        symbol = self.normalize_symbol(
            symbol
        )
        period = int(period)
        limit = int(limit)
        if period <= 0:
            raise ValueError(
                "period must be greater than zero."
            )
        if limit <= 0:
            return []
        key = (
            symbol,
            period,
        )
        candles = list(
            self._history.get(
                key,
                (),
            )
        )
        if not candles:
            ticks = list(
                self._ticks.get(
                    symbol,
                    (),
                )
            )
            if ticks:
                candles = self._aggregate_ticks(
                    ticks,
                    period,
                )
        return candles[-limit:]
    def get_ticks(
        self,
        symbol: str,
        limit: int = 100,
    ) -> list[PocketOptionTick]:
        symbol = self.normalize_symbol(
            symbol
        )
        limit = int(limit)
        if limit <= 0:
            return []
        ticks = list(
            self._ticks.get(
                symbol,
                (),
            )
        )
        return ticks[-limit:]
    def get_last_tick(
        self,
        symbol: str,
    ) -> PocketOptionTick | None:
        symbol = self.normalize_symbol(
            symbol
        )
        ticks = self._ticks.get(
            symbol
        )
        if not ticks:
            return None
        return ticks[-1]
    # ==========================================================================
    # WAITERS
    # ==========================================================================
    def _resolve_history_waiters(
        self,
        key: tuple[str, int],
        candles: list[PocketOptionCandle],
    ) -> None:
        waiters = self._history_waiters.get(
            key
        )
        if not waiters:
            return
        self._history_waiters[key] = []
        for future in waiters:
            if not future.done():
                future.set_result(
                    list(candles)
                )
    def _reject_history_waiters(
        self,
        error: Exception,
    ) -> None:
        for key, waiters in list(
            self._history_waiters.items()
        ):
            for future in waiters:
                if not future.done():
                    future.set_exception(
                        error
                    )
            self._history_waiters.pop(
                key,
                None,
            )
        self._active_history_key = None
        self._history_requests.clear()
    async def _wait_until_authenticated(
        self,
        timeout: float = 15.0,
    ) -> None:
        if self.is_authenticated:
            return
        # Поддерживаем безопасный вызов request_history()
        # даже если пользователь ещё не вызвал start().
        if (
            self._runner_task is None
            or self._runner_task.done()
        ):
            await self.start()
        if not self.is_connected:
            try:
                await self.connect()
            except Exception:
                # run_forever() может одновременно устанавливать
                # соединение. Повторно не поднимаем ошибку здесь,
                # если lifecycle уже продолжает работу.
                if not self.is_connected:
                    raise
        if self.is_authenticated:
            return
        try:
            await asyncio.wait_for(
                self._authenticated.wait(),
                timeout=timeout,
            )
        except asyncio.TimeoutError as exc:
            raise PocketOptionWebSocketError(
                "Pocket Option authentication timeout."
            ) from exc
    # ==========================================================================
    # RECONNECT LOOP
    # ==========================================================================
    async def run_forever(self) -> None:
        """
        Основной lifecycle клиента.
        При разрыве:
            disconnect
                ↓
            backoff
                ↓
            reconnect
                ↓
            Engine.IO handshake
                ↓
            Pocket Option auth
                ↓
            auth/success
                ↓
            restore subscriptions
                ↓
            realtime reader
        """
        reconnect_delay = self.RECONNECT_MIN
        while not self._stop_requested:
            try:
                await self.connect()
                # connect() только отправляет auth.
                # Фактический auth/success приходит через run().
                await asyncio.wait_for(
                    self._authenticated.wait(),
                    timeout=15.0,
                )
                reconnect_delay = self.RECONNECT_MIN
                await self._restore_subscriptions()
                await self.run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._last_error = (
                    f"{type(exc).__name__}: {exc}"
                )
            finally:
                self._connected.clear()
                self._authenticated.clear()
                if (
                    self.ws is not None
                    and self.ws.closed
                ):
                    self.ws = None
            if self._stop_requested:
                break
            await asyncio.sleep(
                reconnect_delay
            )
            reconnect_delay = min(
                reconnect_delay * 2.0,
                self.RECONNECT_MAX,
            )
    async def start(self) -> None:
        """
        Запустить background lifecycle.
        """
        if (
            self._runner_task is not None
            and not self._runner_task.done()
        ):
            return
        self._stop_requested = False
        self._runner_task = asyncio.create_task(
            self.run_forever()
        )
    async def stop(self) -> None:
        """
        Корректно остановить WebSocket,
        background lifecycle и pending requests.
        """
        self._stop_requested = True
        self._reject_history_waiters(
            PocketOptionWebSocketError(
                "Pocket Option WebSocket client stopped."
            )
        )
        if self._runner_task is not None:
            if (
                not self._runner_task.done()
                and self._runner_task
                is not asyncio.current_task()
            ):
                self._runner_task.cancel()
                try:
                    await self._runner_task
                except asyncio.CancelledError:
                    pass
            self._runner_task = None
        await self._close_socket()
        if (
            self.session is not None
            and not self.session.closed
        ):
            await self.session.close()
        self.session = None
    async def _close_socket(self) -> None:
        if self.ws is None:
            self._connected.clear()
            self._authenticated.clear()
            return
        ws = self.ws
        self.ws = None
        try:
            await ws.close()
        except Exception:
            pass
        self._connected.clear()
        self._authenticated.clear()
    # ==========================================================================
    # SYMBOL / TIMEFRAME HELPERS
    # ==========================================================================
    @staticmethod
    def normalize_symbol(
        symbol: str,
    ) -> str:
        """
        Преобразует UI-имя инструмента в технический
        Pocket Option identifier.
        Примеры:
            EUR/USD       -> EURUSD
            EUR/USD OTC   -> EURUSD_otc
            EURUSD_otc    -> EURUSD_otc
            CHF NOK_otc   -> CHFNOK_otc
        """
        value = str(symbol).strip()
        if not value:
            raise ValueError(
                "Symbol is required."
            )
        value = value.replace(
            " ",
            "",
        )
        otc = False
        upper = value.upper()
        if upper.endswith("_OTC"):
            otc = True
            value = value[:-4]
        elif upper.endswith("OTC"):
            otc = True
            value = value[:-3]
        value = value.replace(
            "/",
            "",
        )
        value = value.upper()
        if otc:
            return f"{value}_otc"
        return value
    @staticmethod
    def timeframe_to_seconds(
        timeframe: str | int,
    ) -> int:
        if isinstance(
            timeframe,
            int,
        ):
            if timeframe <= 0:
                raise ValueError(
                    "Invalid timeframe."
                )
            return timeframe
        value = str(
            timeframe
        ).strip().lower()
        mapping = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
            "4h": 14400,
            "1d": 86400,
        }
        if value not in mapping:
            raise ValueError(
                f"Unsupported timeframe: {timeframe}"
            )
        return mapping[value]
    # ==========================================================================
    # TIME
    # ==========================================================================
    def server_timestamp(self) -> float:
        """
        Возвращает текущее время для protocol requests.
        Пока отдельная надёжная server-time синхронизация
        не подтверждена, используется local time + offset.
        """
        return time.time() + self._server_time_offset
    # ==========================================================================
    # VALIDATION / DEDUPLICATION
    # ==========================================================================
    @staticmethod
    def _deduplicate_candles(
        candles: list[PocketOptionCandle],
    ) -> list[PocketOptionCandle]:
        unique: dict[
            int,
            PocketOptionCandle,
        ] = {}
        for candle in candles:
            unique[
                candle.timestamp
            ] = candle
        return [
            unique[timestamp]
            for timestamp in sorted(unique)
        ]
    def _infer_period(
        self,
        symbol: str,
    ) -> int:
        periods = self._subscriptions.get(
            symbol
        )
        if periods:
            return min(periods)
        return 60
    # ==========================================================================
    # STATUS
    # ==========================================================================
    def status(self) -> dict[str, Any]:
        started = (
            self._runner_task is not None
            and not self._runner_task.done()
        )
        return {
            "started": started,
            "connected": self.is_connected,
            "authenticated": self.is_authenticated,
            "analytics_only": True,
            "trade_execution": False,
            "known_symbols": sorted(
                self._known_symbols
            ),
            "subscriptions": {
                symbol: sorted(periods)
                for symbol, periods
                in self._subscriptions.items()
            },
            "last_message_at": (
                self._last_message_at
            ),
            "last_tick_at": (
                self._last_tick_at
            ),
            "last_error": self._last_error,
        }
# ==============================================================================
# DIAGNOSTIC ENTRY POINT
# ==============================================================================
async def main() -> None:
    """
    Локальная диагностика WebSocket.
    Никаких торговых операций.
    """
    client = PocketOptionWebSocketClient()
    try:
        await client.start()
        await asyncio.wait_for(
            client._authenticated.wait(),
            timeout=20.0,
        )
        print(
            "Pocket Option WebSocket: AUTHENTICATED"
        )
        await client.subscribe(
            "EURUSD_otc",
            60,
        )
        candles = await client.request_history(
            "EURUSD_otc",
            60,
            offset=500,
            timeout=20.0,
        )
        print(
            f"Historical candles: {len(candles)}"
        )
        while True:
            await asyncio.sleep(5)
            ticks = client.get_ticks(
                "EURUSD_otc",
                limit=5,
            )
            print(
                "Ticks:",
                [
                    (
                        tick.timestamp,
                        tick.price,
                    )
                    for tick in ticks
                ],
            )
            print(
                "Status:",
                client.status(),
            )
    finally:
        await client.stop()
if __name__ == "__main__":
    asyncio.run(main())
