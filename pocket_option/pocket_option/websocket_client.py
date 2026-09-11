"""
Pocket Option WebSocket client.

Analytics/data only:
- connection
- authentication
- assets
- ticks
- history
- candles
- subscriptions

Automatic trading is intentionally NOT implemented.
"""

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


@dataclass
class PocketOptionTick:
    symbol: str
    timestamp: float
    price: float


@dataclass
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

    ВАЖНО:
    Этот класс используется только для получения рыночных данных.
    Открытие/закрытие сделок здесь отсутствует намеренно.
    """

    DEFAULT_WS_URL = (
        "wss://api-spb.po.market/socket.io/"
        "?EIO=4&transport=websocket"
    )

    DEFAULT_LANG = "ru"
    DEFAULT_CURRENT_URL = "cabinet/quick-high-low/USD"

    PING_INTERVAL = 25
    PING_TIMEOUT = 20

    RECONNECT_MIN = 1
    RECONNECT_MAX = 30

    MAX_TICKS = 5000
    MAX_HISTORY = 5000

    def __init__(
        self,
        ws_url: str | None = None,
        ssid: str | None = None,
        uid: str | None = None,
        tick_callback: TickCallback = None,
    ):
        self.ws_url = (
            ws_url
            or os.getenv("POCKET_OPTION_WS_URL")
            or self.DEFAULT_WS_URL
        )

        self.ssid = (
            ssid
            if ssid is not None
            else os.getenv("POCKET_OPTION_SSID", "").strip()
        )

        self.uid = (
            uid
            if uid is not None
            else os.getenv("POCKET_OPTION_UID", "").strip()
        )

        self.lang = os.getenv(
            "POCKET_OPTION_LANG",
            self.DEFAULT_LANG,
        )

        self.current_url = os.getenv(
            "POCKET_OPTION_CURRENT_URL",
            self.DEFAULT_CURRENT_URL,
        )

        self.is_chart = os.getenv(
            "POCKET_OPTION_IS_CHART",
            "1",
        ).lower() not in {
            "0",
            "false",
            "no",
            "off",
        }

        self.tick_callback = tick_callback

        self.session: aiohttp.ClientSession | None = None
        self.ws: aiohttp.ClientWebSocketResponse | None = None

        self._runner_task: asyncio.Task | None = None
        self._reader_task: asyncio.Task | None = None
        self._ping_task: asyncio.Task | None = None

        self._connection_lock = asyncio.Lock()

        self._connected = asyncio.Event()
        self._authenticated = asyncio.Event()

        # ВАЖНО:
        # Socket.IO namespace connection:
        # 40{"sid":"..."}
        #
        # AUTH отправляется только после установки этого события.
        self._socketio_connected = asyncio.Event()

        self._stop_requested = False

        self._last_error: str | None = None
        self._last_message_time: float | None = None
        self._last_message_preview: str | None = None

        self._socketio_sid: str | None = None
        self._engineio_sid: str | None = None

        self._ping_interval = self.PING_INTERVAL
        self._ping_timeout = self.PING_TIMEOUT

        self._reconnect_delay = self.RECONNECT_MIN

        self._subscriptions: dict[str, int] = {}

        self._ticks: dict[str, deque[PocketOptionTick]] = defaultdict(
            lambda: deque(maxlen=self.MAX_TICKS)
        )

        self._history: dict[str, deque[PocketOptionCandle]] = defaultdict(
            lambda: deque(maxlen=self.MAX_HISTORY)
        )

        self._assets: dict[str, Any] = {}

        self._history_waiters: dict[
            tuple[str, int],
            asyncio.Future,
        ] = {}

        self._raw_assets_payload: Any = None

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set() and self.ws is not None

    @property
    def is_authenticated(self) -> bool:
        return self._authenticated.is_set()

    @property
    def known_symbols(self) -> list[str]:
        symbols = set(self._ticks.keys())
        symbols.update(self._history.keys())
        symbols.update(self._assets.keys())
        return sorted(symbols)

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """
        Устанавливает WebSocket и выполняет Engine.IO / Socket.IO handshake.

        Строгая последовательность:

        1. WebSocket CONNECT
        2. receive 0{"sid":...}
        3. send 40
        4. receive 40{"sid":...}
        5. send AUTH
        6. reader начинает получать successauth и данные
        """

        async with self._connection_lock:
            if self.is_connected:
                return

            if not self.ssid:
                raise PocketOptionWebSocketError(
                    "POCKET_OPTION_SSID не задан."
                )

            if not self.uid:
                raise PocketOptionWebSocketError(
                    "POCKET_OPTION_UID не задан."
                )

            self._stop_requested = False
            self._connected.clear()
            self._authenticated.clear()
            self._socketio_connected.clear()

            self._last_error = None
            self._socketio_sid = None
            self._engineio_sid = None

            timeout = aiohttp.ClientTimeout(
                total=None,
                connect=20,
                sock_connect=20,
                sock_read=None,
            )

            headers = {
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

            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers=headers,
            )

            try:
                print(
                    f"[PO] Подключение WebSocket: {self.ws_url}"
                )

                self.ws = await self.session.ws_connect(
                    self.ws_url,
                    heartbeat=None,
                    autoping=False,
                    receive_timeout=None,
                )

                self._connected.set()

                print("[PO] WebSocket соединение установлено")

                # ------------------------------------------------------
                # КРИТИЧЕСКИЙ HANDSHAKE
                # ------------------------------------------------------

                await self._perform_engineio_handshake()

                # После получения 40{"sid":...}
                # только теперь отправляем AUTH.
                await self.send_auth()

                print("[PO] AUTH отправлен")

            except Exception:
                self._connected.clear()
                self._authenticated.clear()
                self._socketio_connected.clear()

                await self._close_socket()
                raise

    async def _perform_engineio_handshake(self) -> None:
        """
        Engine.IO + Socket.IO handshake.

        Ожидаем:

            0{"sid":"..."}

        отправляем:

            40

        затем ОБЯЗАТЕЛЬНО ждём:

            40{"sid":"..."}

        Только после этого connect() имеет право отправить AUTH.
        """

        if self.ws is None:
            raise PocketOptionWebSocketError(
                "WebSocket отсутствует во время handshake."
            )

        # --------------------------------------------------------------
        # STEP 1: Engine.IO OPEN
        # --------------------------------------------------------------

        print("[PO] Ожидание Engine.IO OPEN (0)...")

        message = await asyncio.wait_for(
            self.ws.receive(),
            timeout=20,
        )

        text = self._message_to_text(message)

        if not text:
            raise PocketOptionWebSocketError(
                "Pocket Option прислал пустой handshake."
            )

        print(f"[PO] ← {self._safe_preview(text)}")

        if not text.startswith("0"):
            raise PocketOptionWebSocketError(
                "Ожидался Engine.IO OPEN (0), "
                f"получено: {self._safe_preview(text)}"
            )

        self._parse_engineio_open(text)

        print("[PO] Engine.IO OPEN получен")

        # --------------------------------------------------------------
        # STEP 2: Socket.IO CONNECT request
        # --------------------------------------------------------------

        await self.ws.send_str("40")

        print("[PO] → 40")

        # --------------------------------------------------------------
        # STEP 3: ОБЯЗАТЕЛЬНО ждём Socket.IO CONNECT
        # --------------------------------------------------------------

        print(
            '[PO] Ожидание Socket.IO CONNECT '
            '(40{"sid":...})...'
        )

        deadline = time.monotonic() + 20.0

        while True:
            remaining = deadline - time.monotonic()

            if remaining <= 0:
                raise PocketOptionWebSocketError(
                    'Таймаут ожидания Socket.IO CONNECT '
                    '(40{"sid":...}).'
                )

            message = await asyncio.wait_for(
                self.ws.receive(),
                timeout=remaining,
            )

            text = self._message_to_text(message)

            if not text:
                continue

            print(f"[PO] ← {self._safe_preview(text)}")

            # Engine.IO ping может прийти прямо здесь.
            if text == "2":
                await self.ws.send_str("3")
                print("[PO] → 3 (pong)")
                continue

            # Ожидаемый Socket.IO CONNECT.
            if text.startswith("40"):
                self._parse_socketio_connect(text)

                self._socketio_connected.set()

                print(
                    "[PO] Socket.IO CONNECT подтверждён"
                )

                return

            # Ошибка сервера.
            if text.startswith("41"):
                raise PocketOptionWebSocketError(
                    "Pocket Option отклонил Socket.IO "
                    "подключение (41)."
                )

    def _parse_engineio_open(self, text: str) -> None:
        """Разбирает Engine.IO OPEN."""

        try:
            payload = json.loads(text[1:])

            self._engineio_sid = payload.get("sid")

            ping_interval_ms = payload.get("pingInterval")
            ping_timeout_ms = payload.get("pingTimeout")

            if ping_interval_ms:
                self._ping_interval = max(
                    5,
                    float(ping_interval_ms) / 1000.0,
                )

            if ping_timeout_ms:
                self._ping_timeout = max(
                    5,
                    float(ping_timeout_ms) / 1000.0,
                )

            print(
                "[PO] Engine.IO SID: "
                f"{self._engineio_sid}"
            )

            print(
                "[PO] pingInterval="
                f"{self._ping_interval:.1f}s "
                f"pingTimeout={self._ping_timeout:.1f}s"
            )

        except Exception as exc:
            raise PocketOptionWebSocketError(
                f"Ошибка разбора Engine.IO OPEN: {exc}"
            ) from exc

    def _parse_socketio_connect(self, text: str) -> None:
        """
        Разбирает:

            40{"sid":"..."}
        """

        payload_text = text[2:].strip()

        if not payload_text:
            return

        try:
            payload = json.loads(payload_text)

            if isinstance(payload, dict):
                self._socketio_sid = payload.get("sid")

        except json.JSONDecodeError:
            # Некоторые серверы могут прислать просто 40.
            pass

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    async def send_auth(self) -> None:
        """
        Отправляет AUTH после подтверждённого Socket.IO CONNECT.

        Здесь сохраняем текущий формат нашего клиента:
        sessionToken / uid / lang / currentUrl / isChart.

        Формат AUTH не смешиваем с чужими торговыми клиентами.
        """

        if self.ws is None:
            raise PocketOptionWebSocketError(
                "Невозможно отправить AUTH: WebSocket отсутствует."
            )

        if not self._socketio_connected.is_set():
            raise PocketOptionWebSocketError(
                "Невозможно отправить AUTH: "
                "Socket.IO CONNECT ещё не подтверждён."
            )

        payload = {
            "sessionToken": self.ssid,
            "uid": self._safe_uid(),
            "lang": self.lang,
            "currentUrl": self.current_url,
            "isChart": 1 if self.is_chart else 0,
        }

        packet = json.dumps(
            ["auth", payload],
            ensure_ascii=False,
            separators=(",", ":"),
        )

        packet = "42" + packet

        await self.ws.send_str(packet)

        # В лог НЕ выводим SSID.
        print(
            "[PO] → AUTH "
            f'(uid={payload["uid"]}, '
            f'lang={payload["lang"]}, '
            f'isChart={payload["isChart"]})'
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Запустить фоновый цикл подключения."""

        if self._runner_task and not self._runner_task.done():
            return

        self._stop_requested = False

        self._runner_task = asyncio.create_task(
            self.run_forever()
        )

    async def stop(self) -> None:
        """Остановить клиент."""

        self._stop_requested = True

        current = asyncio.current_task()

        if (
            self._runner_task
            and not self._runner_task.done()
            and self._runner_task is not current
        ):
            self._runner_task.cancel()

            try:
                await self._runner_task
            except asyncio.CancelledError:
                pass

        self._runner_task = None

        await self._close_socket()

    async def run_forever(self) -> None:
        """Поддерживать соединение и выполнять reconnect."""

        while not self._stop_requested:
            try:
                await self.connect()

                # Reader запускаем ПОСЛЕ:
                #
                # 0
                # 40
                # 40{"sid":...}
                # AUTH
                #
                # Это важно: во время handshake нет второго reader.
                reader_task = asyncio.create_task(
                    self.run()
                )

                self._reader_task = reader_task

                # Даем reader возможность обработать successauth.
                try:
                    await asyncio.wait_for(
                        self._authenticated.wait(),
                        timeout=20,
                    )

                    print(
                        "[PO] Аутентификация подтверждена "
                        "(successauth)"
                    )

                except asyncio.TimeoutError as exc:
                    raise PocketOptionWebSocketError(
                        "Pocket Option authentication timeout: "
                        "successauth не получен."
                    ) from exc

                self._reconnect_delay = self.RECONNECT_MIN

                # Восстанавливаем подписки после AUTH.
                await self._restore_subscriptions()

                # Пока reader работает — соединение живо.
                await reader_task

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                self._last_error = str(exc)

                print(
                    "[PO] Ошибка WebSocket: "
                    f"{type(exc).__name__}: {exc}"
                )

            finally:
                if self._reader_task:
                    if (
                        not self._reader_task.done()
                        and self._reader_task
                        is not asyncio.current_task()
                    ):
                        self._reader_task.cancel()

                    self._reader_task = None

                await self._close_socket()

            if self._stop_requested:
                break

            delay = self._reconnect_delay

            print(
                f"[PO] Reconnect через {delay:.1f} сек."
            )

            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise

            self._reconnect_delay = min(
                self.RECONNECT_MAX,
                max(
                    self.RECONNECT_MIN,
                    self._reconnect_delay * 2,
                ),
            )

    # ------------------------------------------------------------------
    # Reader
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Единственный постоянный reader WebSocket."""

        if self.ws is None:
            return

        try:
            async for message in self.ws:
                self._last_message_time = time.time()

                try:
                    await self._handle_message(message)
                except Exception as exc:
                    self._last_error = str(exc)

                    print(
                        "[PO] Ошибка обработки сообщения: "
                        f"{type(exc).__name__}: {exc}"
                    )

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            self._last_error = str(exc)

            print(
                "[PO] Reader завершён с ошибкой: "
                f"{type(exc).__name__}: {exc}"
            )

        finally:
            self._connected.clear()

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    async def _handle_message(self, message: Any) -> None:
        """Обработать входящий WebSocket message."""

        if isinstance(message, aiohttp.WSMessage):
            if message.type == aiohttp.WSMsgType.TEXT:
                text = message.data

            elif message.type == aiohttp.WSMsgType.BINARY:
                await self._handle_binary(
                    message.data
                )
                return

            elif message.type == aiohttp.WSMsgType.CLOSED:
                self._connected.clear()
                return

            elif message.type == aiohttp.WSMsgType.ERROR:
                self._last_error = str(
                    message.data
                )
                self._connected.clear()
                return

            else:
                return

        else:
            text = self._message_to_text(message)

        if not text:
            return

        self._last_message_preview = (
            self._safe_preview(text)
        )

        # --------------------------------------------------------------
        # Engine.IO heartbeat
        # --------------------------------------------------------------

        if text == "2":
            if self.ws is not None:
                await self.ws.send_str("3")

            return

        if text == "3":
            return

        # --------------------------------------------------------------
        # Engine.IO OPEN
        #
        # Обычно он обрабатывается connect(), но оставляем безопасную
        # обработку на случай reconnect/нестандартного сервера.
        # --------------------------------------------------------------

        if text.startswith("0"):
            return

        # --------------------------------------------------------------
        # Socket.IO CONNECT
        # --------------------------------------------------------------

        if text.startswith("40"):
            if not self._socketio_connected.is_set():
                self._parse_socketio_connect(text)
                self._socketio_connected.set()

            return

        # --------------------------------------------------------------
        # Socket.IO disconnect
        # --------------------------------------------------------------

        if text.startswith("41"):
            self._connected.clear()
            self._authenticated.clear()
            self._socketio_connected.clear()
            return

        # --------------------------------------------------------------
        # Socket.IO event
        # --------------------------------------------------------------

        if text.startswith("42"):
            await self._handle_socketio_event(
                text[2:]
            )
            return

        # --------------------------------------------------------------
        # Socket.IO binary event header
        # --------------------------------------------------------------

        if text.startswith("45"):
            # Binary attachments are handled separately.
            # Save header for diagnostics.
            return

    async def _handle_socketio_event(
        self,
        payload_text: str,
    ) -> None:
        """Разобрать Socket.IO 42[...] event."""

        try:
            data = json.loads(payload_text)

        except json.JSONDecodeError:
            self._last_error = (
                "Некорректный Socket.IO JSON"
            )
            return

        if not isinstance(data, list) or not data:
            return

        event = data[0]

        event_data = (
            data[1]
            if len(data) > 1
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

            print(
                "[PO] ← AUTH SUCCESS: "
                f"{event}"
            )

            return

        # --------------------------------------------------------------
        # AUTH ERROR
        # --------------------------------------------------------------

        if event in {
            "auth/fail",
            "auth/error",
            "NotAuthorized",
        }:
            self._authenticated.clear()

            self._last_error = (
                "Pocket Option отклонил авторизацию."
            )

            print(
                "[PO] ← AUTH ERROR: "
                f"{self._safe_preview(str(event_data))}"
            )

            return

        # --------------------------------------------------------------
        # ASSETS
        # --------------------------------------------------------------

        if event == "updateAssets":
            self._raw_assets_payload = event_data
            self._store_assets(event_data)
            return

        # --------------------------------------------------------------
        # STREAM / TICKS
        # --------------------------------------------------------------

        if event in {
            "updateStream",
            "updateCloseValue",
        }:
            await self._handle_stream_event(
                event_data
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
            "updateHistoryNew",
        }:
            await self._handle_history_event(
                event,
                event_data,
            )
            return

        # --------------------------------------------------------------
        # DISCONNECT
        # --------------------------------------------------------------

        if event == "disconnect":
            self._connected.clear()
            self._authenticated.clear()
            return

    # ------------------------------------------------------------------
    # Assets
    # ------------------------------------------------------------------

    def _store_assets(self, payload: Any) -> None:
        """Сохранить список инструментов."""

        self._assets.clear()

        if isinstance(payload, dict):
            assets = payload.get("assets")

            if isinstance(assets, dict):
                self._assets.update(assets)
                return

            if isinstance(assets, list):
                for item in assets:
                    self._store_single_asset(item)

                return

            self._assets.update(payload)
            return

        if isinstance(payload, list):
            for item in payload:
                self._store_single_asset(item)

    def _store_single_asset(self, item: Any) -> None:
        """Сохранить один инструмент."""

        if isinstance(item, dict):
            symbol = (
                item.get("symbol")
                or item.get("asset")
                or item.get("name")
            )

            if symbol:
                self._assets[str(symbol)] = item

    # ------------------------------------------------------------------
    # Stream / ticks
    # ------------------------------------------------------------------

    async def _handle_stream_event(
        self,
        payload: Any,
    ) -> None:
        """Обработать поток котировок."""

        for symbol, timestamp, price in self._extract_ticks(
            payload
        ):
            tick = PocketOptionTick(
                symbol=symbol,
                timestamp=timestamp,
                price=price,
            )

            self._ticks[symbol].append(tick)

            if self.tick_callback:
                try:
                    await self.tick_callback(
                        symbol,
                        timestamp,
                        price,
                    )
                except Exception as exc:
                    self._last_error = (
                        f"Tick callback error: {exc}"
                    )

    def _extract_ticks(
        self,
        payload: Any,
    ) -> list[tuple[str, float, float]]:
        """
        Извлечь ticks из разных форматов Pocket Option.
        """

        result: list[
            tuple[str, float, float]
        ] = []

        if isinstance(payload, dict):
            symbol = (
                payload.get("asset")
                or payload.get("symbol")
                or payload.get("pair")
            )

            if symbol:
                timestamp = (
                    payload.get("time")
                    or payload.get("timestamp")
                    or payload.get("at")
                    or time.time()
                )

                price = (
                    payload.get("price")
                    or payload.get("close")
                    or payload.get("value")
                )

                if price is not None:
                    try:
                        result.append(
                            (
                                str(symbol),
                                float(timestamp),
                                float(price),
                            )
                        )
                    except (
                        TypeError,
                        ValueError,
                    ):
                        pass

            # Иногда данные находятся внутри массивов.
            for key in (
                "data",
                "stream",
                "ticks",
                "prices",
            ):
                nested = payload.get(key)

                if nested is not None:
                    result.extend(
                        self._extract_ticks(nested)
                    )

            return result

        if isinstance(payload, list):
            # Формат:
            # [timestamp, price]
            if (
                len(payload) >= 2
                and self._is_number(payload[0])
                and self._is_number(payload[1])
            ):
                # Без symbol здесь невозможно безопасно
                # привязать tick.
                return result

            # Формат:
            # [symbol, timestamp, price]
            if (
                len(payload) >= 3
                and isinstance(payload[0], str)
                and self._is_number(payload[1])
                and self._is_number(payload[2])
            ):
                result.append(
                    (
                        payload[0],
                        float(payload[1]),
                        float(payload[2]),
                    )
                )
                return result

            for item in payload:
                result.extend(
                    self._extract_ticks(item)
                )

        return result

    # ------------------------------------------------------------------
    # History
    # ------------------------------------------------------------------

    async def _handle_history_event(
        self,
        event: str,
        payload: Any,
    ) -> None:
        """Обработать исторические свечи."""

        candles_by_symbol = (
            self._extract_candles(payload)
        )

        for symbol, candles in candles_by_symbol.items():
            if not candles:
                continue

            history = self._history[symbol]

            for candle in candles:
                history.append(candle)

            # Удаляем дубликаты по timestamp.
            unique: dict[
                int,
                PocketOptionCandle,
            ] = {}

            for candle in history:
                unique[candle.timestamp] = candle

            ordered = sorted(
                unique.values(),
                key=lambda item: item.timestamp,
            )

            history.clear()
            history.extend(
                ordered[-self.MAX_HISTORY:]
            )

            period = self._subscriptions.get(
                symbol,
                60,
            )

            key = (symbol, period)

            waiter = self._history_waiters.get(key)

            if waiter and not waiter.done():
                waiter.set_result(
                    list(history)
                )

    def _extract_candles(
        self,
        payload: Any,
    ) -> dict[
        str,
        list[PocketOptionCandle],
    ]:
        """Извлечь свечи из известных структур."""

        result: dict[
            str,
            list[PocketOptionCandle],
        ] = defaultdict(list)

        if isinstance(payload, dict):
            symbol = (
                payload.get("asset")
                or payload.get("symbol")
                or payload.get("pair")
            )

            for key in (
                "candles",
                "history",
                "data",
                "result",
            ):
                nested = payload.get(key)

                if nested is not None:
                    nested_result = (
                        self._extract_candles(nested)
                    )

                    for nested_symbol, candles in nested_result.items():
                        target = (
                            str(symbol)
                            if symbol
                            else nested_symbol
                        )

                        result[target].extend(
                            candles
                        )

            candle = self._parse_candle(payload)

            if candle and symbol:
                result[str(symbol)].append(candle)

            return result

        if isinstance(payload, list):
            # Одиночная свеча в массиве.
            candle = self._parse_candle(payload)

            if candle:
                # Без symbol невозможно надёжно
                # определить инструмент.
                return result

            for item in payload:
                nested_result = (
                    self._extract_candles(item)
                )

                for symbol, candles in nested_result.items():
                    result[symbol].extend(candles)

        return result

    def _parse_candle(
        self,
        value: Any,
    ) -> PocketOptionCandle | None:
        """Попытаться распознать свечу."""

        if isinstance(value, dict):
            timestamp = (
                value.get("time")
                or value.get("timestamp")
                or value.get("at")
            )

            open_price = value.get("open")
            high = value.get("high")
            low = value.get("low")
            close = value.get("close")

            if None in (
                timestamp,
                open_price,
                high,
                low,
                close,
            ):
                return None

            try:
                return PocketOptionCandle(
                    timestamp=int(float(timestamp)),
                    open=float(open_price),
                    high=float(high),
                    low=float(low),
                    close=float(close),
                    volume=float(
                        value.get("volume", 0.0)
                        or 0.0
                    ),
                )
            except (
                TypeError,
                ValueError,
            ):
                return None

        if isinstance(value, (list, tuple)):
            if len(value) < 5:
                return None

            try:
                return PocketOptionCandle(
                    timestamp=int(float(value[0])),
                    open=float(value[1]),
                    high=float(value[2]),
                    low=float(value[3]),
                    close=float(value[4]),
                    volume=(
                        float(value[5])
                        if len(value) > 5
                        else 0.0
                    ),
                )
            except (
                TypeError,
                ValueError,
            ):
                return None

        return None

    # ------------------------------------------------------------------
    # Subscribe
    # ------------------------------------------------------------------

    async def subscribe(
        self,
        symbol: str,
        period: int = 60,
    ) -> bool:
        """
        Подписаться на инструмент.

        Автоматического открытия сделок здесь нет.
        """

        await self._wait_until_authenticated()

        normalized = self.normalize_symbol(symbol)

        self._subscriptions[normalized] = int(period)

        await self._send_socketio(
            "changeSymbol",
            {
                "asset": normalized,
                "period": int(period),
            },
        )

        await self._send_socketio(
            "subfor",
            normalized,
        )

        print(
            f"[PO] Подписка: "
            f"{normalized} / {period}s"
        )

        return True

    async def request_history(
        self,
        symbol: str,
        period: int = 60,
        count: int = 500,
        timeout: float = 20.0,
    ) -> list[PocketOptionCandle]:
        """Запросить историю свечей."""

        await self._wait_until_authenticated()

        normalized = self.normalize_symbol(symbol)

        self._subscriptions[normalized] = int(period)

        await self.subscribe(
            normalized,
            period,
        )

        key = (
            normalized,
            int(period),
        )

        loop = asyncio.get_running_loop()

        waiter = loop.create_future()

        self._history_waiters[key] = waiter

        try:
            await self._send_socketio(
                "loadHistoryPeriod",
                {
                    "asset": normalized,
                    "index": 0,
                    "time": int(time.time()),
                    "offset": int(count),
                    "period": int(period),
                },
            )

            try:
                candles = await asyncio.wait_for(
                    waiter,
                    timeout=timeout,
                )

                return candles

            except asyncio.TimeoutError:
                # Если сервер уже прислал данные раньше,
                # используем кэш.
                cached = list(
                    self._history.get(
                        normalized,
                        [],
                    )
                )

                return cached[-count:]

        finally:
            current = self._history_waiters.get(key)

            if current is waiter:
                self._history_waiters.pop(
                    key,
                    None,
                )

    # ------------------------------------------------------------------
    # Socket.IO send
    # ------------------------------------------------------------------

    async def _send_socketio(
        self,
        event: str,
        data: Any = None,
    ) -> None:
        """Отправить Socket.IO event."""

        if self.ws is None:
            raise PocketOptionWebSocketError(
                "WebSocket не подключён."
            )

        if not self._authenticated.is_set():
            raise PocketOptionWebSocketError(
                "WebSocket ещё не аутентифицирован."
            )

        if data is None:
            packet = json.dumps(
                [event],
                ensure_ascii=False,
                separators=(",", ":"),
            )
        else:
            packet = json.dumps(
                [event, data],
                ensure_ascii=False,
                separators=(",", ":"),
            )

        await self.ws.send_str(
            "42" + packet
        )

    # ------------------------------------------------------------------
    # Restore subscriptions
    # ------------------------------------------------------------------

    async def _restore_subscriptions(self) -> None:
        """Восстановить подписки после reconnect."""

        if not self._subscriptions:
            return

        for symbol, period in list(
            self._subscriptions.items()
        ):
            try:
                await self.subscribe(
                    symbol,
                    period,
                )

            except Exception as exc:
                self._last_error = str(exc)

                print(
                    f"[PO] Не удалось восстановить "
                    f"подписку {symbol}: {exc}"
                )

    # ------------------------------------------------------------------
    # Authentication wait
    # ------------------------------------------------------------------

    async def _wait_until_authenticated(
        self,
        timeout: float = 20.0,
    ) -> None:
        """Дождаться подтверждённой авторизации."""

        if self._authenticated.is_set():
            return

        if not self._runner_task:
            await self.start()

        if not self.is_connected:
            await self.connect()

        try:
            await asyncio.wait_for(
                self._authenticated.wait(),
                timeout=timeout,
            )

        except asyncio.TimeoutError as exc:
            raise PocketOptionWebSocketError(
                "Ожидание authentication timeout: "
                "successauth не получен."
            ) from exc

    # ------------------------------------------------------------------
    # Ping
    # ------------------------------------------------------------------

    async def _ping_loop(self) -> None:
        """Engine.IO heartbeat."""

        while (
            not self._stop_requested
            and self.ws is not None
        ):
            try:
                await asyncio.sleep(
                    self._ping_interval
                )

                if (
                    self.ws
                    and not self.ws.closed
                ):
                    await self.ws.send_str("2")

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                self._last_error = str(exc)
                return

    # ------------------------------------------------------------------
    # Binary
    # ------------------------------------------------------------------

    async def _handle_binary(
        self,
        data: bytes,
    ) -> None:
        """
        Обработка binary frame.

        Пока сохраняем данные для диагностики.
        Конкретная структура бинарных attachment должна
        подтверждаться реальным трафиком Pocket Option.
        """

        self._last_message_preview = (
            f"<binary {len(data)} bytes>"
        )

    # ------------------------------------------------------------------
    # Close
    # ------------------------------------------------------------------

    async def _close_socket(self) -> None:
        """Закрыть WebSocket и HTTP session."""

        self._connected.clear()
        self._authenticated.clear()
        self._socketio_connected.clear()

        if self._ping_task:
            if not self._ping_task.done():
                self._ping_task.cancel()

            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass

            self._ping_task = None

        if self.ws is not None:
            try:
                await self.ws.close()
            except Exception:
                pass

            self.ws = None

        if self.session is not None:
            try:
                await self.session.close()
            except Exception:
                pass

            self.session = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _message_to_text(
        message: Any,
    ) -> str | None:
        """Преобразовать WebSocket message в text."""

        if isinstance(message, aiohttp.WSMessage):
            if message.type == aiohttp.WSMsgType.TEXT:
                return str(message.data)

            if message.type == aiohttp.WSMsgType.BINARY:
                try:
                    return bytes(
                        message.data
                    ).decode("utf-8")
                except UnicodeDecodeError:
                    return None

            return None

        if isinstance(message, str):
            return message

        if isinstance(
            message,
            (bytes, bytearray),
        ):
            try:
                return bytes(message).decode(
                    "utf-8"
                )
            except UnicodeDecodeError:
                return None

        if isinstance(message, memoryview):
            try:
                return bytes(message).decode(
                    "utf-8"
                )
            except UnicodeDecodeError:
                return None

        return None

    @staticmethod
    def _safe_preview(
        value: str,
        limit: int = 300,
    ) -> str:
        """Безопасный preview для логов."""

        if len(value) <= limit:
            return value

        return value[:limit] + "..."

    def _safe_uid(self) -> int | str:
        """Преобразовать UID в число, если возможно."""

        try:
            return int(self.uid)
        except (TypeError, ValueError):
            return self.uid

    @staticmethod
    def _is_number(value: Any) -> bool:
        try:
            float(value)
            return True
        except (
            TypeError,
            ValueError,
        ):
            return False

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        """
        Нормализация символа Pocket Option.

        Примеры:

        EUR/USD OTC -> EURUSD_otc
        EURUSD OTC  -> EURUSD_otc
        CHFNOK_otc  -> CHFNOK_otc
        """

        value = str(symbol).strip()

        value = value.replace(
            "/",
            "",
        )

        value = value.replace(
            " ",
            "",
        )

        lower = value.lower()

        if lower.endswith("_otc"):
            base = value[:-4]
            return base.upper() + "_otc"

        if lower.endswith("otc"):
            base = value[:-3]
            return base.upper() + "_otc"

        return value.upper()

    # ------------------------------------------------------------------
    # Data access
    # ------------------------------------------------------------------

    def get_ticks(
        self,
        symbol: str,
        limit: int = 100,
    ) -> list[PocketOptionTick]:
        """Получить последние ticks."""

        normalized = self.normalize_symbol(
            symbol
        )

        ticks = self._ticks.get(
            normalized,
            deque(),
        )

        return list(ticks)[-limit:]

    def get_history(
        self,
        symbol: str,
        limit: int = 500,
    ) -> list[PocketOptionCandle]:
        """Получить последние свечи."""

        normalized = self.normalize_symbol(
            symbol
        )

        candles = self._history.get(
            normalized,
            deque(),
        )

        return list(candles)[-limit:]

    def get_assets(self) -> dict[str, Any]:
        """Получить сохранённые инструменты."""

        return dict(self._assets)

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def status(self) -> dict[str, Any]:
        """Статус WebSocket клиента."""

        return {
            "started": (
                self._runner_task is not None
                and not self._runner_task.done()
            ),
            "connected": self.is_connected,
            "authenticated": self.is_authenticated,
            "socketio_connected": (
                self._socketio_connected.is_set()
            ),
            "engineio_sid": self._engineio_sid,
            "socketio_sid": self._socketio_sid,
            "known_symbols": len(
                self.known_symbols
            ),
            "assets": len(
                self._assets
            ),
            "subscriptions": dict(
                self._subscriptions
            ),
            "last_message_time": (
                self._last_message_time
            ),
            "last_message": (
                self._last_message_preview
            ),
            "last_error": self._last_error,
        }


# ----------------------------------------------------------------------
# Standalone diagnostic
# ----------------------------------------------------------------------

async def main() -> None:
    """
    Минимальная диагностика клиента.

    Автоматическая торговля отсутствует.
    """

    client = PocketOptionWebSocketClient()

    try:
        await client.start()

        print(
            "[PO] Ожидание authentication..."
        )

        await asyncio.wait_for(
            client._authenticated.wait(),
            timeout=30,
        )

        print(
            "[PO] Аутентификация подтверждена."
        )

        await asyncio.sleep(3)

        print(
            "[PO] Известные инструменты:",
            client.known_symbols[:20],
        )

        await client.subscribe(
            "EUR/USD OTC",
            60,
        )

        candles = await client.request_history(
            "EUR/USD OTC",
            60,
            500,
            20,
        )

        print(
            f"[PO] Получено свечей: "
            f"{len(candles)}"
        )

        while True:
            await asyncio.sleep(10)

            print(
                "[PO] STATUS:",
                json.dumps(
                    client.status(),
                    ensure_ascii=False,
                    default=str,
                ),
            )

    except asyncio.CancelledError:
        raise

    except Exception as exc:
        print(
            "[PO] Ошибка диагностики:",
            type(exc).__name__,
            str(exc),
        )

    finally:
        await client.stop()


if __name__ == "__main__":
    asyncio.run(main())
