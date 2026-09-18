"""
Pocket Option WebSocket client.

Источник рыночных данных:
    Pocket Option WebSocket / Socket.IO.

Автоматическая торговля отсутствует.

Клиент используется только для получения:
    - ticks
    - history
    - candles
    - assets
    - stream updates
"""

import asyncio
import json
import os
import time
from collections import deque
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
class PocketOptionCandle:
    """Свеча Pocket Option."""

    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }


class PocketOptionWebSocketClient:
    """
    Клиент Pocket Option WebSocket.

    Подтверждённый браузерный порядок:

        0{...}
        40
        40{"sid":"..."}
        42["auth",{
            "sessionToken":"...",
            "uid":"2249701",
            "lang":"ru",
            "currentUrl":"cabinet",
            "isChart":1
        }]
        42["auth/success"]

    Автоматическая торговля отсутствует.
    """

    DEFAULT_WS_URL = (
        "wss://api-spb.po.market/socket.io/"
        "?EIO=4&transport=websocket"
    )

    DEFAULT_LANG = "ru"

    # Значение, подтверждённое браузерным WebSocket.
    DEFAULT_CURRENT_URL = "cabinet"

    PING_INTERVAL = 25
    PING_TIMEOUT = 20

    RECONNECT_MIN = 1
    RECONNECT_MAX = 30

    MAX_TICKS = 5000
    MAX_HISTORY = 5000

    def __init__(
        self,
        ssid: str | None = None,
        uid: str | int | None = None,
        ws_url: str | None = None,
        lang: str | None = None,
        current_url: str | None = None,
        is_chart: bool = True,
        tick_callback: TickCallback = None,
    ) -> None:
        self.ssid = (
            ssid
            if ssid is not None
            else os.getenv(
                "POCKET_OPTION_SSID",
                "",
            )
        )

        self.uid = (
            uid
            if uid is not None
            else os.getenv(
                "POCKET_OPTION_UID",
                "",
            )
        )

        self.ws_url = (
            ws_url
            if ws_url is not None
            else os.getenv(
                "POCKET_OPTION_WS_URL",
                self.DEFAULT_WS_URL,
            )
        )

        self.lang = (
            lang
            if lang is not None
            else os.getenv(
                "POCKET_OPTION_LANG",
                self.DEFAULT_LANG,
            )
        )

        self.current_url = (
            current_url
            if current_url is not None
            else os.getenv(
                "POCKET_OPTION_CURRENT_URL",
                self.DEFAULT_CURRENT_URL,
            )
        )

        env_is_chart = os.getenv(
            "POCKET_OPTION_IS_CHART"
        )

        if env_is_chart is not None:
            self.is_chart = env_is_chart.lower() not in {
                "0",
                "false",
                "no",
                "off",
            }
        else:
            self.is_chart = bool(is_chart)

        self.tick_callback = tick_callback

        self.session: aiohttp.ClientSession | None = None
        self.ws: aiohttp.ClientWebSocketResponse | None = None

        self.reader_task: asyncio.Task[None] | None = None

        self.connected = False
        self.socketio_connected = False
        self.authenticated = False

        self.engine_sid: str | None = None
        self.socketio_sid: str | None = None

        self.last_error: str | None = None
        self.last_message: str | None = None

        self.ping_interval = self.PING_INTERVAL
        self.ping_timeout = self.PING_TIMEOUT

        self.auth_event = asyncio.Event()

        # Время отправки последней AUTH.
        # Используется только для диагностики ответа 41.
        self.auth_sent_at: float | None = None

        # Номер AUTH-попытки.
        self.auth_attempt = 0

        self.ticks: dict[
            str,
            deque[tuple[float, float]],
        ] = {}

        self.history: dict[
            tuple[str, int],
            list[PocketOptionCandle],
        ] = {}

        self.assets: list[Any] = []

        self.subscriptions: set[
            tuple[str, int]
        ] = set()

        self._history_waiters: dict[
            tuple[str, int],
            asyncio.Future[
                list[PocketOptionCandle]
            ],
        ] = {}

    # ================================================================
    # HELPERS
    # ================================================================

    def _get_uid(self) -> str:
        """UID Pocket Option передаётся как строка."""
        return str(self.uid).strip()

    @staticmethod
    def timeframe_to_seconds(
        timeframe: str,
    ) -> int:
        """Преобразует таймфрейм в секунды."""

        normalized = str(
            timeframe
        ).strip().lower()

        mapping = {
            "1m": 60,
            "3m": 180,
            "5m": 300,
            "10m": 600,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
            "2h": 7200,
            "4h": 14400,
            "6h": 21600,
            "12h": 43200,
            "1d": 86400,
        }

        if normalized in mapping:
            return mapping[normalized]

        if normalized.isdigit():
            value = int(normalized)

            if value > 0:
                return value

        raise ValueError(
            f"Unsupported timeframe: {timeframe}"
        )

    @staticmethod
    def normalize_symbol(
        symbol: str,
    ) -> str:
        """Преобразует символ в формат Pocket Option."""

        value = str(symbol).strip()

        if not value:
            raise ValueError(
                "Symbol is empty"
            )

        upper = value.upper()

        if upper.endswith(" OTC"):
            base = upper[:-4].replace(
                "/",
                "",
            )
            return f"{base}_otc"

        if upper.endswith("_OTC"):
            base = upper[:-4].replace(
                "/",
                "",
            )
            return f"{base}_otc"

        if upper.endswith("OTC"):
            base = upper[:-3].replace(
                "/",
                "",
            )
            return f"{base}_otc"

        return value

    # ================================================================
    # CONNECTION
    # ================================================================

    async def connect(self) -> None:
        """Подключается к Pocket Option WebSocket."""

        if self.connected:
            return

        if not self.ssid:
            raise PocketOptionWebSocketError(
                "POCKET_OPTION_SSID is not configured"
            )

        if not self.uid:
            raise PocketOptionWebSocketError(
                "POCKET_OPTION_UID is not configured"
            )

        # Очень важно:
        # новая попытка подключения не должна использовать
        # старое состояние успешной AUTH.
        self.auth_event.clear()
        self.authenticated = False
        self.socketio_connected = False
        self.connected = False
        self.auth_sent_at = None

        print(
            "[PO] Подключение к WebSocket: "
            f"{self.ws_url}"
        )

        headers = {
            "Origin": "https://pocketoption.com",
            "User-Agent": (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) "
                "AppleWebKit/605.1.15 "
                "(KHTML, like Gecko) Version/18.0 "
                "Mobile/15E148 Safari/604.1"
            ),
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

        self.session = aiohttp.ClientSession(
            headers=headers
        )

        try:
            self.ws = await self.session.ws_connect(
                self.ws_url,
                heartbeat=None,
                autoping=False,
                receive_timeout=None,
            )

            print(
                "[PO] Соединение WebSocket установлено"
            )

            await self._wait_engine_open()

            await self._send_socketio_connect()

            await self._wait_socketio_connect()

            self.connected = True

            self.reader_task = asyncio.create_task(
                self._reader_loop()
            )

            await asyncio.sleep(0)

            await self.send_auth()

        except Exception:
            await self._cleanup_connection()
            raise

    async def _wait_engine_open(
        self,
    ) -> None:
        """Ожидает Engine.IO OPEN."""

        if self.ws is None:
            raise PocketOptionWebSocketError(
                "WebSocket is not initialized"
            )

        print(
            "[PO] Ожидание Engine.IO OPEN (0)..."
        )

        while True:
            message = await self.ws.receive(
                timeout=self.PING_TIMEOUT
            )

            if message.type == aiohttp.WSMsgType.TEXT:
                data = message.data

                self.last_message = data

                print(
                    "[PO] ← "
                    f"{data}"
                )

                if data.startswith("0"):
                    try:
                        info = json.loads(
                            data[1:]
                        )
                    except json.JSONDecodeError:
                        info = {}

                    self.engine_sid = info.get(
                        "sid"
                    )

                    self.ping_interval = (
                        float(
                            info.get(
                                "pingInterval",
                                self.PING_INTERVAL
                                * 1000,
                            )
                        )
                        / 1000.0
                    )

                    self.ping_timeout = (
                        float(
                            info.get(
                                "pingTimeout",
                                self.PING_TIMEOUT
                                * 1000,
                            )
                        )
                        / 1000.0
                    )

                    print(
                        "[PO] Engine.IO SID: "
                        f"{self.engine_sid}"
                    )

                    print(
                        "[PO] pingInterval="
                        f"{self.ping_interval:.1f}s "
                        "pingTimeout="
                        f"{self.ping_timeout:.1f}s"
                    )

                    print(
                        "[PO] Получено Engine.IO OPEN"
                    )

                    return

            elif message.type in {
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.CLOSED,
                aiohttp.WSMsgType.ERROR,
            }:
                raise PocketOptionWebSocketError(
                    "WebSocket closed before "
                    "Engine.IO OPEN"
                )

    async def _send_socketio_connect(
        self,
    ) -> None:
        """Отправляет Socket.IO CONNECT."""

        if self.ws is None:
            raise PocketOptionWebSocketError(
                "WebSocket is not initialized"
            )

        await self.ws.send_str("40")

        print(
            "[PO] → 40"
        )

    async def _wait_socketio_connect(
        self,
    ) -> None:
        """Ожидает Socket.IO CONNECT."""

        if self.ws is None:
            raise PocketOptionWebSocketError(
                "WebSocket is not initialized"
            )

        print(
            "[PO] Ожидание Socket.IO CONNECT "
            "(40{\"sid\":\"...\"})..."
        )

        while True:
            message = await self.ws.receive(
                timeout=self.PING_TIMEOUT
            )

            if message.type == aiohttp.WSMsgType.TEXT:
                data = message.data

                self.last_message = data

                print(
                    "[PO] ← "
                    f"{data}"
                )

                if data.startswith("40"):
                    self.socketio_connected = True

                    payload = data[2:]

                    if payload:
                        try:
                            info = json.loads(
                                payload
                            )

                            if isinstance(
                                info,
                                dict,
                            ):
                                self.socketio_sid = (
                                    info.get(
                                        "sid"
                                    )
                                )

                        except json.JSONDecodeError:
                            pass

                    print(
                        "[PO] Socket.IO CONNECT "
                        "подтверждён"
                    )

                    return

                if data.startswith("41"):
                    raise PocketOptionWebSocketError(
                        "Socket.IO connection rejected"
                    )

            elif message.type in {
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.CLOSED,
                aiohttp.WSMsgType.ERROR,
            }:
                raise PocketOptionWebSocketError(
                    "WebSocket closed before "
                    "Socket.IO CONNECT"
                )

    # ================================================================
    # AUTHENTICATION
    # ================================================================

    async def send_auth(
        self,
    ) -> None:
        """Отправляет AUTH в формате браузера."""

        if self.ws is None:
            raise PocketOptionWebSocketError(
                "WebSocket is not initialized"
            )

        # Новая AUTH = новое ожидание.
        # Это исправляет использование старого Event
        # после предыдущего подключения.
        self.auth_event.clear()
        self.authenticated = False
        self.last_error = None

        self.auth_attempt += 1

        payload = {
            "sessionToken": self.ssid,
            "uid": self._get_uid(),
            "lang": self.lang,
            "currentUrl": self.current_url,
            "isChart": 1 if self.is_chart else 0,
        }

        debug_payload = dict(payload)

        token = str(
            debug_payload.get(
                "sessionToken",
                "",
            )
        )

        if len(token) > 8:
            debug_payload["sessionToken"] = (
                token[:4]
                + "..."
                + token[-4:]
            )
        elif token:
            debug_payload["sessionToken"] = "***"
        else:
            debug_payload["sessionToken"] = (
                "<empty>"
            )

        print(
            "[PO] AUTH DEBUG: "
            + json.dumps(
                [
                    "auth",
                    debug_payload,
                ],
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )

        print(
            "[PO] AUTH uid type: "
            f"{type(payload['uid']).__name__}"
        )

        packet = (
            "42"
            + json.dumps(
                [
                    "auth",
                    payload,
                ],
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )

        print(
            "[PO] AUTH PACKET LENGTH: "
            f"{len(packet)}"
        )

        # Запоминаем момент непосредственно перед отправкой.
        self.auth_sent_at = time.monotonic()

        await self.ws.send_str(packet)

        print(
            "[PO] → AUTH "
            f"(attempt={self.auth_attempt}, "
            f"uid={payload['uid']}, "
            f"lang={payload['lang']}, "
            f"isChart={payload['isChart']}, "
            f"currentUrl={payload['currentUrl']})"
        )

        print(
            "[PO] AUTH отправлен"
        )

    async def wait_authenticated(
        self,
        timeout: float = 20.0,
    ) -> None:
        """Ожидает успешную авторизацию."""

        if self.authenticated:
            return

        try:
            await asyncio.wait_for(
                self.auth_event.wait(),
                timeout=timeout,
            )

        except asyncio.TimeoutError as exc:
            raise PocketOptionWebSocketError(
                "authentication timeout: "
                "successauth не получен"
            ) from exc

        if not self.authenticated:
            raise PocketOptionWebSocketError(
                self.last_error
                or "Pocket Option authentication failed"
            )

    # ================================================================
    # READER
    # ================================================================

    async def _reader_loop(
        self,
    ) -> None:
        """Основной цикл чтения WebSocket."""

        if self.ws is None:
            return

        print(
            "[PO] Reader запущен"
        )

        try:
            async for message in self.ws:
                if message.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_message(
                        message.data
                    )

                elif message.type == aiohttp.WSMsgType.BINARY:
                    await self._handle_binary(
                        message.data
                    )

                elif message.type in {
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                }:
                    print(
                        "[PO] WebSocket закрыт"
                    )

                    print(
                        "[PO] WebSocket close_code="
                        f"{self.ws.close_code}"
                    )

                    break

                elif message.type == aiohttp.WSMsgType.ERROR:
                    error = self.ws.exception()

                    self.last_error = str(
                        error
                        or "WebSocket error"
                    )

                    print(
                        "[PO] WebSocket ERROR: "
                        f"{self.last_error}"
                    )

                    break

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            self.last_error = str(exc)

            print(
                "[PO] Reader error: "
                f"{exc}"
            )

        finally:
            self.connected = False
            self.socketio_connected = False
            self.authenticated = False

            print(
                "[PO] Reader остановлен"
            )

    async def _handle_message(
        self,
        data: str,
    ) -> None:
        """Обрабатывает Engine.IO/Socket.IO сообщение."""

        self.last_message = data

        print(
            "[PO] ← RAW: "
            f"{data}"
        )

        if data == "2":
            await self._send_engine_pong()
            return

        if data == "3":
            return

        if data.startswith("0"):
            return

        if data.startswith("40"):
            self.socketio_connected = True
            return

        if data.startswith("41"):
            self.socketio_connected = False
            self.authenticated = False

            elapsed_ms: float | None = None

            if self.auth_sent_at is not None:
                elapsed_ms = (
                    time.monotonic()
                    - self.auth_sent_at
                ) * 1000.0

            close_code: int | None = None
            close_reason: str | None = None
            ws_exception: str | None = None

            if self.ws is not None:
                close_code = self.ws.close_code

                try:
                    ws_exception_value = (
                        self.ws.exception()
                    )

                    if ws_exception_value is not None:
                        ws_exception = str(
                            ws_exception_value
                        )
                except Exception:
                    ws_exception = None

            elapsed_text = (
                f"{elapsed_ms:.1f} ms"
                if elapsed_ms is not None
                else "unknown"
            )

            self.last_error = (
                "Pocket Option Socket.IO "
                "DISCONNECT (41) after AUTH; "
                f"elapsed={elapsed_text}; "
                f"engine_sid={self.engine_sid}; "
                f"socketio_sid={self.socketio_sid}; "
                f"uid={self._get_uid()}; "
                f"currentUrl={self.current_url}; "
                f"isChart={1 if self.is_chart else 0}; "
                f"close_code={close_code}; "
                f"ws_exception={ws_exception}"
            )

            self.auth_event.set()

            print(
                "[PO] ← Socket.IO DISCONNECT (41)"
            )

            print(
                "[PO] AUTH DISCONNECT DIAGNOSTICS:"
            )

            print(
                "[PO]   auth_attempt="
                f"{self.auth_attempt}"
            )

            print(
                "[PO]   elapsed="
                f"{elapsed_text}"
            )

            print(
                "[PO]   engine_sid="
                f"{self.engine_sid}"
            )

            print(
                "[PO]   socketio_sid="
                f"{self.socketio_sid}"
            )

            print(
                "[PO]   uid="
                f"{self._get_uid()}"
            )

            print(
                "[PO]   currentUrl="
                f"{self.current_url}"
            )

            print(
                "[PO]   isChart="
                f"{1 if self.is_chart else 0}"
            )

            print(
                "[PO]   close_code="
                f"{close_code}"
            )

            print(
                "[PO]   ws_exception="
                f"{ws_exception}"
            )

            print(
                "[PO] AUTH DISCONNECT ERROR: "
                f"{self.last_error}"
            )

            return

        if data.startswith("42"):
            await self._handle_socketio_event(
                data[2:]
            )

    async def _send_engine_pong(
        self,
    ) -> None:
        """Отправляет Engine.IO pong."""

        if self.ws is None:
            return

        try:
            await self.ws.send_str("3")

            print(
                "[PO] → 3"
            )

        except Exception as exc:
            self.last_error = str(exc)

    async def _handle_socketio_event(
        self,
        payload: str,
    ) -> None:
        """Обрабатывает Socket.IO event."""

        try:
            event = json.loads(
                payload
            )

        except json.JSONDecodeError:
            print(
                "[PO] Некорректный "
                "Socket.IO JSON"
            )
            return

        if not isinstance(
            event,
            list,
        ) or not event:
            return

        event_name = event[0]

        event_data: Any = (
            event[1]
            if len(event) > 1
            else None
        )

        if event_name in {
            "auth/success",
            "successauth",
            "successAuth",
        }:
            self.authenticated = True
            self.auth_event.set()

            elapsed_ms: float | None = None

            if self.auth_sent_at is not None:
                elapsed_ms = (
                    time.monotonic()
                    - self.auth_sent_at
                ) * 1000.0

            if elapsed_ms is not None:
                print(
                    "[PO] AUTH SUCCESS "
                    f"after {elapsed_ms:.1f} ms"
                )
            else:
                print(
                    "[PO] AUTH SUCCESS"
                )

            return

        if event_name in {
            "auth/fail",
            "auth/error",
            "NotAuthorized",
        }:
            self.authenticated = False

            self.last_error = (
                "Authentication failed: "
                f"{event_data}"
            )

            self.auth_event.set()

            print(
                "[PO] AUTH ERROR: "
                f"{event_data}"
            )

            return

        if event_name == "updateAssets":
            self.assets = (
                event_data
                if isinstance(
                    event_data,
                    list,
                )
                else []
            )

            print(
                "[PO] Assets обновлены: "
                f"{len(self.assets)}"
            )

            return

        if event_name == "updateStream":
            await self._handle_update_stream(
                event_data
            )
            return

        if event_name == "updateCloseValue":
            await self._handle_update_close_value(
                event_data
            )
            return

        if event_name in {
            "updateHistoryNewFast",
            "updateHistory",
            "history",
            "history/success",
        }:
            await self._handle_history_event(
                event_name,
                event_data,
            )
            return

        if event_name in {
            "ping-server",
            "pong-server",
        }:
            return

    # ================================================================
    # TICKS
    # ================================================================

    async def _handle_update_stream(
        self,
        data: Any,
    ) -> None:
        """Обрабатывает updateStream."""

        items = (
            data
            if isinstance(
                data,
                list,
            )
            else [data]
        )

        for item in items:
            await self._process_tick_like_data(
                item
            )

    async def _handle_update_close_value(
        self,
        data: Any,
    ) -> None:
        """Обрабатывает updateCloseValue."""

        await self._process_tick_like_data(
            data
        )

    async def _process_tick_like_data(
        self,
        data: Any,
    ) -> None:
        """Извлекает tick из JSON."""

        if isinstance(data, dict):
            symbol = (
                data.get("asset")
                or data.get("symbol")
                or data.get("active")
            )

            price = (
                data.get("price")
                if data.get("price") is not None
                else data.get("value")
            )

            timestamp = (
                data.get("timestamp")
                if data.get("timestamp") is not None
                else data.get("time")
            )

            if (
                symbol is not None
                and price is not None
            ):
                try:
                    await self._store_tick(
                        str(symbol),
                        float(price),
                        float(
                            timestamp
                            if timestamp is not None
                            else time.time()
                        ),
                    )
                except (
                    TypeError,
                    ValueError,
                ):
                    pass

            return

        if (
            isinstance(data, list)
            and len(data) >= 2
        ):
            try:
                symbol = data[0]
                price = data[1]

                timestamp = (
                    data[2]
                    if len(data) >= 3
                    else time.time()
                )

                await self._store_tick(
                    str(symbol),
                    float(price),
                    float(timestamp),
                )

            except (
                TypeError,
                ValueError,
            ):
                pass

    async def _store_tick(
        self,
        symbol: str,
        price: float,
        timestamp: float,
    ) -> None:
        """Сохраняет tick."""

        if symbol not in self.ticks:
            self.ticks[symbol] = deque(
                maxlen=self.MAX_TICKS
            )

        self.ticks[symbol].append(
            (
                timestamp,
                price,
            )
        )

        if self.tick_callback is not None:
            try:
                await self.tick_callback(
                    symbol,
                    price,
                    timestamp,
                )

            except Exception as exc:
                print(
                    "[PO] Tick callback error: "
                    f"{exc}"
                )

    # ================================================================
    # BINARY
    # ================================================================

    async def _handle_binary(
        self,
        data: bytes,
    ) -> None:
        """Обрабатывает бинарное сообщение."""

        preview = data[:32].hex()

        print(
            "[PO] ← BINARY "
            f"len={len(data)} "
            f"preview={preview}"
        )

    # ================================================================
    # SUBSCRIPTION
    # ================================================================

    async def subscribe(
        self,
        symbol: str,
        period: int = 60,
    ) -> None:
        """Подписывается на инструмент."""

        await self.wait_authenticated()

        if self.ws is None:
            raise PocketOptionWebSocketError(
                "WebSocket is not initialized"
            )

        normalized = self.normalize_symbol(
            symbol
        )

        period = int(period)

        change_symbol = (
            "42"
            + json.dumps(
                [
                    "changeSymbol",
                    {
                        "asset": normalized,
                        "period": period,
                    },
                ],
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )

        await self.ws.send_str(
            change_symbol
        )

        print(
            "[PO] → changeSymbol "
            f"{normalized} "
            f"period={period}"
        )

        subfor = (
            "42"
            + json.dumps(
                [
                    "subfor",
                    normalized,
                ],
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )

        await self.ws.send_str(
            subfor
        )

        print(
            "[PO] → subfor "
            f"{normalized}"
        )

        self.subscriptions.add(
            (
                normalized,
                period,
            )
        )

    # ================================================================
    # HISTORY
    # ================================================================

    async def request_history(
        self,
        symbol: str,
        period: int = 60,
        count: int = 500,
        timeout: float = 20.0,
        offset: int | None = None,
    ) -> list[PocketOptionCandle]:
        """Запрашивает историю свечей."""

        await self.wait_authenticated(
            timeout=timeout
        )

        normalized = self.normalize_symbol(
            symbol
        )

        period = int(period)

        if offset is not None:
            count = int(offset)

        count = max(
            1,
            int(count),
        )

        await self.subscribe(
            normalized,
            period,
        )

        key = (
            normalized,
            period,
        )

        old_waiter = (
            self._history_waiters.get(
                key
            )
        )

        if (
            old_waiter is not None
            and not old_waiter.done()
        ):
            old_waiter.cancel()

        loop = asyncio.get_running_loop()

        waiter: asyncio.Future[
            list[PocketOptionCandle]
        ] = loop.create_future()

        self._history_waiters[key] = waiter

        payload = {
            "asset": normalized,
            "index": 0,
            "time": int(
                time.time()
            ),
            "offset": count,
            "period": period,
        }

        packet = (
            "42"
            + json.dumps(
                [
                    "loadHistoryPeriod",
                    payload,
                ],
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )

        if self.ws is None:
            self._history_waiters.pop(
                key,
                None,
            )

            raise PocketOptionWebSocketError(
                "WebSocket is not initialized"
            )

        await self.ws.send_str(
            packet
        )

        print(
            "[PO] → loadHistoryPeriod "
            f"asset={normalized} "
            f"period={period} "
            f"offset={count}"
        )

        try:
            result = await asyncio.wait_for(
                waiter,
                timeout=timeout,
            )

            return result

        except asyncio.TimeoutError:
            cached = self.history.get(
                key,
                [],
            )

            if cached:
                print(
                    "[PO] History timeout, "
                    f"используем cache: "
                    f"{len(cached)}"
                )

                return cached[-count:]

            raise PocketOptionWebSocketError(
                "History timeout: "
                f"{normalized} "
                f"period={period}"
            )

        finally:
            current = (
                self._history_waiters.get(
                    key
                )
            )

            if current is waiter:
                self._history_waiters.pop(
                    key,
                    None,
                )

    async def _handle_history_event(
        self,
        event_name: str,
        data: Any,
    ) -> None:
        """Обрабатывает историю свечей."""

        candles = self._extract_candles(
            data
        )

        if not candles:
            print(
                "[PO] History event без "
                f"свечей: {event_name}"
            )
            return

        symbol: str | None = None
        period: int | None = None

        if isinstance(data, dict):
            raw_symbol = (
                data.get("asset")
                or data.get("symbol")
                or data.get("active")
            )

            if raw_symbol is not None:
                symbol = self.normalize_symbol(
                    str(raw_symbol)
                )

            raw_period = (
                data.get("period")
                or data.get("timeframe")
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

        if symbol is None:
            for (
                candidate_symbol,
                candidate_period,
            ) in self.subscriptions:
                if (
                    period is None
                    or candidate_period == period
                ):
                    symbol = candidate_symbol
                    break

        if (
            period is None
            and symbol is not None
        ):
            candidates = [
                p
                for s, p in self.subscriptions
                if s == symbol
            ]

            if candidates:
                period = candidates[-1]

        if (
            symbol is None
            or period is None
        ):
            print(
                "[PO] History получена, "
                "но symbol/period "
                "не определены"
            )
            return

        key = (
            symbol,
            period,
        )

        self.history[key] = candles[
            -self.MAX_HISTORY:
        ]

        print(
            "[PO] History сохранена: "
            f"{symbol} "
            f"period={period} "
            f"candles={len(candles)}"
        )

        waiter = (
            self._history_waiters.get(
                key
            )
        )

        if (
            waiter is not None
            and not waiter.done()
        ):
            waiter.set_result(
                self.history[key]
            )

    @staticmethod
    def _extract_candles(
        data: Any,
    ) -> list[PocketOptionCandle]:
        """Извлекает свечи из JSON."""

        raw_items: Any = data

        if isinstance(data, dict):
            for key in (
                "history",
                "candles",
                "data",
                "result",
                "items",
                "values",
            ):
                if key in data:
                    raw_items = data[key]
                    break

        if isinstance(
            raw_items,
            dict,
        ):
            raw_items = [raw_items]

        if not isinstance(
            raw_items,
            list,
        ):
            return []

        result: list[
            PocketOptionCandle
        ] = []

        for item in raw_items:
            candle = (
                PocketOptionWebSocketClient
                ._parse_candle(item)
            )

            if candle is not None:
                result.append(
                    candle
                )

        result.sort(
            key=lambda candle:
                candle.timestamp
        )

        return result

    @staticmethod
    def _parse_candle(
        item: Any,
    ) -> PocketOptionCandle | None:
        """Преобразует один элемент в свечу."""

        if isinstance(item, dict):
            timestamp = (
                item.get("timestamp")
                if item.get(
                    "timestamp"
                ) is not None
                else item.get("time")
            )

            open_value = item.get(
                "open"
            )
            high_value = item.get(
                "high"
            )
            low_value = item.get(
                "low"
            )
            close_value = item.get(
                "close"
            )
            volume_value = item.get(
                "volume",
                0,
            )

            if any(
                value is None
                for value in (
                    timestamp,
                    open_value,
                    high_value,
                    low_value,
                    close_value,
                )
            ):
                return None

            try:
                return PocketOptionCandle(
                    timestamp=int(
                        float(timestamp)
                    ),
                    open=float(
                        open_value
                    ),
                    high=float(
                        high_value
                    ),
                    low=float(
                        low_value
                    ),
                    close=float(
                        close_value
                    ),
                    volume=float(
                        volume_value
                    ),
                )

            except (
                TypeError,
                ValueError,
            ):
                return None

        if isinstance(
            item,
            (list, tuple),
        ):
            if len(item) < 5:
                return None

            try:
                return PocketOptionCandle(
                    timestamp=int(
                        float(item[0])
                    ),
                    open=float(item[1]),
                    high=float(item[2]),
                    low=float(item[3]),
                    close=float(item[4]),
                    volume=(
                        float(item[5])
                        if len(item) > 5
                        else 0.0
                    ),
                )

            except (
                TypeError,
                ValueError,
            ):
                return None

        return None

    # ================================================================
    # PUBLIC DATA
    # ================================================================

    def get_ticks(
        self,
        symbol: str,
        limit: int = 100,
    ) -> list[
        tuple[float, float]
    ]:
        """Возвращает последние ticks."""

        normalized = self.normalize_symbol(
            symbol
        )

        values = list(
            self.ticks.get(
                normalized,
                [],
            )
        )

        return values[
            -int(limit):
        ]

    def get_history(
        self,
        symbol: str,
        period: int = 60,
        limit: int = 500,
    ) -> list[
        PocketOptionCandle
    ]:
        """Возвращает кэшированную историю."""

        normalized = self.normalize_symbol(
            symbol
        )

        values = self.history.get(
            (
                normalized,
                int(period),
            ),
            [],
        )

        return values[
            -int(limit):
        ]

    def get_assets(
        self,
    ) -> list[Any]:
        """Возвращает список активов."""
        return list(self.assets)

    # ================================================================
    # STATUS
    # ================================================================

    def status(
        self,
    ) -> dict[str, Any]:
        """Возвращает диагностический статус."""

        return {
            "connected": self.connected,
            "socketio_connected": (
                self.socketio_connected
            ),
            "authenticated": (
                self.authenticated
            ),
            "engine_sid": self.engine_sid,
            "socketio_sid": self.socketio_sid,
            "known_symbols": list(
                self.ticks.keys()
            ),
            "assets_count": len(
                self.assets
            ),
            "subscriptions": [
                {
                    "symbol": symbol,
                    "period": period,
                }
                for symbol, period
                in self.subscriptions
            ],
            "last_message": self.last_message,
            "last_error": self.last_error,
        }

    # ================================================================
    # START / STOP
    # ================================================================

    async def start(
        self,
    ) -> None:
        """Запускает WebSocket-клиент."""
        await self.connect()

    async def _cleanup_connection(
        self,
    ) -> None:
        """Безопасно закрывает текущее соединение."""

        if self.reader_task is not None:
            current = asyncio.current_task()

            if (
                self.reader_task is not current
                and not self.reader_task.done()
            ):
                self.reader_task.cancel()

                try:
                    await self.reader_task
                except asyncio.CancelledError:
                    pass

            self.reader_task = None

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

        self.connected = False
        self.socketio_connected = False
        self.authenticated = False

    async def stop(
        self,
    ) -> None:
        """Останавливает WebSocket-клиент."""

        print(
            "[PO] Остановка WebSocket клиента..."
        )

        await self._cleanup_connection()

        self.auth_event.clear()
        self.auth_sent_at = None

        print(
            "[PO] WebSocket клиент остановлен"
        )

    async def run_forever(
        self,
        reconnect: bool = True,
    ) -> None:
        """Запускает постоянное подключение."""

        delay = self.RECONNECT_MIN

        while True:
            try:
                await self.connect()

                delay = self.RECONNECT_MIN

                if self.reader_task is not None:
                    await self.reader_task
                else:
                    return

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                self.last_error = str(
                    exc
                )

                print(
                    "[PO] Ошибка WebSocket: "
                    f"{exc}"
                )

                if not reconnect:
                    raise

            finally:
                if not self.connected:
                    self.socketio_connected = False
                    self.authenticated = False

            if not reconnect:
                return

            print(
                "[PO] Повторное подключение через "
                f"{delay} сек."
            )

            await asyncio.sleep(delay)

            delay = min(
                delay * 2,
                self.RECONNECT_MAX,
            )


async def main() -> None:
    """Диагностический запуск."""

    client = PocketOptionWebSocketClient()

    try:
        await client.start()

        await client.wait_authenticated()

        print(
            "[PO] Клиент авторизован"
        )

        print(
            "[PO] STATUS:"
        )

        print(
            json.dumps(
                client.status(),
                ensure_ascii=False,
                indent=2,
            )
        )

        while True:
            await asyncio.sleep(60)

    except KeyboardInterrupt:
        pass

    finally:
        await client.stop()


if __name__ == "__main__":
    asyncio.run(main())
