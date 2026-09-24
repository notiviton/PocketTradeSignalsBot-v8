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
import email.utils
import hashlib
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

    После неожиданного DISCONNECT (41), закрытия WebSocket
    или ошибки reader клиент автоматически выполняет reconnect.
    """

    DEFAULT_WS_URL = (
        "wss://events-po.com/socket.io/"
        "?EIO=4&transport=websocket"
    )

    DEFAULT_LANG = "ru"
    DEFAULT_CURRENT_URL = "cabinet"

    PING_INTERVAL = 25
    PING_TIMEOUT = 20

    RECONNECT_MIN = 1
    RECONNECT_MAX = 30

    MAX_TICKS = 5000
    MAX_HISTORY = 5000

    DEFAULT_HISTORY_OFFSET = 9000

    MAX_BINARY_ATTACHMENTS = 10

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

        # ============================================================
        # CONFIG
        # ============================================================

        raw_ssid = (
            ssid
            if ssid is not None
            else os.getenv(
                "POCKET_OPTION_SSID",
                "",
            )
        )

        self.ssid = str(
            raw_ssid
            if raw_ssid is not None
            else ""
        ).strip()

        raw_uid = (
            uid
            if uid is not None
            else os.getenv(
                "POCKET_OPTION_UID",
                "",
            )
        )

        self.uid = str(
            raw_uid
            if raw_uid is not None
            else ""
        ).strip()

        raw_ws_url = (
            ws_url
            if ws_url is not None
            else os.getenv(
                "POCKET_OPTION_WS_URL",
                self.DEFAULT_WS_URL,
            )
        )

        self.ws_url = str(
            raw_ws_url
            if raw_ws_url is not None
            else self.DEFAULT_WS_URL
        ).strip()

        raw_lang = (
            lang
            if lang is not None
            else os.getenv(
                "POCKET_OPTION_LANG",
                self.DEFAULT_LANG,
            )
        )

        self.lang = str(
            raw_lang
            if raw_lang is not None
            else self.DEFAULT_LANG
        ).strip()

        raw_current_url = (
            current_url
            if current_url is not None
            else os.getenv(
                "POCKET_OPTION_CURRENT_URL",
                self.DEFAULT_CURRENT_URL,
            )
        )

        self.current_url = str(
            raw_current_url
            if raw_current_url is not None
            else self.DEFAULT_CURRENT_URL
        ).strip()

        if not self.current_url:
            self.current_url = self.DEFAULT_CURRENT_URL

        env_is_chart = os.getenv(
            "POCKET_OPTION_IS_CHART"
        )

        if env_is_chart is not None:
            self.is_chart = (
                env_is_chart.strip().lower()
                not in {
                    "0",
                    "false",
                    "no",
                    "off",
                }
            )
        else:
            self.is_chart = bool(is_chart)

        self.tick_callback = tick_callback

        # ============================================================
        # CONNECTION STATE
        # ============================================================

        self.session: aiohttp.ClientSession | None = None
        self.ws: aiohttp.ClientWebSocketResponse | None = None

        self.reader_task: asyncio.Task[None] | None = None
        self.lifecycle_task: asyncio.Task[None] | None = None

        self._connection_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()

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

        self.auth_sent_at: float | None = None
        self.auth_attempt = 0

        # ============================================================
        # SERVER TIME
        # ============================================================

        self.server_time_offset = 0.0
        self.server_time_synced = False

        # ============================================================
        # SOCKET.IO BINARY EVENT STATE
        # ============================================================

        self._binary_event: dict[str, Any] | None = None
        self._binary_attachments: dict[
            int,
            bytes,
        ] = {}

        # ============================================================
        # DATA
        # ============================================================

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

        # ============================================================
        # UPDATE STREAM DIAGNOSTICS
        #
        # Нужны только для определения того,
        # приходит ли после subscribe реальный updateStream.
        # ============================================================

        self.update_stream_count = 0
        self.last_update_stream_at: float | None = None
        self.last_update_stream_symbol: str | None = None
        self.last_update_stream_price: float | None = None
        self.last_update_stream_timestamp: float | None = None

        # ============================================================
        # HISTORY REQUEST CORRELATION
        #
        # ВАЖНО:
        # Каждый loadHistoryPeriod получает уникальный index.
        # Ответ history сопоставляется с waiter именно по index.
        # ============================================================

        self._history_request_index = 0

        self._history_waiters_by_index: dict[
            int,
            asyncio.Future[
                list[PocketOptionCandle]
            ],
        ] = {}

        self._history_request_meta: dict[
            int,
            tuple[str, int],
        ] = {}

    # ================================================================
    # SAFE DIAGNOSTICS
    # ================================================================

    def _ssid_fingerprint(self) -> str:
        """Безопасный SHA-256 отпечаток SSID."""

        if not self.ssid:
            return "<empty>"

        return hashlib.sha256(
            self.ssid.encode("utf-8")
        ).hexdigest()[:16]

    def _ssid_debug_info(self) -> dict[str, Any]:
        """Безопасная информация о SSID."""

        return {
            "length": len(self.ssid),
            "sha256_16": self._ssid_fingerprint(),
            "has_leading_space": (
                bool(self.ssid)
                and self.ssid[0].isspace()
            ),
            "has_trailing_space": (
                bool(self.ssid)
                and self.ssid[-1].isspace()
            ),
        }

    # ================================================================
    # HELPERS
    # ================================================================

    def _get_uid(self) -> str:
        """UID Pocket Option передаётся как строка."""

        return str(self.uid).strip()

    def _server_timestamp(self) -> int:
        """Возвращает текущий timestamp с учётом server offset."""

        return int(
            time.time()
            + self.server_time_offset
        )

    def _next_history_index(self) -> int:
        """
        Генерирует уникальный index для history-запроса.

        Pocket Option использует index для сопоставления
        loadHistoryPeriod с LoadHistoryPeriodResult.
        """

        self._history_request_index += 1

        if self._history_request_index >= 2**63:
            self._history_request_index = 1

        return self._history_request_index

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

    @staticmethod
    def _normalize_timestamp(
        value: Any,
    ) -> int | None:
        """Нормализует timestamp секунд/миллисекунд."""

        try:
            timestamp = float(value)
        except (
            TypeError,
            ValueError,
        ):
            return None

        if timestamp <= 0:
            return None

        # Миллисекунды.
        if timestamp > 100_000_000_000:
            timestamp /= 1000.0

        return int(timestamp)

    # ================================================================
    # SERVER TIME
    # ================================================================

    def _sync_time_from_headers(
        self,
        headers: Any,
    ) -> None:
        """
        Синхронизирует время по HTTP Date header.

        Это не меняет системные часы сервера.
        Мы только храним локальную поправку.
        """

        try:
            date_value = headers.get("Date")
        except Exception:
            date_value = None

        if not date_value:
            return

        try:
            parsed = email.utils.parsedate_to_datetime(
                date_value
            )

            server_timestamp = (
                parsed.timestamp()
            )

            local_timestamp = time.time()

            self.server_time_offset = (
                server_timestamp
                - local_timestamp
            )

            self.server_time_synced = True

            print(
                "[PO] Server time synced: "
                f"offset={self.server_time_offset:+.3f}s"
            )

        except Exception as exc:
            print(
                "[PO] Server time sync warning: "
                f"{exc}"
            )

    # ================================================================
    # CONNECTION
    # ================================================================

    async def connect(self) -> None:
        """
        Создаёт новое WebSocket-соединение.

        Старое соединение сначала полностью очищается.
        """

        async with self._connection_lock:

            if self._stop_event.is_set():
                raise PocketOptionWebSocketError(
                    "WebSocket client is stopped"
                )

            if (
                self.connected
                and self.socketio_connected
            ):
                return

            await self._cleanup_connection(
                preserve_lifecycle=True
            )

            self.ssid = str(
                self.ssid
            ).strip()

            self.uid = str(
                self.uid
            ).strip()

            self.lang = str(
                self.lang
            ).strip()

            self.current_url = str(
                self.current_url
            ).strip()

            if not self.ssid:
                raise PocketOptionWebSocketError(
                    "POCKET_OPTION_SSID is not configured"
                )

            if not self.uid:
                raise PocketOptionWebSocketError(
                    "POCKET_OPTION_UID is not configured"
                )

            if not self.ws_url:
                raise PocketOptionWebSocketError(
                    "POCKET_OPTION_WS_URL is not configured"
                )

            self.auth_event.clear()

            self.authenticated = False
            self.socketio_connected = False
            self.connected = False

            self.engine_sid = None
            self.socketio_sid = None

            self.auth_sent_at = None

            self._binary_event = None
            self._binary_attachments.clear()

            print(
                "[PO] Подключение к WebSocket: "
                f"{self.ws_url}"
            )

            print("[PO] CONFIG:")

            print(
                "[PO]   uid="
                f"{self._get_uid()}"
            )

            print(
                "[PO]   lang="
                f"{self.lang}"
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
                "[PO]   SSID length="
                f"{len(self.ssid)}"
            )

            print(
                "[PO]   SSID SHA256="
                f"{self._ssid_fingerprint()}"
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

                # aiohttp.ClientWebSocketResponse не предоставляет
                # публичного .headers. Не обращаемся к нему: серверное
                # время не должно влиять на установление WebSocket/AUTH.
                self.server_time_synced = False

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
                await self._cleanup_connection(
                    preserve_lifecycle=True
                )
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
                        "Socket.IO connection rejected "
                        "before AUTH"
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
        """Отправляет AUTH в современном браузерном формате."""

        if self.ws is None:
            raise PocketOptionWebSocketError(
                "WebSocket is not initialized"
            )

        if not self.socketio_connected:
            raise PocketOptionWebSocketError(
                "Socket.IO is not connected before AUTH"
            )

        self.ssid = str(
            self.ssid
        ).strip()

        if not self.ssid:
            raise PocketOptionWebSocketError(
                "POCKET_OPTION_SSID is empty before AUTH"
            )

        self.auth_event.clear()
        self.authenticated = False
        self.last_error = None

        self.auth_attempt += 1

        uid = self._get_uid()

        payload = {
            "sessionToken": self.ssid,
            "uid": uid,
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
            "[PO] AUTH SSID INFO: "
            + json.dumps(
                self._ssid_debug_info(),
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )

        print(
            "[PO] AUTH uid type: "
            f"{type(uid).__name__}"
        )

        socketio_payload = [
            "auth",
            payload,
        ]

        encoded_payload = json.dumps(
            socketio_payload,
            ensure_ascii=False,
            separators=(",", ":"),
        )

        packet = (
            "42"
            + encoded_payload
        )

        print(
            "[PO] AUTH PACKET LENGTH: "
            f"{len(packet)}"
        )

        print(
            "[PO] AUTH PACKET PREFIX: "
            f"{packet[:40]}"
        )

        self.auth_sent_at = time.monotonic()

        try:
            await self.ws.send_str(
                packet
            )
        except Exception as exc:
            self.last_error = (
                "AUTH send failed: "
                f"{exc}"
            )

            raise PocketOptionWebSocketError(
                self.last_error
            ) from exc

        print(
            "[PO] → AUTH "
            f"(attempt={self.auth_attempt}, "
            f"uid={uid}, "
            f"lang={self.lang}, "
            f"isChart={1 if self.is_chart else 0}, "
            f"currentUrl={self.current_url})"
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

        if (
            not self.connected
            or not self.socketio_connected
        ):
            raise PocketOptionWebSocketError(
                "WebSocket is not connected"
            )

        try:
            await asyncio.wait_for(
                self.auth_event.wait(),
                timeout=timeout,
            )

        except asyncio.TimeoutError as exc:
            self.last_error = (
                "authentication timeout: "
                "successauth не получен"
            )

            raise PocketOptionWebSocketError(
                self.last_error
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

            self._fail_history_waiters(
                "WebSocket connection lost"
            )

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
                f"ssid_length={len(self.ssid)}; "
                f"ssid_sha256={self._ssid_fingerprint()}; "
                f"auth_packet_state=sent; "
                f"close_code={close_code}; "
                f"ws_exception={ws_exception}"
            )

            self.auth_event.set()

            self._fail_history_waiters(
                "Pocket Option Socket.IO DISCONNECT (41)"
            )

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
                "[PO]   ssid_length="
                f"{len(self.ssid)}"
            )

            print(
                "[PO]   ssid_sha256="
                f"{self._ssid_fingerprint()}"
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

        if data.startswith("45"):
            await self._handle_binary_event_header(
                data
            )
            return

        if data.startswith("42"):
            await self._handle_socketio_event(
                data[2:]
            )
            return

        # ============================================================
        # RAW JSON
        #
        # ВАЖНО:
        # HistoryResult может приходить не как 42[...],
        # а как обычный JSON объект.
        # ============================================================

        try:
            raw = json.loads(data)
        except json.JSONDecodeError:
            return

        if isinstance(raw, dict):
            if (
                "index" in raw
                and (
                    "data" in raw
                    or "candles" in raw
                    or "history" in raw
                    or "result" in raw
                )
            ):
                print(
                    "[PO] ← RAW HISTORY RESULT"
                )

                await self._handle_history_result(
                    raw,
                    event_name="raw-json",
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

    # ================================================================
    # SOCKET.IO EVENT
    # ================================================================

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

        if not isinstance(
            event_name,
            str,
        ):
            return

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
            "loadHistoryPeriod",
            "loadHistoryPeriodFast",
        }:
            print(
                "[PO] History event: "
                f"{event_name}"
            )

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

        print(
            "[PO] Необработанный Socket.IO event: "
            f"{event_name}"
        )

        if isinstance(
            event_data,
            dict,
        ):
            print(
                "[PO] Event data keys: "
                f"{list(event_data.keys())[:30]}"
            )

    # ================================================================
    # BINARY SOCKET.IO EVENTS
    # ================================================================

    async def _handle_binary_event_header(
        self,
        data: str,
    ) -> None:
        """Обрабатывает Socket.IO BINARY_EVENT header."""

        try:
            separator = data.find("-")

            if separator < 0:
                print(
                    "[PO] Некорректный binary event header"
                )
                return

            packet_type_and_count = data[:separator]

            if not packet_type_and_count.startswith(
                "45"
            ):
                return

            attachments = int(
                packet_type_and_count[2:]
            )

            payload = data[
                separator + 1:
            ]

            event = json.loads(
                payload
            )

            if not isinstance(
                event,
                list,
            ) or not event:
                print(
                    "[PO] Binary event JSON "
                    "имеет неверный формат"
                )
                return

            self._binary_event = {
                "event": event,
                "attachments": attachments,
            }

            self._binary_attachments.clear()

            print(
                "[PO] ← BINARY EVENT header "
                f"attachments={attachments}"
            )

            if attachments <= 0:
                await self._dispatch_binary_event(
                    event
                )
                self._binary_event = None
                return

            if attachments > self.MAX_BINARY_ATTACHMENTS:
                print(
                    "[PO] Слишком много binary "
                    f"attachments: {attachments}"
                )

                self._binary_event = None
                self._binary_attachments.clear()

        except Exception as exc:
            print(
                "[PO] Binary event header error: "
                f"{exc}"
            )

            self._binary_event = None
            self._binary_attachments.clear()

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

        # ============================================================
        # RAW BINARY JSON
        #
        # В рабочей реализации history result может приходить
        # напрямую бинарным JSON без предварительного 451-header.
        # Раньше такой пакет просто терялся.
        # ============================================================

        if self._binary_event is None:
            try:
                decoded = data.decode(
                    "utf-8"
                ).strip()

                if decoded.startswith(
                    "42"
                ):
                    await self._handle_socketio_event(
                        decoded[2:]
                    )
                    return

                raw = json.loads(
                    decoded
                )

                if isinstance(
                    raw,
                    dict,
                ):
                    if (
                        "index" in raw
                        and (
                            "data" in raw
                            or "candles" in raw
                            or "history" in raw
                            or "result" in raw
                        )
                    ):
                        print(
                            "[PO] ← RAW BINARY "
                            "HISTORY RESULT"
                        )

                        await self._handle_history_result(
                            raw,
                            event_name="raw-binary-json",
                        )

                        return

            except (
                UnicodeDecodeError,
                json.JSONDecodeError,
            ):
                pass

            return

        attachments = int(
            self._binary_event.get(
                "attachments",
                0,
            )
        )

        index = len(
            self._binary_attachments
        )

        self._binary_attachments[
            index
        ] = data

        print(
            "[PO] Binary attachment "
            f"{index + 1}/{attachments} "
            f"получен"
        )

        if len(
            self._binary_attachments
        ) < attachments:
            return

        event = self._binary_event.get(
            "event"
        )

        if not isinstance(
            event,
            list,
        ):
            self._binary_event = None
            self._binary_attachments.clear()
            return

        try:
            reconstructed = (
                self._replace_binary_placeholders(
                    event
                )
            )

            await self._dispatch_binary_event(
                reconstructed
            )

        except Exception as exc:
            print(
                "[PO] Binary event dispatch error: "
                f"{exc}"
            )

        finally:
            self._binary_event = None
            self._binary_attachments.clear()

    def _replace_binary_placeholders(
        self,
        value: Any,
    ) -> Any:
        """Заменяет Socket.IO binary placeholders."""

        if isinstance(
            value,
            dict,
        ):
            if (
                value.get("_placeholder") is True
                and "num" in value
            ):
                try:
                    index = int(
                        value["num"]
                    )
                except (
                    TypeError,
                    ValueError,
                ):
                    return value

                return self._binary_attachments.get(
                    index,
                    b"",
                )

            return {
                key: self._replace_binary_placeholders(
                    item
                )
                for key, item in value.items()
            }

        if isinstance(
            value,
            list,
        ):
            return [
                self._replace_binary_placeholders(
                    item
                )
                for item in value
            ]

        return value

    async def _dispatch_binary_event(
        self,
        event: Any,
    ) -> None:
        """Передаёт восстановленный binary event обычному обработчику."""

        if not isinstance(
            event,
            list,
        ) or not event:
            return

        event_name = event[0]

        event_data = (
            event[1]
            if len(event) > 1
            else None
        )

        if not isinstance(
            event_name,
            str,
        ):
            return

        print(
            "[PO] Binary event decoded: "
            f"{event_name}"
        )

        if event_name in {
            "updateHistoryNewFast",
            "updateHistory",
            "history",
            "history/success",
            "loadHistoryPeriod",
            "loadHistoryPeriodFast",
        }:
            await self._handle_history_event(
                event_name,
                event_data,
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

        if isinstance(
            event_data,
            (dict, list),
        ):
            await self._handle_socketio_event(
                json.dumps(
                    event,
                    ensure_ascii=False,
                )
            )

    # ================================================================
    # TICKS
    # ================================================================

    async def _handle_update_stream(
        self,
        data: Any,
    ) -> None:
        """
        Обрабатывает updateStream.

        Диагностика показывает:
            - сколько updateStream пришло;
            - тип данных;
            - количество элементов;
            - содержимое каждого элемента;
            - какой symbol/price/timestamp удалось извлечь;
            - сколько ticks уже накоплено по EURUSD_otc.
        """

        self.update_stream_count += 1
        self.last_update_stream_at = time.monotonic()

        items = (
            data
            if isinstance(
                data,
                list,
            )
            else [data]
        )

        print(
            "[PO] UPDATE STREAM:"
        )

        print(
            "[PO]   count="
            f"{self.update_stream_count}"
        )

        print(
            "[PO]   data_type="
            f"{type(data).__name__}"
        )

        print(
            "[PO]   items="
            f"{len(items)}"
        )

        for item_index, item in enumerate(items, start=1):
            print(
                "[PO]   item["
                f"{item_index}"
                "]="
                f"{item!r}"
            )

            symbol: str | None = None
            price: float | None = None
            timestamp: float | None = None

            if isinstance(item, dict):
                raw_symbol = (
                    item.get("asset")
                    or item.get("symbol")
                    or item.get("active")
                )

                raw_price = (
                    item.get("price")
                    if item.get("price") is not None
                    else item.get("value")
                )

                raw_timestamp = (
                    item.get("timestamp")
                    if item.get("timestamp") is not None
                    else item.get("time")
                )

                if raw_symbol is not None:
                    symbol = str(raw_symbol)

                try:
                    if raw_price is not None:
                        price = float(raw_price)
                except (
                    TypeError,
                    ValueError,
                ):
                    price = None

                try:
                    if raw_timestamp is not None:
                        timestamp = float(raw_timestamp)
                except (
                    TypeError,
                    ValueError,
                ):
                    timestamp = None

            elif isinstance(item, list):
                if len(item) >= 1:
                    symbol = str(item[0])

                if len(item) >= 2:
                    try:
                        first = float(item[1])
                    except (
                        TypeError,
                        ValueError,
                    ):
                        first = None

                    second: float | None = None

                    if len(item) >= 3:
                        try:
                            second = float(item[2])
                        except (
                            TypeError,
                            ValueError,
                        ):
                            second = None

                    if first is not None:
                        if (
                            second is not None
                            and first > 1_000_000_000
                        ):
                            timestamp = first
                            price = second
                        else:
                            price = first

                            if second is not None:
                                timestamp = second

            if symbol is not None:
                self.last_update_stream_symbol = symbol

            if price is not None:
                self.last_update_stream_price = price

            if timestamp is not None:
                self.last_update_stream_timestamp = timestamp

            print(
                "[PO]   extracted:"
                f" symbol={symbol}"
                f" price={price}"
                f" timestamp={timestamp}"
            )

            await self._process_tick_like_data(
                item
            )

        normalized_eurusd = "EURUSD_otc"

        print(
            "[PO] UPDATE STREAM STATE:"
        )

        print(
            "[PO]   total="
            f"{self.update_stream_count}"
        )

        print(
            "[PO]   last_symbol="
            f"{self.last_update_stream_symbol}"
        )

        print(
            "[PO]   last_price="
            f"{self.last_update_stream_price}"
        )

        print(
            "[PO]   last_timestamp="
            f"{self.last_update_stream_timestamp}"
        )

        print(
            "[PO]   EURUSD_otc ticks="
            f"{len(self.ticks.get(normalized_eurusd, []))}"
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
                            else self._server_timestamp()
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

                first = float(
                    data[1]
                )

                if len(data) >= 3:
                    second = float(
                        data[2]
                    )

                    if first > 1_000_000_000:
                        timestamp = first
                        price = second
                    else:
                        price = first
                        timestamp = second
                else:
                    price = first
                    timestamp = self._server_timestamp()

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

        normalized = self.normalize_symbol(
            symbol
        )

        if normalized not in self.ticks:
            self.ticks[normalized] = deque(
                maxlen=self.MAX_TICKS
            )

        self.ticks[normalized].append(
            (
                timestamp,
                price,
            )
        )

        if self.tick_callback is not None:
            try:
                await self.tick_callback(
                    normalized,
                    price,
                    timestamp,
                )

            except Exception as exc:
                print(
                    "[PO] Tick callback error: "
                    f"{exc}"
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
        timeout: float = 30.0,
        offset: int | None = None,
    ) -> list[PocketOptionCandle]:
        """
        Запрашивает историю.

        count:
            Сколько свечей нужно вернуть вызывающему коду.

        offset:
            Сколько данных просить у сервера.

        Если offset не задан, используется
        DEFAULT_HISTORY_OFFSET.
        """

        await self.wait_authenticated(
            timeout=timeout
        )

        normalized = self.normalize_symbol(
            symbol
        )

        period = int(period)

        requested_count = max(
            1,
            int(count),
        )

        history_offset = (
            int(offset)
            if offset is not None
            else self.DEFAULT_HISTORY_OFFSET
        )

        history_offset = max(
            requested_count,
            history_offset,
        )

        key = (
            normalized,
            period,
        )

        # ============================================================
        # УНИКАЛЬНЫЙ REQUEST INDEX
        # ============================================================

        request_index = (
            self._next_history_index()
        )

        loop = asyncio.get_running_loop()

        waiter: asyncio.Future[
            list[PocketOptionCandle]
        ] = loop.create_future()

        self._history_waiters_by_index[
            request_index
        ] = waiter

        self._history_request_meta[
            request_index
        ] = (
            normalized,
            period,
        )

        try:
            # ========================================================
            # SUBSCRIBE
            # ========================================================

            await self.subscribe(
                normalized,
                period,
            )

            # ========================================================
            # ДИАГНОСТИЧЕСКАЯ ПАУЗА
            # ========================================================

            await asyncio.sleep(1.0)

            print(
                "[PO] После subscribe: "
                "пауза 1.0 сек завершена"
            )

            # ========================================================
            # UPDATE STREAM DIAGNOSTICS
            # ========================================================

            print(
                "[PO] UPDATE STREAM BEFORE HISTORY:"
            )

            print(
                "[PO]   update_stream_count="
                f"{self.update_stream_count}"
            )

            print(
                "[PO]   last_update_stream_at="
                f"{self.last_update_stream_at}"
            )

            print(
                "[PO]   last_update_stream_symbol="
                f"{self.last_update_stream_symbol}"
            )

            print(
                "[PO]   last_update_stream_price="
                f"{self.last_update_stream_price}"
            )

            print(
                "[PO]   last_update_stream_timestamp="
                f"{self.last_update_stream_timestamp}"
            )

            print(
                "[PO]   EURUSD_otc ticks="
                f"{len(self.ticks.get('EURUSD_otc', []))}"
            )

            # ========================================================
            # HISTORY REQUEST
            # ========================================================

            payload = {
                "asset": normalized,
                "index": request_index,
                "time": self._server_timestamp(),
                "offset": history_offset,
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
                raise PocketOptionWebSocketError(
                    "WebSocket is not initialized"
                )

            print(
                "[PO] HISTORY REQUEST:"
            )

            print(
                "[PO]   asset="
                f"{normalized}"
            )

            print(
                "[PO]   period="
                f"{period}"
            )

            print(
                "[PO]   requested_count="
                f"{requested_count}"
            )

            print(
                "[PO]   server_offset="
                f"{history_offset}"
            )

            print(
                "[PO]   server_time="
                f"{payload['time']}"
            )

            print(
                "[PO]   time_offset="
                f"{self.server_time_offset:+.3f}s"
            )

            print(
                "[PO]   request_index="
                f"{request_index}"
            )

            print(
                "[PO]   waiter=READY"
            )

            print(
                "[PO] → loadHistoryPeriod "
                f"asset={normalized} "
                f"period={period} "
                f"offset={history_offset} "
                f"index={request_index}"
            )

            print(
                "[PO] HISTORY WIRE PACKET:"
            )

            print(
                "[PO] "
                + packet
            )

            await self.ws.send_str(
                packet
            )

            print(
                "[PO] loadHistoryPeriod "
                "отправлен, ожидание history..."
            )

            # ========================================================
            # WAIT HISTORY
            # ========================================================

            result = await asyncio.wait_for(
                waiter,
                timeout=timeout,
            )

            if not result:
                print(
                    "[PO] History response пустой"
                )

                return []

            print(
                "[PO] History response получен: "
                f"{len(result)} candles "
                f"index={request_index}"
            )

            return result[
                -requested_count:
            ]

        except asyncio.TimeoutError:
            cached = self.history.get(
                key,
                [],
            )

            print(
                "[PO] HISTORY TIMEOUT:"
            )

            print(
                "[PO]   asset="
                f"{normalized}"
            )

            print(
                "[PO]   period="
                f"{period}"
            )

            print(
                "[PO]   requested_count="
                f"{requested_count}"
            )

            print(
                "[PO]   offset="
                f"{history_offset}"
            )

            print(
                "[PO]   request_index="
                f"{request_index}"
            )

            print(
                "[PO]   cached="
                f"{len(cached)}"
            )

            print(
                "[PO]   update_stream_count="
                f"{self.update_stream_count}"
            )

            print(
                "[PO]   last_update_stream_symbol="
                f"{self.last_update_stream_symbol}"
            )

            print(
                "[PO]   last_update_stream_price="
                f"{self.last_update_stream_price}"
            )

            print(
                "[PO]   last_update_stream_timestamp="
                f"{self.last_update_stream_timestamp}"
            )

            print(
                "[PO]   EURUSD_otc ticks="
                f"{len(self.ticks.get('EURUSD_otc', []))}"
            )

            if cached:
                print(
                    "[PO] Используем cache: "
                    f"{len(cached)}"
                )

                return cached[
                    -requested_count:
                ]

            raise PocketOptionWebSocketError(
                "History timeout: "
                f"{normalized} "
                f"period={period} "
                f"offset={history_offset} "
                f"index={request_index}"
            )

        except Exception:
            raise

        finally:
            self._history_waiters_by_index.pop(
                request_index,
                None
            )

            self._history_request_meta.pop(
                request_index,
                None
            )

    async def _handle_history_event(
        self,
        event_name: str,
        data: Any,
    ) -> None:
        """
        Обрабатывает history event.

        Основная корреляция выполняется по index.
        Если index отсутствует, используется осторожный
        fallback по symbol/period.
        """

        if not isinstance(
            data,
            (dict, list),
        ):
            print(
                "[PO] History event без "
                f"поддерживаемого data: {event_name}"
            )
            return

        if isinstance(
            data,
            dict,
        ):
            if "index" in data:
                await self._handle_history_result(
                    data,
                    event_name=event_name,
                )
                return

            # Иногда результат может быть вложен.
            for nested_key in (
                "result",
                "data",
            ):
                nested = data.get(
                    nested_key
                )

                if (
                    isinstance(
                        nested,
                        dict,
                    )
                    and "index" in nested
                ):
                    await self._handle_history_result(
                        nested,
                        event_name=event_name,
                    )
                    return

        # Старый / fallback-путь.
        candles = self._extract_candles(
            data,
            period=self._infer_period_from_data(
                data
            ),
        )

        if not candles:
            # Возможно, это ticks.
            raw_items = self._extract_history_items(
                data
            )

            period = self._infer_period_from_data(
                data
            )

            if raw_items and period is not None:
                candles = (
                    self._build_candles_from_history_items(
                        raw_items,
                        period,
                    )
                )

        if not candles:
            print(
                "[PO] History event без "
                f"свечей: {event_name}"
            )

            if isinstance(
                data,
                dict,
            ):
                print(
                    "[PO] History keys: "
                    f"{list(data.keys())[:50]}"
                )

            elif isinstance(
                data,
                list,
            ):
                print(
                    "[PO] History data type=list "
                    f"len={len(data)}"
                )

            return

        symbol, period = (
            self._infer_symbol_period(
                data
            )
        )

        if (
            symbol is None
            or period is None
        ):
            print(
                "[PO] History получена, "
                "но symbol/period "
                "не определены"
            )

            print(
                "[PO] event="
                f"{event_name}"
            )

            return

        key = (
            symbol,
            period,
        )

        self.history[key] = (
            candles[
                -self.MAX_HISTORY:
            ]
        )

        print(
            "[PO] History сохранена: "
            f"{symbol} "
            f"period={period} "
            f"candles={len(candles)} "
            f"event={event_name}"
        )

        self._resolve_legacy_history_waiter(
            symbol,
            period,
            self.history[key],
        )

    async def _handle_history_result(
        self,
        result: dict[str, Any],
        event_name: str,
    ) -> None:
        """
        Обрабатывает LoadHistoryPeriodResult.

        Поддерживаются:
            - OHLC candles
            - raw ticks {time, price}
            - вложенный result/data
        """

        raw_index = result.get(
            "index"
        )

        try:
            request_index = int(
                raw_index
            )
        except (
            TypeError,
            ValueError,
        ):
            request_index = None

        meta = (
            self._history_request_meta.get(
                request_index
            )
            if request_index is not None
            else None
        )

        if meta is None:
            print(
                "[PO] HISTORY RESULT "
                "с неизвестным index: "
                f"{raw_index}"
            )

        meta_symbol: str | None = None
        meta_period: int | None = None

        if meta is not None:
            meta_symbol, meta_period = meta

        symbol, period = (
            self._infer_symbol_period(
                result,
                fallback_symbol=meta_symbol,
                fallback_period=meta_period,
            )
        )

        if symbol is None:
            symbol = meta_symbol

        if period is None:
            period = meta_period

        if period is None:
            period = 60

        raw_items = self._extract_history_items(
            result
        )

        candles: list[
            PocketOptionCandle
        ] = []

        if raw_items:
            candles = (
                self._build_candles_from_history_items(
                    raw_items,
                    period,
                )
            )

        if not candles:
            candles = self._extract_candles(
                result,
                period=period,
            )

        if not candles:
            print(
                "[PO] HISTORY RESULT "
                "получен, но data не распознаны:"
            )

            print(
                "[PO]   index="
                f"{raw_index}"
            )

            print(
                "[PO]   event="
                f"{event_name}"
            )

            print(
                "[PO]   keys="
                f"{list(result.keys())[:50]}"
            )

            return

        if symbol is None:
            print(
                "[PO] HISTORY RESULT: "
                "symbol не определён"
            )
            return

        key = (
            symbol,
            period,
        )

        candles = self._merge_history_with_recent_ticks(
            symbol,
            period,
            candles,
        )

        candles = candles[
            -self.MAX_HISTORY:
        ]

        self.history[key] = candles

        print(
            "[PO] HISTORY RESULT OK:"
        )

        print(
            "[PO]   event="
            f"{event_name}"
        )

        print(
            "[PO]   index="
            f"{raw_index}"
        )

        print(
            "[PO]   asset="
            f"{symbol}"
        )

        print(
            "[PO]   period="
            f"{period}"
        )

        print(
            "[PO]   raw_items="
            f"{len(raw_items)}"
        )

        print(
            "[PO]   candles="
            f"{len(candles)}"
        )

        if request_index is not None:
            waiter = (
                self._history_waiters_by_index.get(
                    request_index
                )
            )

            if (
                waiter is not None
                and not waiter.done()
            ):
                waiter.set_result(
                    candles
                )

                print(
                    "[PO] HISTORY WAITER "
                    "RESOLVED:"
                    f" index={request_index}"
                )

        else:
            self._resolve_legacy_history_waiter(
                symbol,
                period,
                candles,
            )

    def _extract_history_items(
        self,
        data: Any,
    ) -> list[Any]:
        """Извлекает массив history data."""

        if isinstance(
            data,
            dict,
        ):
            # Основной формат:
            # {"index": ..., "data": [...]}
            for key in (
                "data",
                "candles",
                "history",
                "items",
                "values",
            ):
                candidate = data.get(
                    key
                )

                if isinstance(
                    candidate,
                    list,
                ):
                    return candidate

            nested_result = data.get(
                "result"
            )

            if isinstance(
                nested_result,
                dict,
            ):
                nested_items = (
                    self._extract_history_items(
                        nested_result
                    )
                )

                if nested_items:
                    return nested_items

            if isinstance(
                nested_result,
                list,
            ):
                return nested_result

            return []

        if isinstance(
            data,
            list,
        ):
            return data

        return []

    def _infer_symbol_period(
        self,
        data: Any,
        fallback_symbol: str | None = None,
        fallback_period: int | None = None,
    ) -> tuple[
        str | None,
        int | None,
    ]:
        """Определяет asset и period."""

        symbol: str | None = None
        period: int | None = fallback_period

        if isinstance(
            data,
            dict,
        ):
            raw_symbol = (
                data.get("asset")
                or data.get("symbol")
                or data.get("active")
            )

            if raw_symbol is not None:
                try:
                    symbol = self.normalize_symbol(
                        str(raw_symbol)
                    )
                except ValueError:
                    symbol = None

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
                    pass

            if symbol is None:
                nested_result = data.get(
                    "result"
                )

                if isinstance(
                    nested_result,
                    dict,
                ):
                    nested_symbol, nested_period = (
                        self._infer_symbol_period(
                            nested_result,
                            fallback_symbol=fallback_symbol,
                            fallback_period=period,
                        )
                    )

                    if nested_symbol is not None:
                        symbol = nested_symbol

                    if nested_period is not None:
                        period = nested_period

        if symbol is None:
            symbol = fallback_symbol

        if symbol is None:
            # Используем активную подписку как fallback.
            candidates = list(
                self.subscriptions
            )

            if candidates:
                if period is not None:
                    for (
                        candidate_symbol,
                        candidate_period,
                    ) in candidates:
                        if (
                            candidate_period
                            == period
                        ):
                            symbol = candidate_symbol
                            break

                if symbol is None:
                    symbol = candidates[-1][0]

        if (
            period is None
            and symbol is not None
        ):
            candidates = [
                candidate_period
                for (
                    candidate_symbol,
                    candidate_period,
                ) in self.subscriptions
                if candidate_symbol == symbol
            ]

            if candidates:
                period = candidates[-1]

        return (
            symbol,
            period,
        )

    def _infer_period_from_data(
        self,
        data: Any,
    ) -> int | None:
        """Определяет period без определения symbol."""

        if isinstance(
            data,
            dict,
        ):
            raw_period = (
                data.get("period")
                or data.get("timeframe")
            )

            if raw_period is not None:
                try:
                    return int(
                        raw_period
                    )
                except (
                    TypeError,
                    ValueError,
                ):
                    pass

        for (
            _symbol,
            candidate_period,
        ) in self.subscriptions:
            return candidate_period

        return None

    def _build_candles_from_history_items(
        self,
        items: list[Any],
        period: int,
    ) -> list[PocketOptionCandle]:
        """
        Преобразует history data в свечи.

        Если сервер прислал OHLC:
            OHLC сохраняются как свечи.

        Если сервер прислал ticks:
            ticks группируются по period.
        """

        if not items:
            return []

        ohlc_candles: list[
            PocketOptionCandle
        ] = []

        tick_items: list[
            tuple[int, float]
        ] = []

        for item in items:
            candle = self._parse_candle(
                item
            )

            if candle is not None:
                ohlc_candles.append(
                    candle
                )
                continue

            tick = self._parse_history_tick(
                item
            )

            if tick is not None:
                tick_items.append(
                    tick
                )

        # Если есть полноценные OHLC,
        # используем их.
        if ohlc_candles:
            ohlc_candles.sort(
                key=lambda candle:
                    candle.timestamp
            )

            unique: dict[
                int,
                PocketOptionCandle,
            ] = {}

            for candle in ohlc_candles:
                unique[
                    candle.timestamp
                ] = candle

            return list(
                sorted(
                    unique.values(),
                    key=lambda candle:
                        candle.timestamp,
                )
            )

        if not tick_items:
            return []

        return self._compile_ticks_to_candles(
            tick_items,
            period,
        )

    @staticmethod
    def _parse_history_tick(
        item: Any,
    ) -> tuple[int, float] | None:
        """Преобразует history item в (timestamp, price)."""

        if isinstance(
            item,
            dict,
        ):
            timestamp = (
                item.get("time")
                if item.get("time") is not None
                else item.get("timestamp")
            )

            price = (
                item.get("price")
                if item.get("price") is not None
                else item.get("value")
            )

            if (
                timestamp is None
                or price is None
            ):
                return None

            normalized_timestamp = (
                PocketOptionWebSocketClient
                ._normalize_timestamp(
                    timestamp
                )
            )

            if normalized_timestamp is None:
                return None

            try:
                return (
                    normalized_timestamp,
                    float(price),
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
            if len(item) < 2:
                return None

            # Формат [timestamp, price].
            try:
                first = float(
                    item[0]
                )
                second = float(
                    item[1]
                )
            except (
                TypeError,
                ValueError,
            ):
                return None

            if first > 1_000_000_000:
                timestamp = first
                price = second
            elif second > 1_000_000_000:
                price = first
                timestamp = second
            else:
                return None

            normalized_timestamp = (
                PocketOptionWebSocketClient
                ._normalize_timestamp(
                    timestamp
                )
            )

            if normalized_timestamp is None:
                return None

            return (
                normalized_timestamp,
                price,
            )

        return None

    @staticmethod
    def _compile_ticks_to_candles(
        ticks: list[
            tuple[int, float]
        ],
        period: int,
    ) -> list[PocketOptionCandle]:
        """Собирает OHLC candles из ticks."""

        if period <= 0:
            return []

        buckets: dict[
            int,
            list[tuple[int, float]],
        ] = {}

        for timestamp, price in ticks:
            bucket = (
                int(timestamp)
                // period
            ) * period

            buckets.setdefault(
                bucket,
                [],
            ).append(
                (
                    int(timestamp),
                    float(price),
                )
            )

        candles: list[
            PocketOptionCandle
        ] = []

        for bucket_timestamp in sorted(
            buckets
        ):
            values = sorted(
                buckets[
                    bucket_timestamp
                ],
                key=lambda item:
                    item[0],
            )

            if not values:
                continue

            prices = [
                price
                for _timestamp, price
                in values
            ]

            candles.append(
                PocketOptionCandle(
                    timestamp=bucket_timestamp,
                    open=prices[0],
                    high=max(prices),
                    low=min(prices),
                    close=prices[-1],
                    volume=float(
                        len(prices)
                    ),
                )
            )

        return candles

    def _merge_history_with_recent_ticks(
        self,
        symbol: str,
        period: int,
        candles: list[PocketOptionCandle],
    ) -> list[PocketOptionCandle]:
        """
        Добавляет recent ticks к истории.

        Это позволяет не оставлять разрыв между history
        и текущим live stream.
        """

        values = list(
            self.ticks.get(
                symbol,
                [],
            )
        )

        if not values:
            return candles

        history_last = (
            candles[-1].timestamp
            if candles
            else 0
        )

        recent_ticks = []

        for timestamp, price in values:
            normalized_timestamp = (
                self._normalize_timestamp(
                    timestamp
                )
            )

            if normalized_timestamp is None:
                continue

            if normalized_timestamp < history_last:
                continue

            recent_ticks.append(
                (
                    normalized_timestamp,
                    float(price),
                )
            )

        if not recent_ticks:
            return candles

        live_candles = (
            self._compile_ticks_to_candles(
                recent_ticks,
                period,
            )
        )

        if not live_candles:
            return candles

        merged: dict[
            int,
            PocketOptionCandle,
        ] = {
            candle.timestamp: candle
            for candle in candles
        }

        for candle in live_candles:
            existing = merged.get(
                candle.timestamp
            )

            if existing is None:
                merged[
                    candle.timestamp
                ] = candle
                continue

            # Для текущей свечи ticks считаются
            # более свежими данными.
            merged[
                candle.timestamp
            ] = candle

        return list(
            sorted(
                merged.values(),
                key=lambda candle:
                    candle.timestamp,
            )
        )

    @staticmethod
    def _extract_candles(
        data: Any,
        period: int | None = None,
    ) -> list[PocketOptionCandle]:
        """Извлекает полноценные OHLC свечи из JSON."""

        raw_items: Any = data

        if isinstance(
            data,
            dict,
        ):
            for key in (
                "candles",
                "history",
                "data",
                "result",
                "items",
                "values",
            ):
                if key in data:
                    candidate = data[key]

                    if isinstance(
                        candidate,
                        list,
                    ) and candidate:
                        raw_items = candidate
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

        unique: dict[
            int,
            PocketOptionCandle,
        ] = {}

        for candle in result:
            unique[
                candle.timestamp
            ] = candle

        return list(
            sorted(
                unique.values(),
                key=lambda candle:
                    candle.timestamp,
            )
        )

    @staticmethod
    def _parse_candle(
        item: Any,
    ) -> PocketOptionCandle | None:
        """Преобразует один элемент в OHLC свечу."""

        if isinstance(item, dict):
            timestamp = (
                item.get("timestamp")
                if item.get(
                    "timestamp"
                ) is not None
                else item.get("time")
            )

            if timestamp is None:
                timestamp = item.get(
                    "from"
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

            normalized_timestamp = (
                PocketOptionWebSocketClient
                ._normalize_timestamp(
                    timestamp
                )
            )

            if normalized_timestamp is None:
                return None

            try:
                return PocketOptionCandle(
                    timestamp=normalized_timestamp,
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
                timestamp = (
                    PocketOptionWebSocketClient
                    ._normalize_timestamp(
                        item[0]
                    )
                )

                if timestamp is None:
                    return None

                a = float(item[1])
                b = float(item[2])
                c = float(item[3])
                d = float(item[4])

            except (
                TypeError,
                ValueError,
            ):
                return None

            candidates = [
                (
                    a,
                    b,
                    c,
                    d,
                    "open-high-low-close",
                ),
                (
                    a,
                    c,
                    d,
                    b,
                    "open-close-high-low",
                ),
            ]

            valid_candidates: list[
                tuple[
                    float,
                    float,
                    float,
                    float,
                    str,
                ]
            ] = []

            for (
                open_value,
                high_value,
                low_value,
                close_value,
                fmt,
            ) in candidates:
                if (
                    high_value
                    >= max(
                        open_value,
                        close_value,
                    )
                    and low_value
                    <= min(
                        open_value,
                        close_value,
                    )
                    and high_value >= low_value
                ):
                    valid_candidates.append(
                        (
                            open_value,
                            high_value,
                            low_value,
                            close_value,
                            fmt,
                        )
                    )

            if not valid_candidates:
                return None

            if len(valid_candidates) > 1:
                selected = next(
                    (
                        candidate
                        for candidate
                        in valid_candidates
                        if candidate[4]
                        == "open-close-high-low"
                    ),
                    valid_candidates[0],
                )
            else:
                selected = valid_candidates[0]

            (
                open_value,
                high_value,
                low_value,
                close_value,
                _,
            ) = selected

            volume = 0.0

            if len(item) > 5:
                try:
                    volume = float(
                        item[5]
                    )
                except (
                    TypeError,
                    ValueError,
                ):
                    volume = 0.0

            return PocketOptionCandle(
                timestamp=timestamp,
                open=open_value,
                high=high_value,
                low=low_value,
                close=close_value,
                volume=volume,
            )

        return None

    def _resolve_legacy_history_waiter(
        self,
        symbol: str,
        period: int,
        candles: list[PocketOptionCandle],
    ) -> None:
        """
        Fallback для history-событий без index.

        Основной путь использует request_index.
        """

        for (
            request_index,
            meta,
        ) in list(
            self._history_request_meta.items()
        ):
            if meta != (
                symbol,
                period,
            ):
                continue

            waiter = (
                self._history_waiters_by_index.get(
                    request_index
                )
            )

            if (
                waiter is not None
                and not waiter.done()
            ):
                waiter.set_result(
                    candles
                )

                print(
                    "[PO] HISTORY FALLBACK "
                    "WAITER RESOLVED:"
                    f" index={request_index}"
                )

                return

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

        return list(
            self.assets
        )

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
            "history_cached": {
                f"{symbol}:{period}": len(
                    candles
                )
                for (
                    symbol,
                    period,
                ), candles in self.history.items()
            },
            "subscriptions": [
                {
                    "symbol": symbol,
                    "period": period,
                }
                for symbol, period
                in self.subscriptions
            ],
            "pending_history_requests": [
                {
                    "index": request_index,
                    "symbol": meta[0],
                    "period": meta[1],
                }
                for (
                    request_index,
                    meta,
                ) in self._history_request_meta.items()
            ],
            "server_time_synced": (
                self.server_time_synced
            ),
            "server_time_offset": (
                self.server_time_offset
            ),
            "update_stream_count": (
                self.update_stream_count
            ),
            "last_update_stream_symbol": (
                self.last_update_stream_symbol
            ),
            "last_update_stream_price": (
                self.last_update_stream_price
            ),
            "last_update_stream_timestamp": (
                self.last_update_stream_timestamp
            ),
            "last_message": self.last_message,
            "last_error": self.last_error,
            "lifecycle_running": (
                self.lifecycle_task is not None
                and not self.lifecycle_task.done()
            ),
            "reader_running": (
                self.reader_task is not None
                and not self.reader_task.done()
            ),
        }

    # ================================================================
    # HISTORY WAITER MANAGEMENT
    # ================================================================

    def _fail_history_waiters(
        self,
        reason: str,
    ) -> None:
        """Завершает ожидающие history-запросы ошибкой."""

        error = PocketOptionWebSocketError(
            reason
        )

        for waiter in list(
            self._history_waiters_by_index.values()
        ):
            if waiter.done():
                continue

            waiter.set_exception(
                PocketOptionWebSocketError(
                    str(error)
                )
            )

        self._history_waiters_by_index.clear()
        self._history_request_meta.clear()

    # ================================================================
    # START / STOP
    # ================================================================

    async def start(
        self,
    ) -> None:
        """
        Запускает WebSocket-клиент.

        Первое подключение выполняется синхронно.
        После него отдельный lifecycle task следит
        за reader и автоматически переподключается.
        """

        if (
            self.connected
            and self.socketio_connected
        ):
            return

        self._stop_event.clear()

        await self.connect()

        if (
            self.lifecycle_task is None
            or self.lifecycle_task.done()
        ):
            self.lifecycle_task = asyncio.create_task(
                self._lifecycle_loop()
            )

            print(
                "[PO] Lifecycle/reconnect task запущен"
            )

    async def _lifecycle_loop(
        self,
    ) -> None:
        """
        Следит за reader.

        Если WebSocket оборвался:

            reader завершился
                ↓
            cleanup
                ↓
            reconnect
                ↓
            новый AUTH
        """

        delay = self.RECONNECT_MIN

        try:
            while not self._stop_event.is_set():

                reader = self.reader_task

                if reader is not None:
                    try:
                        await reader

                    except asyncio.CancelledError:
                        if self._stop_event.is_set():
                            return

                    except Exception as exc:
                        self.last_error = str(
                            exc
                        )

                        print(
                            "[PO] Lifecycle увидел "
                            "ошибку reader: "
                            f"{exc}"
                        )

                if self._stop_event.is_set():
                    return

                self.connected = False
                self.socketio_connected = False
                self.authenticated = False

                print(
                    "[PO] Соединение потеряно. "
                    "Запускаем reconnect."
                )

                await self._cleanup_connection(
                    preserve_lifecycle=True
                )

                if self._stop_event.is_set():
                    return

                print(
                    "[PO] Повторное подключение через "
                    f"{delay} сек."
                )

                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=delay,
                    )

                    return

                except asyncio.TimeoutError:
                    pass

                try:
                    await self.connect()

                    delay = self.RECONNECT_MIN

                    print(
                        "[PO] Reconnect успешен"
                    )

                except asyncio.CancelledError:
                    raise

                except Exception as exc:
                    self.last_error = str(
                        exc
                    )

                    print(
                        "[PO] Reconnect ошибка: "
                        f"{exc}"
                    )

                    delay = min(
                        delay * 2,
                        self.RECONNECT_MAX,
                    )

        except asyncio.CancelledError:
            if not self._stop_event.is_set():
                print(
                    "[PO] Lifecycle task отменён"
                )

            raise

        finally:
            print(
                "[PO] Lifecycle/reconnect task остановлен"
            )

    async def _cleanup_connection(
        self,
        preserve_lifecycle: bool = False,
    ) -> None:
        """
        Безопасно закрывает текущее соединение.

        preserve_lifecycle=True используется самим
        lifecycle task, чтобы он случайно не отменил себя.
        """

        current = asyncio.current_task()

        reader = self.reader_task

        if (
            reader is not None
            and reader is not current
            and not reader.done()
        ):
            reader.cancel()

            try:
                await reader

            except asyncio.CancelledError:
                pass

            except Exception:
                pass

        self.reader_task = None

        if not preserve_lifecycle:
            self._fail_history_waiters(
                "WebSocket connection closed"
            )

        ws = self.ws
        self.ws = None

        if ws is not None:
            try:
                await ws.close()

            except Exception:
                pass

        session = self.session
        self.session = None

        if session is not None:
            try:
                await session.close()

            except Exception:
                pass

        self.connected = False
        self.socketio_connected = False
        self.authenticated = False

        self._binary_event = None
        self._binary_attachments.clear()

    async def stop(
        self,
    ) -> None:
        """Полностью останавливает WebSocket-клиент."""

        print(
            "[PO] Остановка WebSocket клиента..."
        )

        self._stop_event.set()

        lifecycle = self.lifecycle_task

        if (
            lifecycle is not None
            and lifecycle is not asyncio.current_task()
            and not lifecycle.done()
        ):
            lifecycle.cancel()

            try:
                await lifecycle

            except asyncio.CancelledError:
                pass

            except Exception:
                pass

        self.lifecycle_task = None

        self._fail_history_waiters(
            "WebSocket client stopped"
        )

        await self._cleanup_connection(
            preserve_lifecycle=False
        )

        self.auth_event.clear()
        self.auth_sent_at = None

        print(
            "[PO] WebSocket клиент остановлен"
        )

    async def run_forever(
        self,
        reconnect: bool = True,
    ) -> None:
        """
        Совместимость со старым интерфейсом.

        Основной lifecycle запускается через start().
        """

        if not reconnect:
            await self.connect()

            try:
                if self.reader_task is not None:
                    await self.reader_task

            finally:
                await self.stop()

            return

        await self.start()

        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(60)

        except asyncio.CancelledError:
            raise

        finally:
            await self.stop()


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
