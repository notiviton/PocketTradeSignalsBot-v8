import asyncio
import json
import os
import time
from collections import deque
from typing import Any, Awaitable, Callable

import aiohttp


class PocketOptionWebSocketError(Exception):
    """Ошибка Pocket Option WebSocket."""


TickCallback = Callable[
    [str, float, float],
    Awaitable[None],
] | None


class PocketOptionWebSocketClient:
    """
    Асинхронный клиент Pocket Option Socket.IO / Engine.IO.

    Архитектура:

        Engine.IO
            ↓
        Socket.IO namespace
            ↓
        auth
            ↓
        auth/success
            ↓
        ticks / history
            ↓
        tick_callback

    ВАЖНО:

    Этот клиент является ТОЛЬКО аналитическим транспортным слоем.

    Он:
        - подключается к WebSocket;
        - авторизуется;
        - получает рыночные данные;
        - принимает ticks/history;
        - позволяет подписаться на поток данных.

    Он НЕ:
        - открывает сделки;
        - закрывает сделки;
        - размещает ордера;
        - содержит buy/sell/place_order;
        - содержит торговый executor;
        - передаёт сигналы в торговый API.
    """

    DEFAULT_WS_URL = (
        "wss://api-spb.po.market/"
        "socket.io/?EIO=4&transport=websocket"
    )

    def __init__(
        self,
        websocket_url: str | None = None,
        ssid: str | None = None,
        uid: str | None = None,
        lang: str | None = None,
        current_url: str | None = None,
        is_chart: int | None = None,
        tick_callback: TickCallback = None,
        buffer_size: int = 10000,
    ) -> None:

        # =====================================================================
        # CONFIG
        # =====================================================================

        self.websocket_url = (
            websocket_url
            or os.getenv("POCKET_OPTION_WS_URL")
            or self.DEFAULT_WS_URL
        )

        # ВАЖНО:
        # POCKET_OPTION_SSID содержит ТОЛЬКО raw sessionToken.
        self.ssid = (
            ssid
            or os.getenv("POCKET_OPTION_SSID")
        )

        self.uid = (
            uid
            or os.getenv("POCKET_OPTION_UID")
        )

        self.lang = (
            lang
            or os.getenv("POCKET_OPTION_LANG")
            or "ru"
        )

        self.current_url = (
            current_url
            or os.getenv("POCKET_OPTION_CURRENT_URL")
            or "cabinet/quick-high-low/USD"
        )

        self.is_chart = (
            is_chart
            if is_chart is not None
            else self._env_int(
                "POCKET_OPTION_IS_CHART",
                1,
            )
        )

        self.tick_callback = tick_callback

        # =====================================================================
        # AIOHTTP
        # =====================================================================

        self.session: aiohttp.ClientSession | None = None
        self.ws: aiohttp.ClientWebSocketResponse | None = None

        # =====================================================================
        # CONNECTION STATE
        # =====================================================================

        self.connected = False
        self.namespace_connected = False
        self.authenticated = False

        self._running = False
        self._stop_requested = False

        # =====================================================================
        # SOCKET.IO BINARY STATE
        # =====================================================================

        self._binary_event_name: str | None = None
        self._binary_event_payload: Any = None
        self._binary_expected = 0
        self._binary_received = 0
        self._binary_attachments: dict[int, bytes] = {}

        # =====================================================================
        # DATA BUFFERS
        # =====================================================================

        self._ticks: deque[
            tuple[str, float, float]
        ] = deque(
            maxlen=buffer_size
        )

        self._last_tick: dict[
            str,
            tuple[float, float],
        ] = {}

        # =====================================================================
        # TIME / DIAGNOSTICS
        # =====================================================================

        self._last_message_time = 0.0
        self._last_tick_time = 0.0
        self._authenticated_at = 0.0

        self._subscriptions: set[str] = set()

    # =========================================================================
    # ENV
    # =========================================================================

    @staticmethod
    def _env_int(
        name: str,
        default: int,
    ) -> int:
        value = os.getenv(name)

        if value is None:
            return default

        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    # =========================================================================
    # CONNECT
    # =========================================================================

    async def connect(self) -> None:
        """
        Создаёт WebSocket-соединение.

        ВАЖНО:
        connect() означает только установление WebSocket.

        authenticated становится True только после:
            42["auth/success"]
        """

        if not self.websocket_url:
            raise PocketOptionWebSocketError(
                "POCKET_OPTION_WS_URL is not configured"
            )

        if not self.ssid:
            raise PocketOptionWebSocketError(
                "POCKET_OPTION_SSID is not configured"
            )

        if not self.uid:
            raise PocketOptionWebSocketError(
                "POCKET_OPTION_UID is not configured"
            )

        await self.close()

        self.session = aiohttp.ClientSession(
            headers={
                "Origin": "https://pocketoption.com",
                "User-Agent": (
                    "Mozilla/5.0 "
                    "(Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 "
                    "(KHTML, like Gecko) "
                    "Chrome/140.0 Safari/537.36"
                ),
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
        )

        try:
            self.ws = await self.session.ws_connect(
                self.websocket_url,
                heartbeat=None,
                autoping=False,
                receive_timeout=None,
                max_msg_size=1_000_000,
            )

        except Exception as exc:
            await self.close()

            raise PocketOptionWebSocketError(
                f"WebSocket connection failed: {exc}"
            ) from exc

        self.connected = True
        self.namespace_connected = False
        self.authenticated = False

        self._reset_binary_state()

        print(
            "~ Pocket Option WebSocket connected"
        )
        print(
            f"  URL: {self.websocket_url}"
        )

    # =========================================================================
    # CLOSE
    # =========================================================================

    async def close(self) -> None:
        """Корректно закрывает WebSocket и HTTP session."""

        self.connected = False
        self.namespace_connected = False
        self.authenticated = False

        self._reset_binary_state()

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

    # =========================================================================
    # SEND TEXT
    # =========================================================================

    async def send_text(
        self,
        message: str,
    ) -> None:
        """Отправляет TEXT WebSocket frame."""

        if self.ws is None:
            raise PocketOptionWebSocketError(
                "WebSocket is not connected"
            )

        if self.ws.closed:
            raise PocketOptionWebSocketError(
                "WebSocket is closed"
            )

        await self.ws.send_str(message)

    # =========================================================================
    # ENGINE.IO PING/PONG
    # =========================================================================

    async def _send_engineio_pong(self) -> None:
        """Отвечает на Engine.IO ping."""

        await self.send_text("3")

    # =========================================================================
    # AUTH
    # =========================================================================

    async def send_auth(self) -> None:
        """
        Отправляет АКТУАЛЬНЫЙ формат Pocket Option auth.

        Подтверждённый browser-пакет:

            42["auth",{
                "sessionToken":"...",
                "uid":"...",
                "lang":"ru",
                "currentUrl":"cabinet/quick-high-low/USD",
                "isChart":1
            }]

        POCKET_OPTION_SSID должен содержать только raw sessionToken.
        """

        if not self.ssid:
            raise PocketOptionWebSocketError(
                "POCKET_OPTION_SSID is empty"
            )

        if not self.uid:
            raise PocketOptionWebSocketError(
                "POCKET_OPTION_UID is empty"
            )

        auth_payload = {
            "sessionToken": self.ssid,
            "uid": str(self.uid),
            "lang": self.lang,
            "currentUrl": self.current_url,
            "isChart": self.is_chart,
        }

        message = (
            "42"
            + json.dumps(
                [
                    "auth",
                    auth_payload,
                ],
                separators=(",", ":"),
                ensure_ascii=False,
            )
        )

        await self.send_text(message)

        print(
            "~ Pocket Option auth message sent"
        )
        print(
            "  sessionToken: SET"
        )
        print(
            f"  uid: {self.uid}"
        )
        print(
            f"  lang: {self.lang}"
        )
        print(
            f"  currentUrl: {self.current_url}"
        )
        print(
            f"  isChart: {self.is_chart}"
        )

    # =========================================================================
    # SUBSCRIBE
    # =========================================================================

    async def subscribe(
        self,
        symbol: str,
    ) -> None:
        """
        Подписывается на поток рыночных данных.

        Используется Socket.IO событие:

            42["subfor","EURUSD_otc"]

        Этот метод предназначен ТОЛЬКО для получения данных.
        Торговых команд здесь нет.
        """

        symbol = str(symbol).strip()

        if not symbol:
            raise PocketOptionWebSocketError(
                "Cannot subscribe to empty symbol"
            )

        if not self.authenticated:
            raise PocketOptionWebSocketError(
                "Cannot subscribe before authentication"
            )

        payload = [
            "subfor",
            symbol,
        ]

        message = (
            "42"
            + json.dumps(
                payload,
                separators=(",", ":"),
                ensure_ascii=False,
            )
        )

        await self.send_text(message)

        self._subscriptions.add(symbol)

        print(
            "~ Subscribed to market data: "
            f"{symbol}"
        )

    async def unsubscribe(
        self,
        symbol: str,
    ) -> None:
        """
        Удаляет символ из локального списка подписок.

        Не отправляет торговых команд.
        """

        symbol = str(symbol).strip()

        if not symbol:
            return

        self._subscriptions.discard(symbol)

        print(
            "~ Local subscription removed: "
            f"{symbol}"
        )

    # =========================================================================
    # RUN
    # =========================================================================

    async def run(self) -> None:
        """
        Основной reader loop.

        ВАЖНО:
        Только этот цикл читает WebSocket.

        Никакие другие методы не вызывают ws.receive()/ws.recv().
        Это необходимо для корректной обработки Socket.IO binary
        attachments.
        """

        await self.connect()

        self._running = True
        self._stop_requested = False

        try:
            if self.ws is None:
                raise PocketOptionWebSocketError(
                    "WebSocket disappeared after connect"
                )

            async for message in self.ws:

                self._last_message_time = time.time()

                if message.type == aiohttp.WSMsgType.TEXT:

                    await self._handle_text(
                        message.data
                    )

                elif message.type == aiohttp.WSMsgType.BINARY:

                    await self._handle_binary(
                        message.data
                    )

                elif message.type == aiohttp.WSMsgType.PING:

                    print(
                        "< WebSocket PING"
                    )

                    if self.ws is not None:
                        await self.ws.pong()

                elif message.type == aiohttp.WSMsgType.PONG:

                    print(
                        "< WebSocket PONG"
                    )

                elif message.type == aiohttp.WSMsgType.CLOSE:

                    print(
                        "< WebSocket CLOSE"
                    )

                    break

                elif message.type == aiohttp.WSMsgType.CLOSED:

                    print(
                        "< WebSocket CLOSED"
                    )

                    break

                elif message.type == aiohttp.WSMsgType.ERROR:

                    error = (
                        self.ws.exception()
                        if self.ws is not None
                        else None
                    )

                    print(
                        "x WebSocket error: "
                        f"{error}"
                    )

                    break

        finally:

            self._running = False

            await self.close()

    # =========================================================================
    # RUN FOREVER
    # =========================================================================

    async def run_forever(
        self,
        reconnect_delay: float = 3.0,
        max_reconnect_delay: float = 30.0,
    ) -> None:
        """
        Работает с автоматическим reconnect.

        После переподключения авторизация выполняется заново.
        Подписки хранятся локально и могут быть восстановлены
        после auth/success.
        """

        self._stop_requested = False

        current_delay = max(
            0.5,
            reconnect_delay,
        )

        while not self._stop_requested:

            try:

                await self.run()

                current_delay = max(
                    0.5,
                    reconnect_delay,
                )

            except asyncio.CancelledError:
                raise

            except Exception as exc:

                print(
                    "x Pocket Option WebSocket error: "
                    f"{type(exc).__name__}: {exc}"
                )

            if self._stop_requested:
                break

            print(
                "t Reconnecting in "
                f"{current_delay:.1f}s..."
            )

            try:
                await asyncio.sleep(
                    current_delay
                )
            except asyncio.CancelledError:
                raise

            current_delay = min(
                current_delay * 2,
                max_reconnect_delay,
            )

    # =========================================================================
    # STOP
    # =========================================================================

    async def stop(self) -> None:
        """Останавливает client."""

        self._stop_requested = True

        await self.close()

    # =========================================================================
    # HANDLE TEXT
    # =========================================================================

    async def _handle_text(
        self,
        data: str,
    ) -> None:
        """
        Обрабатывает Engine.IO / Socket.IO TEXT frames.
        """

        if not data:
            return

        preview = data[:500]

        # Не печатаем потенциально чувствительные auth-пакеты
        # целиком.
        if data.startswith('42["auth"'):
            print(
                "+ TEXT Socket.IO auth packet"
            )
        else:
            print(
                f"+ TEXT {preview}"
            )

        # =====================================================================
        # ENGINE.IO OPEN
        # =====================================================================

        if data.startswith("0"):

            print(
                "~ Engine.IO OPEN"
            )

            try:
                engine_data = json.loads(
                    data[1:]
                )

                if isinstance(
                    engine_data,
                    dict,
                ):
                    print(
                        "  pingInterval:",
                        engine_data.get(
                            "pingInterval"
                        ),
                    )
                    print(
                        "  pingTimeout:",
                        engine_data.get(
                            "pingTimeout"
                        ),
                    )
                    print(
                        "  maxPayload:",
                        engine_data.get(
                            "maxPayload"
                        ),
                    )

            except json.JSONDecodeError:
                print(
                    "~ Engine.IO OPEN payload "
                    "could not be parsed"
                )

            await self.send_text("40")

            return

        # =====================================================================
        # ENGINE.IO PING
        # =====================================================================

        if data == "2":

            print(
                "< Engine.IO PING"
            )

            await self._send_engineio_pong()

            return

        # =====================================================================
        # ENGINE.IO PONG
        # =====================================================================

        if data == "3":

            print(
                "< Engine.IO PONG"
            )

            return

        # =====================================================================
        # SOCKET.IO NAMESPACE
        # =====================================================================

        if data.startswith("40"):

            self.namespace_connected = True

            print(
                "~ Socket.IO namespace connected"
            )

            await self.send_auth()

            return

        # =====================================================================
        # SOCKET.IO EVENT
        # =====================================================================

        if data.startswith("42"):

            await self._parse_socketio_event(
                data
            )

            return

        # =====================================================================
        # SOCKET.IO BINARY EVENT HEADER
        # =====================================================================

        if data.startswith("45"):

            await self._parse_binary_event_header(
                data
            )

            return

        # =====================================================================
        # SOCKET.IO DISCONNECT
        # =====================================================================

        if data.startswith("41"):

            self.namespace_connected = False
            self.authenticated = False

            print(
                "x Socket.IO namespace disconnected"
            )

            return

        print(
            "< Unhandled TEXT frame: "
            f"{preview}"
        )

    # =========================================================================
    # SOCKET.IO EVENT
    # =========================================================================

    async def _parse_socketio_event(
        self,
        data: str,
    ) -> None:
        """Разбирает обычный Socket.IO event."""

        payload = data[2:]

        try:

            event = json.loads(
                payload
            )

        except json.JSONDecodeError:

            print(
                "x Invalid Socket.IO JSON: "
                f"{payload[:500]}"
            )

            return

        if not isinstance(event, list):
            return

        if not event:
            return

        event_name = event[0]

        if not isinstance(
            event_name,
            str,
        ):
            return

        print(
            "< Socket.IO event: "
            f"{event_name}"
        )

        # =====================================================================
        # AUTH SUCCESS
        # =====================================================================

        if event_name in {
            "auth/success",
            "successauth",
        }:

            self.authenticated = True
            self._authenticated_at = time.time()

            print(
                "✓ Pocket Option authentication SUCCESS"
            )

            # После reconnect восстанавливаем подписки.
            if self._subscriptions:

                subscriptions = list(
                    self._subscriptions
                )

                self._subscriptions.clear()

                for symbol in subscriptions:

                    try:
                        await self.subscribe(
                            symbol
                        )
                    except Exception as exc:
                        print(
                            "x Subscription restore failed "
                            f"for {symbol}: {exc}"
                        )

            return

        # =====================================================================
        # AUTH FAILURE
        # =====================================================================

        if event_name in {
            "auth/error",
            "auth/failed",
            "NotAuthorized",
        }:

            self.authenticated = False

            print(
                "x Pocket Option authentication FAILED"
            )

            if len(event) > 1:

                print(
                    "  reason:",
                    self._safe_preview(
                        event[1],
                        500,
                    ),
                )

            return

        # =====================================================================
        # UPDATE STREAM
        # =====================================================================

        if event_name == "updateStream":

            if len(event) > 1:

                ticks = self._extract_stream_ticks(
                    event[1]
                )

                for (
                    symbol,
                    timestamp,
                    price,
                ) in ticks:

                    await self._emit_tick(
                        symbol,
                        timestamp,
                        price,
                    )

            return

        # =====================================================================
        # HISTORY
        # =====================================================================

        if event_name == "updateHistoryNewFast":

            if len(event) > 1:

                ticks = self._extract_history_ticks(
                    event[1]
                )

                for (
                    symbol,
                    timestamp,
                    price,
                ) in ticks:

                    await self._emit_tick(
                        symbol,
                        timestamp,
                        price,
                    )

            return

        # =====================================================================
        # UNKNOWN
        # =====================================================================

        print(
            "~ Unhandled Socket.IO event: "
            f"{event_name}"
        )

    # =========================================================================
    # BINARY HEADER
    # =========================================================================

    async def _parse_binary_event_header(
        self,
        data: str,
    ) -> None:
        """
        Регистрирует Socket.IO binary event.

        Пример:

            451-["updateStream",{
                "_placeholder":true,
                "num":0
            }]

        ВАЖНО:
        Здесь НЕТ ws.recv().

        Следующий binary frame будет получен тем же
        основным reader loop и попадёт в _handle_binary().
        """

        separator = data.find("-")

        if separator < 0:

            print(
                "x Invalid Socket.IO binary header"
            )

            return

        # Для Socket.IO packet:
        #
        # 45 + <attachment_count> + "-"
        #
        # Например:
        #
        # 451-...
        #
        attachment_count_text = data[
            2:separator
        ]

        try:

            attachment_count = int(
                attachment_count_text
            )

        except ValueError:

            print(
                "x Invalid binary attachment count: "
                f"{attachment_count_text}"
            )

            return

        json_part = data[
            separator + 1:
        ]

        try:

            event = json.loads(
                json_part
            )

        except json.JSONDecodeError:

            print(
                "x Invalid binary event JSON"
            )

            return

        if not isinstance(
            event,
            list,
        ):
            return

        if not event:
            return

        event_name = event[0]

        if not isinstance(
            event_name,
            str,
        ):
            return

        payload = (
            event[1]
            if len(event) > 1
            else None
        )

        self._binary_event_name = event_name
        self._binary_event_payload = payload
        self._binary_expected = attachment_count
        self._binary_received = 0
        self._binary_attachments = {}

        print(
            "~ Binary Socket.IO event registered: "
            f"{event_name}"
        )
        print(
            "  expected attachments:",
            attachment_count,
        )

        # Теоретически Socket.IO может прислать binary packet
        # с нулевым количеством attachments.
        if attachment_count == 0:

            self._reset_binary_state()

            await self._process_payload(
                event_name,
                payload,
            )

    # =========================================================================
    # HANDLE BINARY
    # =========================================================================

    async def _handle_binary(
        self,
        data: bytes,
    ) -> None:
        """
        Принимает один Socket.IO binary attachment.

        Этот метод вызывается только основным reader loop.
        """

        print(
            "< BINARY attachment: "
            f"{len(data)} bytes"
        )

        if (
            self._binary_expected <= 0
            or self._binary_event_name is None
        ):

            print(
                "x Binary attachment received "
                "without pending Socket.IO event"
            )

            return

        attachment_index = (
            self._binary_received
        )

        self._binary_attachments[
            attachment_index
        ] = data

        self._binary_received += 1

        if (
            self._binary_received
            < self._binary_expected
        ):
            return

        event_name = (
            self._binary_event_name
        )

        payload = (
            self._binary_event_payload
        )

        attachments = dict(
            self._binary_attachments
        )

        self._reset_binary_state()

        resolved_payload = (
            self._replace_binary_placeholders(
                payload,
                attachments,
            )
        )

        await self._process_payload(
            event_name,
            resolved_payload,
        )

    # =========================================================================
    # PLACEHOLDERS
    # =========================================================================

    @classmethod
    def _replace_binary_placeholders(
        cls,
        value: Any,
        attachments: dict[int, bytes],
    ) -> Any:
        """
        Рекурсивно заменяет Socket.IO placeholders:

            {
                "_placeholder": true,
                "num": 0
            }

        на соответствующий bytes attachment.
        """

        if isinstance(value, dict):

            if (
                value.get("_placeholder") is True
                and "num" in value
            ):

                try:
                    number = int(
                        value["num"]
                    )
                except (
                    TypeError,
                    ValueError,
                ):
                    return value

                return attachments.get(
                    number,
                    value,
                )

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

        if isinstance(value, tuple):

            return tuple(
                cls._replace_binary_placeholders(
                    item,
                    attachments,
                )
                for item in value
            )

        return value

    # =========================================================================
    # PROCESS PAYLOAD
    # =========================================================================

    async def _process_payload(
        self,
        event_name: str,
        payload: Any,
    ) -> None:
        """
        Обрабатывает уже собранный Socket.IO payload.
        """

        if event_name == "updateStream":

            decoded_payload = (
                self._decode_possible_binary_json(
                    payload
                )
            )

            ticks = self._extract_stream_ticks(
                decoded_payload
            )

            for (
                symbol,
                timestamp,
                price,
            ) in ticks:

                await self._emit_tick(
                    symbol,
                    timestamp,
                    price,
                )

            return

        if event_name == "updateHistoryNewFast":

            decoded_payload = (
                self._decode_possible_binary_json(
                    payload
                )
            )

            ticks = self._extract_history_ticks(
                decoded_payload
            )

            for (
                symbol,
                timestamp,
                price,
            ) in ticks:

                await self._emit_tick(
                    symbol,
                    timestamp,
                    price,
                )

            return

        print(
            "~ Unknown binary event payload: "
            f"{event_name}"
        )

        print(
            "  payload:",
            self._safe_preview(
                payload,
                1000,
            ),
        )

    # =========================================================================
    # DECODE BINARY JSON
    # =========================================================================

    @staticmethod
    def _decode_possible_binary_json(
        value: Any,
    ) -> Any:
        """
        Пытается декодировать bytes как UTF-8 JSON.

        Если binary payload не является UTF-8 JSON,
        возвращает исходные bytes.

        Это намеренно НЕ делает предположений о неизвестном
        бинарном формате Pocket Option.
        """

        if not isinstance(
            value,
            (bytes, bytearray),
        ):
            return value

        raw = bytes(value)

        try:

            text = raw.decode(
                "utf-8"
            )

        except UnicodeDecodeError:

            return raw

        try:

            return json.loads(
                text
            )

        except json.JSONDecodeError:

            return raw

    # =========================================================================
    # EXTRACT STREAM TICKS
    # =========================================================================

    @staticmethod
    def _extract_stream_ticks(
        payload: Any,
    ) -> list[
        tuple[str, float, float]
    ]:
        """
        Извлекает ticks из updateStream.

        Поддерживаемый подтверждённый формат:

            [
                [
                    "CHF NOK_otc",
                    1786750548.048,
                    8.958
                ]
            ]

        Также поддерживается одиночный tick.
        """

        result: list[
            tuple[str, float, float]
        ] = []

        if isinstance(
            payload,
            (bytes, bytearray),
        ):
            return result

        if not isinstance(
            payload,
            list,
        ):
            return result

        # ---------------------------------------------------------------------
        # Один tick:
        #
        # ["EURUSD_otc", timestamp, price]
        # ---------------------------------------------------------------------

        if (
            len(payload) >= 3
            and isinstance(
                payload[0],
                str,
            )
        ):

            try:

                result.append(
                    (
                        payload[0],
                        float(payload[1]),
                        float(payload[2]),
                    )
                )

            except (
                TypeError,
                ValueError,
            ):
                pass

            return result

        # ---------------------------------------------------------------------
        # Список ticks:
        #
        # [
        #   ["EURUSD_otc", timestamp, price],
        #   ...
        # ]
        # ---------------------------------------------------------------------

        for item in payload:

            if not isinstance(
                item,
                list,
            ):
                continue

            if len(item) < 3:
                continue

            symbol = item[0]

            if not isinstance(
                symbol,
                str,
            ):
                continue

            try:

                timestamp = float(
                    item[1]
                )

                price = float(
                    item[2]
                )

            except (
                TypeError,
                ValueError,
            ):
                continue

            result.append(
                (
                    symbol,
                    timestamp,
                    price,
                )
            )

        return result

    # =========================================================================
    # EXTRACT HISTORY
    # =========================================================================

    @staticmethod
    def _extract_history_ticks(
        payload: Any,
    ) -> list[
        tuple[str, float, float]
    ]:
        """
        Извлекает историю.

        Поддерживаемые форматы:

        1.
            [
                ["EURUSD_otc", timestamp, price],
                ...
            ]

        2.
            {
                "asset": "CHF NOK_otc",
                "period": 60,
                "history": [
                    [timestamp, price],
                    ...
                ]
            }
        """

        result: list[
            tuple[str, float, float]
        ] = []

        if isinstance(
            payload,
            (bytes, bytearray),
        ):
            return result

        # =====================================================================
        # LIST
        # =====================================================================

        if isinstance(
            payload,
            list,
        ):

            # Один tick.
            if (
                len(payload) >= 3
                and isinstance(
                    payload[0],
                    str,
                )
            ):

                try:

                    result.append(
                        (
                            payload[0],
                            float(payload[1]),
                            float(payload[2]),
                        )
                    )

                except (
                    TypeError,
                    ValueError,
                ):
                    pass

                return result

            # Список ticks.
            for item in payload:

                if (
                    isinstance(
                        item,
                        list,
                    )
                    and len(item) >= 3
                    and isinstance(
                        item[0],
                        str,
                    )
                ):

                    try:

                        result.append(
                            (
                                item[0],
                                float(item[1]),
                                float(item[2]),
                            )
                        )

                    except (
                        TypeError,
                        ValueError,
                    ):
                        continue

            return result

        # =====================================================================
        # DICT
        # =====================================================================

        if isinstance(
            payload,
            dict,
        ):

            symbol = (
                payload.get("asset")
                or payload.get("symbol")
            )

            history = payload.get(
                "history"
            )

            if not isinstance(
                symbol,
                str,
            ):
                return result

            if not isinstance(
                history,
                list,
            ):
                return result

            for item in history:

                if (
                    not isinstance(
                        item,
                        list,
                    )
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

                result.append(
                    (
                        symbol,
                        timestamp,
                        price,
                    )
                )

        return result

    # =========================================================================
    # EMIT TICK
    # =========================================================================

    async def _emit_tick(
        self,
        symbol: str,
        timestamp: float,
        price: float,
    ) -> None:
        """
        Сохраняет tick и передаёт его callback.

        Никакой торговой логики здесь нет.
        """

        symbol = str(
            symbol
        ).strip()

        if not symbol:
            return

        try:

            timestamp = float(
                timestamp
            )

            price = float(
                price
            )

        except (
            TypeError,
            ValueError,
        ):
            return

        if timestamp <= 0:
            return

        if price <= 0:
            return

        tick = (
            symbol,
            timestamp,
            price,
        )

        self._ticks.append(
            tick
        )

        self._last_tick[
            symbol
        ] = (
            timestamp,
            price,
        )

        self._last_tick_time = time.time()

        print(
            f"📈 TICK "
            f"{symbol} "
            f"{timestamp:.3f} "
            f"{price}"
        )

        if self.tick_callback is None:
            return

        try:

            result = self.tick_callback(
                symbol,
                timestamp,
                price,
            )

            if result is not None:
                await result

        except asyncio.CancelledError:
            raise

        except Exception as exc:

            print(
                "x Tick callback error: "
                f"{type(exc).__name__}: {exc}"
            )

    # =========================================================================
    # RESET BINARY
    # =========================================================================

    def _reset_binary_state(
        self,
    ) -> None:

        self._binary_event_name = None
        self._binary_event_payload = None
        self._binary_expected = 0
        self._binary_received = 0
        self._binary_attachments = {}

    # =========================================================================
    # GET TICKS
    # =========================================================================

    def get_ticks(
        self,
        symbol: str | None = None,
    ) -> list[
        tuple[str, float, float]
    ]:
        """Возвращает копию tick buffer."""

        ticks = list(
            self._ticks
        )

        if symbol is None:
            return ticks

        return [
            tick
            for tick in ticks
            if tick[0] == symbol
        ]

    # =========================================================================
    # GET LAST TICK
    # =========================================================================

    def get_last_tick(
        self,
        symbol: str,
    ) -> tuple[float, float] | None:
        """Возвращает последний tick."""

        return self._last_tick.get(
            symbol
        )

    # =========================================================================
    # GET SUBSCRIPTIONS
    # =========================================================================

    def get_subscriptions(self) -> list[str]:
        """Возвращает список текущих локальных подписок."""

        return sorted(
            self._subscriptions
        )

    # =========================================================================
    # STATUS
    # =========================================================================

    def status(self) -> dict[str, Any]:
        """
        Возвращает безопасный статус.

        SSID никогда не возвращается.
        """

        return {
            "connected": self.connected,
            "namespace_connected": (
                self.namespace_connected
            ),
            "authenticated": self.authenticated,
            "running": self._running,
            "ticks_buffered": len(
                self._ticks
            ),
            "last_message_time": (
                self._last_message_time
            ),
            "last_tick_time": (
                self._last_tick_time
            ),
            "authenticated_at": (
                self._authenticated_at
            ),
            "uid": self.uid,
            "subscriptions": (
                self.get_subscriptions()
            ),
            "analytics_only": True,
            "trade_execution": False,
        }

    # =========================================================================
    # SAFE PREVIEW
    # =========================================================================

    @staticmethod
    def _safe_preview(
        value: Any,
        limit: int = 1000,
    ) -> str:
        """Безопасный preview для диагностических логов."""

        try:

            text = json.dumps(
                value,
                ensure_ascii=False,
                default=str,
            )

        except Exception:

            text = repr(value)

        return text[:limit]


# =============================================================================
# DIAGNOSTIC MAIN
# =============================================================================

async def main() -> None:
    """
    Диагностический запуск.

    Только подключение и получение рыночных данных.

    Автоматическая торговля отсутствует.
    """

    print("=" * 64)
    print(
        "PocketTradeSignalsBot"
    )
    print(
        "Pocket Option WebSocket diagnostic client"
    )
    print("=" * 64)

    print(
        "WS URL:",
        os.getenv(
            "POCKET_OPTION_WS_URL"
        )
        or PocketOptionWebSocketClient.DEFAULT_WS_URL,
    )

    print(
        "SSID:",
        "SET"
        if os.getenv(
            "POCKET_OPTION_SSID"
        )
        else "NOT SET",
    )

    print(
        "UID:",
        os.getenv(
            "POCKET_OPTION_UID"
        )
        or "NOT SET",
    )

    print(
        "LANG:",
        os.getenv(
            "POCKET_OPTION_LANG",
            "ru",
        ),
    )

    print(
        "CURRENT URL:",
        os.getenv(
            "POCKET_OPTION_CURRENT_URL",
            "cabinet/quick-high-low/USD",
        ),
    )

    print(
        "IS CHART:",
        os.getenv(
            "POCKET_OPTION_IS_CHART",
            "1",
        ),
    )

    print(
        "TRADE EXECUTION:",
        "DISABLED",
    )

    print("=" * 64)

    async def on_tick(
        symbol: str,
        timestamp: float,
        price: float,
    ) -> None:

        print(
            "CALLBACK:",
            symbol,
            timestamp,
            price,
        )

    client = PocketOptionWebSocketClient(
        tick_callback=on_tick,
    )

    try:

        await client.run_forever()

    except KeyboardInterrupt:

        print(
            "Stopped by user"
        )

    except PocketOptionWebSocketError as exc:

        print(
            "PocketOptionWebSocketError:",
            exc,
        )

    except asyncio.CancelledError:

        print(
            "Cancelled"
        )

        raise

    except Exception as exc:

        print(
            "Unexpected error: "
            f"{type(exc).__name__}: {exc}"
        )

    finally:

        await client.close()

        print(
            "Final status:",
            client.status(),
        )


if __name__ == "__main__":
    asyncio.run(main())
