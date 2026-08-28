cat > /app/pocket_option/pocket_option/websocket_client.py <<'PY'
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
    Pocket Option Socket.IO / Engine.IO WebSocket client.

    Поток подключения:

        Engine.IO OPEN
            ↓
        40
            ↓
        40{...}
            ↓
        42["auth", {...}]
            ↓
        42["auth/success"]

    Клиент получает реальные ticks и не открывает сделки.
    """

    def __init__(
        self,
        websocket_url: str | None = None,
        ssid: str | None = None,
        uid: str | None = None,
        is_demo: int | None = None,
        platform: int | None = None,
        tick_callback: TickCallback = None,
        buffer_size: int = 10000,
    ) -> None:

        self.websocket_url = (
            websocket_url
            or os.getenv("POCKET_OPTION_WS_URL")
            or (
                "wss://api-spb.po.market/"
                "socket.io/?EIO=4&transport=websocket"
            )
        )

        self.ssid = (
            ssid
            or os.getenv("POCKET_OPTION_SSID")
        )

        self.uid = (
            uid
            or os.getenv("POCKET_OPTION_UID")
        )

        self.is_demo = (
            is_demo
            if is_demo is not None
            else self._env_int(
                "POCKET_OPTION_IS_DEMO",
                1,
            )
        )

        self.platform = (
            platform
            if platform is not None
            else self._env_int(
                "POCKET_OPTION_PLATFORM",
                1,
            )
        )

        self.tick_callback = tick_callback

        self.session: aiohttp.ClientSession | None = None
        self.ws: aiohttp.ClientWebSocketResponse | None = None

        self.connected = False
        self.namespace_connected = False
        self.authenticated = False

        self._running = False
        self._stop_requested = False

        self._binary_event_name: str | None = None
        self._binary_expected = 0
        self._binary_attachments: list[bytes] = []

        self._ticks: deque[
            tuple[str, float, float]
        ] = deque(
            maxlen=buffer_size
        )

        self._last_tick: dict[
            str,
            tuple[float, float],
        ] = {}

        self._last_message_time = 0.0
        self._last_tick_time = 0.0

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

        except ValueError:
            return default

    # =========================================================================
    # CONNECT
    # =========================================================================

    async def connect(self) -> None:
        """Создаёт WebSocket-соединение."""

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

        self.session = aiohttp.ClientSession()

        try:
            self.ws = await self.session.ws_connect(
                self.websocket_url,
                heartbeat=20,
                autoping=True,
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
        """Корректно закрывает соединение."""

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
        """Отправляет текстовый WebSocket frame."""

        if self.ws is None:
            raise PocketOptionWebSocketError(
                "WebSocket is not connected"
            )

        await self.ws.send_str(message)

    # =========================================================================
    # AUTH
    # =========================================================================

    async def send_auth(self) -> None:
        """
        Отправляет Pocket Option auth.

        В окружении:

            POCKET_OPTION_SSID
            POCKET_OPTION_UID
            POCKET_OPTION_IS_DEMO
            POCKET_OPTION_PLATFORM
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
            "session": self.ssid,
            "isDemo": self.is_demo,
            "uid": int(self.uid),
            "platform": self.platform,
            "isFastHistory": True,
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
            "  session: SET"
        )
        print(
            f"  uid: {self.uid}"
        )
        print(
            f"  isDemo: {self.is_demo}"
        )
        print(
            f"  platform: {self.platform}"
        )

    # =========================================================================
    # RUN
    # =========================================================================

    async def run(self) -> None:
        """Основной цикл получения WebSocket сообщений."""

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
                    if self.ws is not None:
                        await self.ws.pong()

                elif message.type == aiohttp.WSMsgType.PONG:
                    print("< PONG")

                elif message.type == aiohttp.WSMsgType.CLOSE:
                    print("< CLOSE")
                    break

                elif message.type == aiohttp.WSMsgType.CLOSED:
                    print("< CLOSED")
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
    ) -> None:
        """Работает с автоматическим reconnect."""

        self._stop_requested = False

        while not self._stop_requested:

            try:
                await self.run()

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
                f"{reconnect_delay:.1f}s..."
            )

            await asyncio.sleep(
                reconnect_delay
            )

    # =========================================================================
    # STOP
    # =========================================================================

    async def stop(self) -> None:
        """Останавливает клиент."""

        self._stop_requested = True

        await self.close()

    # =========================================================================
    # HANDLE TEXT
    # =========================================================================

    async def _handle_text(
        self,
        data: str,
    ) -> None:
        """Обрабатывает Engine.IO / Socket.IO TEXT."""

        if not data:
            return

        print(
            f"+ TEXT {data[:500]}"
        )

        # ---------------------------------------------------------------------
        # Engine.IO OPEN
        # ---------------------------------------------------------------------

        if data.startswith("0"):
            print(
                "~ Engine.IO OPEN"
            )

            await self.send_text("40")

            return

        # ---------------------------------------------------------------------
        # Engine.IO PING
        # ---------------------------------------------------------------------

        if data == "2":
            await self.send_text("3")
            return

        # ---------------------------------------------------------------------
        # Engine.IO PONG
        # ---------------------------------------------------------------------

        if data == "3":
            print(
                "< Engine.IO PONG"
            )
            return

        # ---------------------------------------------------------------------
        # Socket.IO namespace
        # ---------------------------------------------------------------------

        if data.startswith("40"):

            self.namespace_connected = True

            print(
                "~ Socket.IO namespace connected"
            )

            await self.send_auth()

            return

        # ---------------------------------------------------------------------
        # Socket.IO event
        # ---------------------------------------------------------------------

        if data.startswith("42"):

            await self._parse_socketio_event(
                data
            )

            return

        # ---------------------------------------------------------------------
        # Socket.IO binary event
        # ---------------------------------------------------------------------

        if data.startswith("45"):

            await self._parse_binary_event_header(
                data
            )

            return

        print(
            "< Unhandled TEXT frame: "
            f"{data[:300]}"
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

        print(
            "< Socket.IO event: "
            f"{event_name}"
        )

        if event_name in {
            "auth/success",
            "successauth",
        }:

            self.authenticated = True

            print(
                "✓ Pocket Option authentication SUCCESS"
            )

        elif event_name in {
            "auth/error",
            "auth/failed",
        }:

            self.authenticated = False

            print(
                "x Pocket Option authentication FAILED"
            )

        elif event_name == "updateStream":

            if len(event) > 1:

                ticks = self._extract_stream_ticks(
                    event[1]
                )

                for symbol, timestamp, price in ticks:

                    await self._emit_tick(
                        symbol,
                        timestamp,
                        price,
                    )

        elif event_name == "updateHistoryNewFast":

            if len(event) > 1:

                ticks = self._extract_history_ticks(
                    event[1]
                )

                for symbol, timestamp, price in ticks:

                    await self._emit_tick(
                        symbol,
                        timestamp,
                        price,
                    )

    # =========================================================================
    # BINARY HEADER
    # =========================================================================

    async def _parse_binary_event_header(
        self,
        data: str,
    ) -> None:
        """Разбирает Socket.IO binary event header."""

        separator = data.find("-")

        if separator < 0:
            print(
                "x Invalid binary event header"
            )
            return

        attachment_count_text = data[
            2:separator
        ]

        try:
            attachment_count = int(
                attachment_count_text
            )

        except ValueError:

            print(
                "x Invalid binary attachment count"
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

        if not isinstance(event, list):
            return

        if not event:
            return

        self._binary_event_name = str(
            event[0]
        )

        self._binary_expected = (
            attachment_count
        )

        self._binary_attachments = []

        print(
            "~ Binary event registered: "
            f"{self._binary_event_name}"
        )

        print(
            "  expected attachments: "
            f"{attachment_count}"
        )

        if attachment_count == 0:

            payload = (
                event[1]
                if len(event) > 1
                else None
            )

            event_name = (
                self._binary_event_name
            )

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
        """Принимает Socket.IO binary attachment."""

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
                "without pending event"
            )

            return

        self._binary_attachments.append(
            data
        )

        if (
            len(self._binary_attachments)
            < self._binary_expected
        ):
            return

        attachments = list(
            self._binary_attachments
        )

        event_name = (
            self._binary_event_name
        )

        self._reset_binary_state()

        await self._process_binary_event(
            event_name,
            attachments,
        )

    # =========================================================================
    # PROCESS BINARY
    # =========================================================================

    async def _process_binary_event(
        self,
        event_name: str,
        attachments: list[bytes],
    ) -> None:
        """Декодирует binary attachments."""

        for attachment in attachments:

            try:
                decoded = attachment.decode(
                    "utf-8"
                )

            except UnicodeDecodeError:

                print(
                    "x Binary attachment "
                    "is not UTF-8"
                )

                continue

            try:
                payload = json.loads(
                    decoded
                )

            except json.JSONDecodeError:

                print(
                    "x Binary attachment "
                    "is not JSON"
                )

                continue

            await self._process_payload(
                event_name,
                payload,
            )

    # =========================================================================
    # PROCESS PAYLOAD
    # =========================================================================

    async def _process_payload(
        self,
        event_name: str,
        payload: Any,
    ) -> None:
        """Преобразует Pocket Option payload в ticks."""

        if event_name == "updateStream":

            ticks = self._extract_stream_ticks(
                payload
            )

            for symbol, timestamp, price in ticks:

                await self._emit_tick(
                    symbol,
                    timestamp,
                    price,
                )

            return

        if event_name == "updateHistoryNewFast":

            ticks = self._extract_history_ticks(
                payload
            )

            for symbol, timestamp, price in ticks:

                await self._emit_tick(
                    symbol,
                    timestamp,
                    price,
                )

            return

        print(
            "~ Unknown binary event: "
            f"{event_name}"
        )

    # =========================================================================
    # EXTRACT STREAM TICKS
    # =========================================================================

    @staticmethod
    def _extract_stream_ticks(
        payload: Any,
    ) -> list[
        tuple[str, float, float]
    ]:
        """Извлекает ticks из updateStream."""

        result: list[
            tuple[str, float, float]
        ] = []

        if not isinstance(payload, list):
            return result

        # Иногда payload сам является одним tick.
        if (
            len(payload) >= 3
            and isinstance(payload[0], str)
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

        for item in payload:

            if not isinstance(item, list):
                continue

            if len(item) < 3:
                continue

            symbol = item[0]

            if not isinstance(symbol, str):
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
    # EXTRACT HISTORY TICKS
    # =========================================================================

    @staticmethod
    def _extract_history_ticks(
        payload: Any,
    ) -> list[
        tuple[str, float, float]
    ]:
        """Извлекает ticks из history payload."""

        result: list[
            tuple[str, float, float]
        ] = []

        # ---------------------------------------------------------------------
        # Простой формат:
        #
        # [
        #   ["EURUSD_otc", timestamp, price],
        #   ...
        # ]
        # ---------------------------------------------------------------------

        if isinstance(payload, list):

            if (
                len(payload) >= 3
                and isinstance(payload[0], str)
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

            for item in payload:

                if (
                    isinstance(item, list)
                    and len(item) >= 3
                    and isinstance(item[0], str)
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

        # ---------------------------------------------------------------------
        # Object format:
        #
        # {
        #   "asset": "EURUSD_otc",
        #   "history": [
        #       [timestamp, price],
        #       ...
        #   ]
        # }
        # ---------------------------------------------------------------------

        if isinstance(payload, dict):

            symbol = (
                payload.get("asset")
                or payload.get("symbol")
            )

            history = payload.get(
                "history"
            )

            if not isinstance(symbol, str):
                return result

            if not isinstance(history, list):
                return result

            for item in history:

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
        """Сохраняет tick и вызывает callback."""

        if not symbol:
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
        self._binary_expected = 0
        self._binary_attachments = []

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
    # STATUS
    # =========================================================================

    def status(self) -> dict[str, Any]:
        """Безопасный статус клиента."""

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
            "uid": self.uid,
            "is_demo": self.is_demo,
            "platform": self.platform,
        }

    # =========================================================================
    # SAFE PREVIEW
    # =========================================================================

    @staticmethod
    def _safe_preview(
        value: Any,
        limit: int = 1000,
    ) -> str:
        """Безопасный preview для логов."""

        try:

            text = json.dumps(
                value,
                ensure_ascii=False,
            )

        except Exception:

            text = repr(value)

        return text[:limit]


# =============================================================================
# TEST MAIN
# =============================================================================

async def main() -> None:
    """
    Диагностический запуск.

    Сделки НЕ открываются.
    """

    print("=" * 60)
    print(
        "Pocket Option WebSocket diagnostic client"
    )
    print("=" * 60)

    print(
        "WS URL:",
        os.getenv(
            "POCKET_OPTION_WS_URL"
        )
        or (
            "wss://api-spb.po.market/"
            "socket.io/?EIO=4&transport=websocket"
        ),
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
        "IS DEMO:",
        os.getenv(
            "POCKET_OPTION_IS_DEMO",
            "1",
        ),
    )

    print(
        "PLATFORM:",
        os.getenv(
            "POCKET_OPTION_PLATFORM",
            "1",
        ),
    )

    print("=" * 60)

    client = PocketOptionWebSocketClient()

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
    asyncio.run(main)
PY
