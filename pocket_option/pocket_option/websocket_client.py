import asyncio
import json
import os
import time
from collections import deque
from typing import Any, Awaitable, Callable

import aiohttp


class PocketOptionWebSocketError(Exception):
    """Ошибка подключения или работы Pocket Option WebSocket."""


TickCallback = Callable[[str, float, float], Awaitable[None] | None]


class PocketOptionWebSocketClient:
    """
    Низкоуровневый клиент Pocket Option WebSocket.

    Поддерживает:

    - Engine.IO handshake;
    - Socket.IO namespace;
    - Pocket Option SSID;
    - Engine.IO ping/pong;
    - Socket.IO text events;
    - Socket.IO binary events;
    - updateHistoryNewFast;
    - updateStream;
    - binary attachments;
    - преобразование tick -> (symbol, timestamp, price);
    - буфер последних тиков;
    - безопасное переподключение.

    ВАЖНО:

    Этот класс НЕ строит OHLC-свечи.
    Он только получает реальные OTC ticks.

    Формат тика:

        symbol
        timestamp
        price

    Например:

        EURUSD_otc
        1787000294.973
        1.21427
    """

    def __init__(
        self,
        websocket_url: str | None = None,
        ssid: str | None = None,
        tick_callback: TickCallback | None = None,
        buffer_size: int = 10000,
    ):
        self.websocket_url = (
            websocket_url
            or os.getenv("POCKET_OPTION_WS_URL")
        )

        self.ssid = (
            ssid
            or os.getenv("POCKET_OPTION_SSID")
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

        self._ticks: deque[tuple[str, float, float]] = deque(
            maxlen=buffer_size
        )

        self._last_tick: dict[str, tuple[float, float]] = {}

        self._last_message_time = 0.0
        self._last_tick_time = 0.0

    # ========================================================
    # CONNECT
    # ========================================================

    async def connect(self) -> None:
        """
        Устанавливает WebSocket-соединение.

        Авторизация выполняется после Engine.IO OPEN
        и Socket.IO namespace connection.
        """

        if not self.websocket_url:
            raise PocketOptionWebSocketError(
                "POCKET_OPTION_WS_URL is not configured"
            )

        if not self.ssid:
            raise PocketOptionWebSocketError(
                "POCKET_OPTION_SSID is not configured"
            )

        if self.session is not None:
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

        print("✓ Pocket Option WebSocket connected")

    # ========================================================
    # CLOSE
    # ========================================================

    async def close(self) -> None:
        """Корректно закрывает WebSocket и HTTP session."""

        self.connected = False
        self.namespace_connected = False
        self.authenticated = False

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

    # ========================================================
    # SEND TEXT
    # ========================================================

    async def send_text(self, message: str) -> None:
        """Отправляет текстовый WebSocket frame."""

        if self.ws is None:
            raise PocketOptionWebSocketError(
                "WebSocket is not connected"
            )

        await self.ws.send_str(message)

    # ========================================================
    # AUTH
    # ========================================================

    async def send_auth(self) -> None:
        """
        Отправляет сохранённый Pocket Option SSID.

        Сам SSID никогда не печатается в лог.

        Ожидаемый формат:

            ["auth", {...}]

        Если переменная уже содержит префикс 42,
        он удаляется перед разбором.
        """

        if not self.ssid:
            raise PocketOptionWebSocketError(
                "POCKET_OPTION_SSID is empty"
            )

        raw = self.ssid.strip()

        # ----------------------------------------------------
        # Если случайно сохранён полный Socket.IO пакет:
        #
        # 42["auth",{...}]
        # ----------------------------------------------------

        if raw.startswith("42"):
            raw = raw[2:].strip()

        try:
            payload = json.loads(raw)

        except json.JSONDecodeError as exc:
            raise PocketOptionWebSocketError(
                "POCKET_OPTION_SSID is not valid JSON. "
                'Expected ["auth", {...}]'
            ) from exc

        if not isinstance(payload, list):
            raise PocketOptionWebSocketError(
                'POCKET_OPTION_SSID must be a JSON list '
                'like ["auth", {...}]'
            )

        if len(payload) < 2:
            raise PocketOptionWebSocketError(
                'POCKET_OPTION_SSID must contain '
                '["auth", {...}]'
            )

        if payload[0] != "auth":
            raise PocketOptionWebSocketError(
                'POCKET_OPTION_SSID first element must be "auth"'
            )

        if not isinstance(payload[1], dict):
            raise PocketOptionWebSocketError(
                'POCKET_OPTION_SSID second element must be an object'
            )

        message = "42" + json.dumps(
            payload,
            separators=(",", ":"),
            ensure_ascii=False,
        )

        await self.send_text(message)

        self.authenticated = True

        print("✓ Pocket Option auth message sent")

    # ========================================================
    # RUN
    # ========================================================

    async def run(self) -> None:
        """
        Основной цикл WebSocket.

        Получает TEXT/BINARY frames и передаёт их
        соответствующим обработчикам.
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
                        "WebSocket error: "
                        f"{error}"
                    )

                    break

        finally:

            self._running = False

            await self.close()

    # ========================================================
    # RUN WITH RECONNECT
    # ========================================================

    async def run_forever(
        self,
        reconnect_delay: float = 3.0,
    ) -> None:
        """
        Запускает WebSocket с автоматическим reconnect.

        Fail-closed:

        если соединение оборвалось,
        старые live-данные не считаются новыми.

        После reconnect клиент снова проходит handshake.
        """

        self._stop_requested = False

        while not self._stop_requested:

            try:

                await self.run()

            except asyncio.CancelledError:

                raise

            except Exception as exc:

                print(
                    "✖ Pocket Option WebSocket error: "
                    f"{type(exc).__name__}: {exc}"
                )

            if self._stop_requested:
                break

            print(
                f"↻ Reconnecting in "
                f"{reconnect_delay:.1f}s..."
            )

            await asyncio.sleep(
                reconnect_delay
            )

    # ========================================================
    # STOP
    # ========================================================

    async def stop(self) -> None:
        """Останавливает reconnect loop и WebSocket."""

        self._stop_requested = True

        await self.close()

    # ========================================================
    # HANDLE TEXT
    # ========================================================

    async def _handle_text(
        self,
        data: str,
    ) -> None:
        """
        Обрабатывает Engine.IO и Socket.IO
        текстовые кадры.
        """

        if not data:
            return

        # Не печатаем огромные binary headers целиком.
        print(
            f"+ TEXT {data[:500]}"
        )

        # ----------------------------------------------------
        # Engine.IO OPEN
        # ----------------------------------------------------

        if data.startswith("0"):

            print("✓ Engine.IO OPEN")

            await self.send_text("40")

            return

        # ----------------------------------------------------
        # Engine.IO PING
        # ----------------------------------------------------

        if data == "2":

            await self.send_text("3")

            return

        # ----------------------------------------------------
        # Engine.IO PONG
        # ----------------------------------------------------

        if data == "3":

            print("< Engine.IO PONG")

            return

        # ----------------------------------------------------
        # Socket.IO namespace connection
        # ----------------------------------------------------

        if data == "40":

            self.namespace_connected = True

            print(
                "✓ Socket.IO namespace connected"
            )

            await self.send_auth()

            return

        # ----------------------------------------------------
        # Socket.IO normal event
        # ----------------------------------------------------

        if data.startswith("42"):

            await self._parse_socketio_event(
                data
            )

            return

        # ----------------------------------------------------
        # Socket.IO binary event
        #
        # Example:
        #
        # 451-["updateHistoryNewFast",
        #      {"_placeholder":true,"num":0}]
        # ----------------------------------------------------

        if data.startswith("451-"):

            await self._parse_binary_event_header(
                data
            )

            return

        # ----------------------------------------------------
        # Other Engine.IO / Socket.IO frames
        # ----------------------------------------------------

        print(
            f"◁ Unhandled TEXT frame: "
            f"{data[:300]}"
        )

    # ========================================================
    # HANDLE BINARY
    # ========================================================

    async def _handle_binary(
        self,
        data: bytes,
    ) -> None:
        """
        Обрабатывает binary attachment.

        Attachment связывается с предыдущим
        451- binary event header.
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
                "⚠ Binary attachment received "
                "without pending binary event"
            )

            return

        self._binary_attachments.append(data)

        attachment_number = (
            len(self._binary_attachments) - 1
        )

        print(
            "  attachment #"
            f"{attachment_number}"
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

    # ========================================================
    # PARSE SOCKET.IO EVENT
    # ========================================================

    async def _parse_socketio_event(
        self,
        data: str,
    ) -> None:
        """
        Разбирает обычный Socket.IO event:

            42["event",{...}]
        """

        payload = data[2:]

        try:

            event = json.loads(
                payload
            )

        except json.JSONDecodeError:

            print(
                "⚠ Invalid Socket.IO JSON: "
                f"{payload[:500]}"
            )

            return

        if not isinstance(event, list):

            print(
                "◁ Socket.IO payload: "
                f"{event!r}"
            )

            return

        if not event:
            return

        event_name = event[0]

        print(
            "◁ Socket.IO event: "
            f"{event_name}"
        )

        if len(event) > 1:

            print(
                "  payload: "
                f"{self._safe_preview(event[1])}"
            )

    # ========================================================
    # PARSE BINARY HEADER
    # ========================================================

    async def _parse_binary_event_header(
        self,
        data: str,
    ) -> None:
        """
        Разбирает Socket.IO binary-event header.

        Пример:

            451-[
                "updateHistoryNewFast",
                {
                    "_placeholder": true,
                    "num": 0
                }
            ]

        Здесь 1 означает количество attachments.
        """

        print(
            "◁ Socket.IO binary event: "
            f"{data[:1000]}"
        )

        # ----------------------------------------------------
        # Формат:
        #
        # 45<attachments>-[...]
        #
        # Для 451-:
        #
        # 45 + 1 + -
        # ----------------------------------------------------

        if not data.startswith("45"):
            return

        separator = data.find("-")

        if separator < 0:
            print(
                "⚠ Invalid binary event header"
            )

            return

        attachment_count_text = data[2:separator]

        try:

            attachment_count = int(
                attachment_count_text
            )

        except ValueError:

            print(
                "⚠ Invalid binary attachment count"
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
                "⚠ Invalid binary event JSON: "
                f"{json_part[:500]}"
            )

            return

        if not isinstance(event, list) or not event:

            print(
                "⚠ Invalid binary event payload"
            )

            return

        event_name = event[0]

        self._binary_event_name = (
            str(event_name)
        )

        self._binary_expected = (
            attachment_count
        )

        self._binary_attachments = []

        print(
            "✓ Binary event registered: "
            f"{self._binary_event_name}"
        )

        print(
            "  expected attachments: "
            f"{self._binary_expected}"
        )

        # ----------------------------------------------------
        # Если по какой-либо причине attachments = 0,
        # сразу обработаем JSON.
        # ----------------------------------------------------

        if attachment_count == 0:

            self._reset_binary_state()

    # ========================================================
    # PROCESS BINARY EVENT
    # ========================================================

    async def _process_binary_event(
        self,
        event_name: str,
        attachments: list[bytes],
    ) -> None:
        """
        Декодирует binary attachment.

        Для Pocket Option мы ожидаем JSON:

            [["EURUSD_otc", timestamp, price]]

        или history:

            {
                "asset": "EURUSD_otc",
                "period": 60,
                "history": [
                    [timestamp, price],
                    ...
                ]
            }
        """

        if not attachments:
            return

        for attachment in attachments:

            try:

                decoded = attachment.decode(
                    "utf-8"
                )

            except UnicodeDecodeError:

                print(
                    "✖ Binary attachment is not UTF-8"
                )

                continue

            print(
                "  decoded binary: "
                f"{decoded[:1000]}"
            )

            try:

                payload = json.loads(
                    decoded
                )

            except json.JSONDecodeError:

                print(
                    "⚠ Binary attachment "
                    "is not JSON"
                )

                continue

            await self._process_payload(
                event_name,
                payload,
            )

    # ========================================================
    # PROCESS PAYLOAD
    # ========================================================

    async def _process_payload(
        self,
        event_name: str,
        payload: Any,
    ) -> None:
        """
        Преобразует известные Pocket Option
        структуры в ticks.
        """

        # ----------------------------------------------------
        # updateStream
        #
        # [
        #   [
        #       "EURUSD_otc",
        #       1787000294.973,
        #       1.21427
        #   ]
        # ]
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # updateHistoryNewFast
        #
        # Вариант 1:
        #
        # [
        #   [
        #       "EURUSD_otc",
        #       timestamp,
        #       price
        #   ]
        # ]
        #
        # Вариант 2:
        #
        # {
        #   "asset": "EURUSD_otc",
        #   "period": 60,
        #   "history": [
        #       [timestamp, price],
        #       ...
        #   ]
        # }
        # ----------------------------------------------------

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
            "⚠ Unknown binary event: "
            f"{event_name}"
        )

    # ========================================================
    # EXTRACT STREAM TICKS
    # ========================================================

    @staticmethod
    def _extract_stream_ticks(
        payload: Any,
    ) -> list[tuple[str, float, float]]:
        """
        Извлекает:

            symbol, timestamp, price

        из:

            [
                [
                    "EURUSD_otc",
                    timestamp,
                    price
                ]
            ]
        """

        result: list[
            tuple[str, float, float]
        ] = []

        if not isinstance(payload, list):
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

                timestamp = float(item[1])
                price = float(item[2])

            except (
                TypeError,
                ValueError,
            ):

                continue

            if timestamp <= 0:
                continue

            if price <= 0:
                continue

            result.append(
                (
                    symbol,
                    timestamp,
                    price,
                )
            )

        return result

    # ========================================================
    # EXTRACT HISTORY TICKS
    # ========================================================

    @staticmethod
    def _extract_history_ticks(
        payload: Any,
    ) -> list[tuple[str, float, float]]:
        """
        Извлекает history ticks.

        Поддерживает:

            {
                "asset": "EURUSD_otc",
                "period": 60,
                "history": [
                    [timestamp, price],
                    ...
                ]
            }

        а также:

            [
                [
                    "EURUSD_otc",
                    timestamp,
                    price
                ]
            ]
        """

        result: list[
            tuple[str, float, float]
        ] = []

        # ----------------------------------------------------
        # Object history
        # ----------------------------------------------------

        if isinstance(payload, dict):

            symbol = payload.get(
                "asset"
            )

            history = payload.get(
                "history"
            )

            if (
                not isinstance(symbol, str)
                or not isinstance(history, list)
            ):
                return result

            for item in history:

                if not isinstance(item, list):
                    continue

                if len(item) < 2:
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

                if timestamp <= 0:
                    continue

                if price <= 0:
                    continue

                result.append(
                    (
                        symbol,
                        timestamp,
                        price,
                    )
                )

            return result

        # ----------------------------------------------------
        # Array format
        # ----------------------------------------------------

        if isinstance(payload, list):

            return PocketOptionWebSocketClient._extract_stream_ticks(
                payload
            )

        return result

    # ========================================================
    # EMIT TICK
    # ========================================================

    async def _emit_tick(
        self,
        symbol: str,
        timestamp: float,
        price: float,
    ) -> None:
        """
        Проверяет и сохраняет tick.

        Дубликаты и регресс времени
        отбрасываются.
        """

        symbol = symbol.strip()

        if not symbol:
            return

        if timestamp <= 0:
            return

        if price <= 0:
            return

        # ----------------------------------------------------
        # Проверка времени.
        #
        # Pocket Option timestamps уже подтверждены
        # как Unix seconds с дробной частью.
        # ----------------------------------------------------

        previous = self._last_tick.get(
            symbol
        )

        if previous is not None:

            previous_timestamp, previous_price = (
                previous
            )

            # Полный дубль.
            if (
                timestamp == previous_timestamp
                and price == previous_price
            ):
                return

            # Время не должно идти назад.
            if timestamp < previous_timestamp:
                print(
                    "⚠ Tick timestamp moved backwards: "
                    f"{symbol} "
                    f"{timestamp} < "
                    f"{previous_timestamp}"
                )

                return

        self._last_tick[symbol] = (
            timestamp,
            price,
        )

        self._last_tick_time = time.time()

        tick = (
            symbol,
            timestamp,
            price,
        )

        self._ticks.append(
            tick
        )

        # ----------------------------------------------------
        # Не печатаем каждый history tick.
        # Live tick можно показать кратко.
        # ----------------------------------------------------

        print(
            "✓ TICK "
            f"{symbol} "
            f"{timestamp:.3f} "
            f"{price}"
        )

        # ----------------------------------------------------
        # Передаём tick наружу.
        # ----------------------------------------------------

        if self.tick_callback is not None:

            try:

                result = self.tick_callback(
                    symbol,
                    timestamp,
                    price,
                )

                if asyncio.iscoroutine(result):

                    await result

            except Exception as exc:

                print(
                    "✖ Tick callback error: "
                    f"{type(exc).__name__}: {exc}"
                )

    # ========================================================
    # RESET BINARY STATE
    # ========================================================

    def _reset_binary_state(
        self,
    ) -> None:

        self._binary_event_name = None
        self._binary_expected = 0
        self._binary_attachments = []

    # ========================================================
    # GET TICKS
    # ========================================================

    def get_ticks(
        self,
        symbol: str | None = None,
    ) -> list[tuple[str, float, float]]:
        """
        Возвращает копию текущего tick buffer.

        Если symbol указан, возвращаются только
        ticks этого инструмента.
        """

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

    # ========================================================
    # GET LAST TICK
    # ========================================================

    def get_last_tick(
        self,
        symbol: str,
    ) -> tuple[float, float] | None:
        """
        Возвращает:

            (timestamp, price)

        последнего известного tick.
        """

        return self._last_tick.get(
            symbol
        )

    # ========================================================
    # STATUS
    # ========================================================

    def status(self) -> dict[str, Any]:
        """Возвращает безопасный статус клиента."""

        return {
            "connected": self.connected,
            "namespace_connected": (
                self.namespace_connected
            ),
            "authenticated": self.authenticated,
            "running": self._running,
            "ticks_buffered": len(self._ticks),
            "last_message_time": (
                self._last_message_time
            ),
            "last_tick_time": (
                self._last_tick_time
            ),
        }

    # ========================================================
    # SAFE PREVIEW
    # ========================================================

    @staticmethod
    def _safe_preview(
        value: Any,
        limit: int = 1000,
    ) -> str:
        """
        Безопасный preview объекта для логов.

        Не используется для вывода SSID.
        """

        try:

            text = json.dumps(
                value,
                ensure_ascii=False,
            )

        except Exception:

            text = repr(value)

        return text[:limit]


# ============================================================
# TEST MAIN
# ============================================================

async def main() -> None:
    """
    Самостоятельный диагностический запуск.

    Этот запуск НЕ открывает сделки.
    """

    print("=" * 60)
    print("Pocket Option WebSocket diagnostic client")
    print("=" * 60)

    print(
        "WS URL:",
        "SET"
        if os.getenv("POCKET_OPTION_WS_URL")
        else "NOT SET",
    )

    print(
        "SSID:",
        "SET"
        if os.getenv("POCKET_OPTION_SSID")
        else "NOT SET",
    )

    print("=" * 60)

    client = PocketOptionWebSocketClient()

    try:

        await client.run_forever()

    except KeyboardInterrupt:

        print("Stopped by user")

    except PocketOptionWebSocketError as exc:

        print(
            "✖ PocketOptionWebSocketError:",
            exc,
        )

    except asyncio.CancelledError:

        print("Cancelled")

        raise

    except Exception as exc:

        print(
            "✖ Unexpected error: "
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
