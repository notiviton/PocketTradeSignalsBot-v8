import asyncio
import json
import os

import aiohttp


class PocketOptionWebSocketError(Exception):
    """Ошибка подключения к Pocket Option WebSocket."""


class PocketOptionWebSocketClient:
    """
    Низкоуровневый клиент Pocket Option.

    Этап 1:
    - Engine.IO handshake;
    - Socket.IO namespace;
    - auth;
    - текстовые события;
    - binary attachments;
    - ping/pong.

    Торговая логика и построение свечей здесь НЕ выполняются.
    """

    def __init__(
        self,
        websocket_url: str | None = None,
        ssid: str | None = None,
    ):
        self.websocket_url = (
            websocket_url
            or os.getenv("POCKET_OPTION_WS_URL")
        )

        self.ssid = (
            ssid
            or os.getenv("POCKET_OPTION_SSID")
        )

        self.session: aiohttp.ClientSession | None = None
        self.ws: aiohttp.ClientWebSocketResponse | None = None

    async def connect(self) -> None:
        if not self.websocket_url:
            raise PocketOptionWebSocketError(
                "POCKET_OPTION_WS_URL is not configured"
            )

        if not self.ssid:
            raise PocketOptionWebSocketError(
                "POCKET_OPTION_SSID is not configured"
            )

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

        print("✅ WebSocket connected")

    async def close(self) -> None:
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

    async def send_text(self, message: str) -> None:
        if self.ws is None:
            raise PocketOptionWebSocketError(
                "WebSocket is not connected"
            )

        await self.ws.send_str(message)

    async def send_auth(self) -> None:
        """
        Отправляет уже сохранённый Pocket Option SSID.

        Сам SSID никогда не выводится в лог.
        """

        try:
            payload = json.loads(self.ssid)

        except json.JSONDecodeError as exc:
            raise PocketOptionWebSocketError(
                "POCKET_OPTION_SSID is not valid JSON"
            ) from exc

        if (
            not isinstance(payload, list)
            or len(payload) < 2
            or payload[0] != "auth"
            or not isinstance(payload[1], dict)
        ):
            raise PocketOptionWebSocketError(
                'POCKET_OPTION_SSID must contain '
                '["auth", {...}]'
            )

        message = "42" + json.dumps(
            payload,
            separators=(",", ":"),
        )

        await self.send_text(message)

        print("✅ Auth message sent")

    async def run(self) -> None:
        await self.connect()

        try:
            async for message in self.ws:
                if message.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_text(message.data)

                elif message.type == aiohttp.WSMsgType.BINARY:
                    await self._handle_binary(message.data)

                elif message.type == aiohttp.WSMsgType.PING:
                    await self.ws.pong()

                elif message.type == aiohttp.WSMsgType.PONG:
                    print("← PONG")

                elif message.type == aiohttp.WSMsgType.CLOSE:
                    print("← CLOSE")
                    break

                elif message.type == aiohttp.WSMsgType.CLOSED:
                    print("← CLOSED")
                    break

                elif message.type == aiohttp.WSMsgType.ERROR:
                    print(
                        "❌ WebSocket error: "
                        f"{self.ws.exception()}"
                    )
                    break

        finally:
            await self.close()

    async def _handle_text(self, data: str) -> None:
        """
        Обрабатывает Engine.IO и Socket.IO текстовые кадры.
        """

        print(f"← TEXT {data[:500]}")

        # Engine.IO OPEN
        if data.startswith("0"):
            print("✅ Engine.IO OPEN")

            # Подключение к Socket.IO namespace.
            await self.send_text("40")

            # После namespace connection отправляем auth.
            await self.send_auth()

            return

        # Engine.IO PING
        if data == "2":
            await self.send_text("3")
            return

        # Engine.IO PONG
        if data == "3":
            print("← Engine.IO PONG")
            return

        # Socket.IO event
        if data.startswith("42"):
            self._parse_socketio_event(data)
            return

        # Socket.IO binary event header.
        #
        # Пример, который мы уже видели:
        #
        # 451-["updateHistoryNewFast", ...]
        #
        if data.startswith("451-"):
            self._parse_binary_event_header(data)
            return

    async def _handle_binary(self, data: bytes) -> None:
        """
        Принимает binary attachment.

        Пока только диагностируем содержимое.
        Связывание attachment с 451- будет следующим этапом.
        """

        print(
            "← BINARY attachment: "
            f"{len(data)} bytes"
        )

        try:
            decoded = data.decode("utf-8")

        except UnicodeDecodeError:
            print("   Binary data is not UTF-8")
            return

        print(
            "   decoded: "
            f"{decoded[:1000]}"
        )

    @staticmethod
    def _parse_socketio_event(data: str) -> None:
        payload = data[2:]

        try:
            event = json.loads(payload)

        except json.JSONDecodeError:
            print(
                "⚠️ Invalid Socket.IO JSON: "
                f"{payload[:500]}"
            )
            return

        if not isinstance(event, list):
            print(
                "← Socket.IO payload: "
                f"{event!r}"
            )
            return

        if not event:
            return

        event_name = event[0]

        print(
            "← Socket.IO event: "
            f"{event_name}"
        )

        if len(event) > 1:
            print(
                "   payload: "
                f"{event[1]!r}"
            )

    @staticmethod
    def _parse_binary_event_header(data: str) -> None:
        print(
            "← Socket.IO binary event: "
            f"{data[:1000]}"
        )


async def main() -> None:
    client = PocketOptionWebSocketClient()

    try:
        await client.run()

    except KeyboardInterrupt:
        print("Stopped")

    except PocketOptionWebSocketError as exc:
        print(
            f"❌ PocketOptionWebSocketError: {exc}"
        )

    except Exception as exc:
        print(
            f"❌ Unexpected error: "
            f"{type(exc).__name__}: {exc}"
        )


if __name__ == "__main__":
    asyncio.run(main())
