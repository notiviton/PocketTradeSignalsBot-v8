import asyncio
import json
import os
import time

import aiohttp


WS_URL = os.getenv(
    "POCKET_OPTION_WS_URL",
    "wss://api-spb.po.market/socket.io/?EIO=4&transport=websocket",
)

SSID = os.getenv("POCKET_OPTION_SSID", "").strip()
UID = os.getenv("POCKET_OPTION_UID", "").strip()
LANG = os.getenv("POCKET_OPTION_LANG", "ru").strip() or "ru"
CURRENT_URL = (
    os.getenv("POCKET_OPTION_CURRENT_URL", "cabinet").strip()
    or "cabinet"
)
IS_CHART = os.getenv("POCKET_OPTION_IS_CHART", "1").strip() or "1"


def mask(value: str, keep: int = 4) -> str:
    if not value:
        return "<EMPTY>"

    if len(value) <= keep * 2:
        return "*" * len(value)

    return value[:keep] + "..." + value[-keep:]


def short_message(value: str, limit: int = 600) -> str:
    value = str(value)

    if len(value) <= limit:
        return value

    return value[:limit] + "... [TRUNCATED]"


async def main() -> None:
    print("=" * 70)
    print("POCKET OPTION WEBSOCKET AUTH DIAGNOSTIC")
    print("=" * 70)

    print()
    print("[CONFIG]")
    print("WS_URL:", WS_URL)
    print("SSID:", mask(SSID))
    print("UID:", UID or "<EMPTY>")
    print("LANG:", LANG)
    print("CURRENT_URL:", CURRENT_URL)
    print("IS_CHART:", IS_CHART)

    if not SSID:
        print()
        print("ERROR: POCKET_OPTION_SSID is empty")
        return

    if not UID:
        print()
        print("ERROR: POCKET_OPTION_UID is empty")
        return

    headers = {
        "Origin": "https://pocketoption.com",
        "User-Agent": (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) "
            "AppleWebKit/605.1.15 "
            "(KHTML, like Gecko) Version/18.0 "
            "Mobile/15E148 Safari/604.1"
        ),
    }

    print()
    print("[HEADERS]")
    print("Origin:", headers["Origin"])
    print("User-Agent:", headers["User-Agent"])

    session = None
    ws = None

    try:
        session = aiohttp.ClientSession(headers=headers)

        print()
        print("[1] CONNECTING...")

        started = time.monotonic()

        ws = await asyncio.wait_for(
            session.ws_connect(
                WS_URL,
                heartbeat=None,
                autoping=False,
                receive_timeout=None,
            ),
            timeout=15,
        )

        elapsed = time.monotonic() - started

        print(f"[OK] WebSocket connected in {elapsed:.2f}s")

        # ----------------------------------------------------------
        # ENGINE.IO OPEN
        # ----------------------------------------------------------

        print()
        print("[2] WAITING FOR ENGINE.IO OPEN (0...)")

        msg = await asyncio.wait_for(
            ws.receive(),
            timeout=15,
        )

        print(
            "[RAW 1]",
            msg.type,
            short_message(str(msg.data)),
        )

        if msg.type != aiohttp.WSMsgType.TEXT:
            print("[ERROR] Expected TEXT message")
            return

        data = msg.data

        if not data.startswith("0"):
            print(
                "[ERROR] Expected Engine.IO OPEN "
                "starting with 0"
            )
            return

        try:
            engine_open = json.loads(data[1:])
        except Exception as exc:
            print(
                "[ERROR] Cannot parse Engine.IO OPEN:",
                exc,
            )
            return

        print()
        print("[ENGINE.IO OPEN]")
        print(
            "sid:",
            mask(str(engine_open.get("sid", ""))),
        )
        print(
            "pingInterval:",
            engine_open.get("pingInterval"),
        )
        print(
            "pingTimeout:",
            engine_open.get("pingTimeout"),
        )

        # ----------------------------------------------------------
        # SOCKET.IO CONNECT
        # ----------------------------------------------------------

        print()
        print("[3] SEND SOCKET.IO CONNECT: 40")

        await ws.send_str("40")

        msg = await asyncio.wait_for(
            ws.receive(),
            timeout=15,
        )

        print(
            "[RAW 2]",
            msg.type,
            short_message(str(msg.data)),
        )

        if msg.type != aiohttp.WSMsgType.TEXT:
            print("[ERROR] Expected TEXT after 40")
            return

        data = msg.data

        if data.startswith("41"):
            print()
            print("=" * 70)
            print("RESULT: SERVER SENT 41 DURING SOCKET.IO CONNECT")
            print("=" * 70)
            return

        if not data.startswith("40"):
            print(
                "[ERROR] Expected Socket.IO CONNECT "
                "starting with 40"
            )
            return

        print("[OK] Socket.IO CONNECT accepted")

        # ----------------------------------------------------------
        # BROWSER-CONFIRMED AUTH
        # ----------------------------------------------------------

        auth_payload = {
            "sessionToken": SSID,
            "uid": UID,
            "lang": LANG,
            "currentUrl": CURRENT_URL,
            "isChart": int(IS_CHART),
        }

        packet = "42" + json.dumps(
            ["auth", auth_payload],
            ensure_ascii=False,
            separators=(",", ":"),
        )

        safe_packet = "42" + json.dumps(
            [
                "auth",
                {
                    "sessionToken": mask(SSID),
                    "uid": UID,
                    "lang": LANG,
                    "currentUrl": CURRENT_URL,
                    "isChart": int(IS_CHART),
                },
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        )

        print()
        print("[4] SEND BROWSER AUTH")
        print(safe_packet)

        await ws.send_str(packet)

        # ----------------------------------------------------------
        # AUTH RESPONSE
        # ----------------------------------------------------------

        print()
        print("[5] WAITING FOR SERVER RESPONSE AFTER AUTH")
        print()

        deadline = time.monotonic() + 20

        while time.monotonic() < deadline:
            remaining = max(
                0.1,
                deadline - time.monotonic(),
            )

            try:
                msg = await asyncio.wait_for(
                    ws.receive(),
                    timeout=min(5, remaining),
                )
            except asyncio.TimeoutError:
                print("[WAIT] No message yet...")
                continue

            now = time.strftime("%H:%M:%S")

            print(
                f"[{now}] TYPE={msg.type} "
                f"DATA={short_message(str(msg.data))}"
            )

            if msg.type == aiohttp.WSMsgType.TEXT:
                data = msg.data

                # Engine.IO ping
                if data == "2":
                    print(
                        "[ENGINE.IO] Ping received -> sending 3"
                    )
                    await ws.send_str("3")
                    continue

                # Engine.IO pong
                if data == "3":
                    print("[ENGINE.IO] Pong received")
                    continue

                # Socket.IO disconnect
                if data.startswith("41"):
                    print()
                    print("=" * 70)
                    print("RESULT: SERVER DISCONNECTED WITH 41")
                    print("=" * 70)
                    print()
                    print(
                        "This is the primary failure."
                    )
                    print(
                        "Authentication timeout is secondary."
                    )
                    break

                # Socket.IO event
                if data.startswith("42"):
                    try:
                        event = json.loads(data[2:])
                    except Exception:
                        event = None

                    if isinstance(event, list) and event:
                        event_name = event[0]

                        print(
                            "[SOCKET.IO EVENT]",
                            event_name,
                        )

                        if event_name in (
                            "auth/success",
                            "successauth",
                            "successAuth",
                        ):
                            print()
                            print("=" * 70)
                            print(
                                "RESULT: AUTHENTICATION SUCCESS"
                            )
                            print("=" * 70)
                            print()
                            break

                        if event_name in (
                            "auth/fail",
                            "auth/error",
                            "NotAuthorized",
                        ):
                            print()
                            print("=" * 70)
                            print(
                                "RESULT: AUTHENTICATION REJECTED"
                            )
                            print("=" * 70)
                            print()
                            break

                continue

            if msg.type == aiohttp.WSMsgType.BINARY:
                print(
                    "[BINARY]",
                    len(msg.data),
                    "bytes",
                )
                continue

            if msg.type == aiohttp.WSMsgType.CLOSE:
                print()
                print("=" * 70)
                print("RESULT: WEBSOCKET CLOSED")
                print("=" * 70)
                print("close_code:", ws.close_code)
                print("data:", msg.data)
                break

            if msg.type == aiohttp.WSMsgType.CLOSED:
                print()
                print("=" * 70)
                print("RESULT: WEBSOCKET CLOSED")
                print("=" * 70)
                print("close_code:", ws.close_code)
                break

            if msg.type == aiohttp.WSMsgType.ERROR:
                print()
                print("=" * 70)
                print("RESULT: WEBSOCKET ERROR")
                print("=" * 70)
                print(msg.data)
                break

        else:
            print()
            print("=" * 70)
            print("RESULT: AUTH RESPONSE TIMEOUT")
            print("=" * 70)

    except asyncio.TimeoutError:
        print()
        print("=" * 70)
        print("ERROR: CONNECTION / HANDSHAKE TIMEOUT")
        print("=" * 70)

    except Exception as exc:
        print()
        print("=" * 70)
        print("ERROR:", type(exc).__name__)
        print(str(exc))
        print("=" * 70)

    finally:
        print()
        print("[CLEANUP]")

        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass

        if session is not None:
            try:
                await session.close()
            except Exception:
                pass

        print("[DONE]")


if __name__ == "__main__":
    asyncio.run(main())
