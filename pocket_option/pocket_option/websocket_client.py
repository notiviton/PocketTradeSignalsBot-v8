"""
Pocket Option WebSocket client.

Только получение рыночных данных:
ticks, history, candles, assets и updateStream.
Автоматическая торговля отсутствует.
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


TickCallback = Callable[[str, float, float], Awaitable[None]] | None


@dataclass
class PocketOptionCandle:
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
    """Компактный Socket.IO/Engine.IO клиент Pocket Option."""

    DEFAULT_WS_URL = (
        "wss://api-spb.po.market/socket.io/"
        "?EIO=4&transport=websocket"
    )
    DEFAULT_HISTORY_OFFSET = 9000
    DEFAULT_LANG = "ru"
    DEFAULT_CURRENT_URL = "cabinet"

    PING_INTERVAL = 25
    PING_TIMEOUT = 20
    RECONNECT_MIN = 1
    RECONNECT_MAX = 30
    MAX_TICKS = 5000
    MAX_HISTORY = 5000
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
        self.ssid = str(
            ssid if ssid is not None else os.getenv("POCKET_OPTION_SSID", "")
        ).strip()
        self.uid = str(
            uid if uid is not None else os.getenv("POCKET_OPTION_UID", "")
        ).strip()
        self.ws_url = str(
            ws_url
            if ws_url is not None
            else os.getenv("POCKET_OPTION_WS_URL", self.DEFAULT_WS_URL)
        ).strip()
        self.lang = str(
            lang
            if lang is not None
            else os.getenv("POCKET_OPTION_LANG", self.DEFAULT_LANG)
        ).strip() or self.DEFAULT_LANG
        self.current_url = str(
            current_url
            if current_url is not None
            else os.getenv(
                "POCKET_OPTION_CURRENT_URL",
                self.DEFAULT_CURRENT_URL,
            )
        ).strip() or self.DEFAULT_CURRENT_URL

        env_chart = os.getenv("POCKET_OPTION_IS_CHART")
        self.is_chart = (
            env_chart.strip().lower() not in {"0", "false", "no", "off"}
            if env_chart is not None
            else bool(is_chart)
        )
        self.tick_callback = tick_callback

        self.session: aiohttp.ClientSession | None = None
        self.ws: aiohttp.ClientWebSocketResponse | None = None
        self.reader_task: asyncio.Task[None] | None = None
        self.lifecycle_task: asyncio.Task[None] | None = None

        self._connection_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self.auth_event = asyncio.Event()
        self.socketio_event = asyncio.Event()

        self.connected = False
        self.socketio_connected = False
        self.authenticated = False

        self.engine_sid: str | None = None
        self.socketio_sid: str | None = None
        self.last_error: str | None = None
        self.last_message: str | None = None
        self.auth_sent_at: float | None = None
        self.auth_attempt = 0

        self.ping_interval = self.PING_INTERVAL
        self.ping_timeout = self.PING_TIMEOUT

        self.server_time_offset = 0.0
        self.server_time_synced = False

        self.ticks: dict[str, deque[tuple[float, float]]] = {}
        self.history: dict[tuple[str, int], list[PocketOptionCandle]] = {}
        self.assets: list[Any] = []
        self.subscriptions: set[tuple[str, int]] = set()

        self.update_stream_count = 0
        self.last_update_stream_at: float | None = None
        self.last_update_stream_symbol: str | None = None
        self.last_update_stream_price: float | None = None
        self.last_update_stream_timestamp: float | None = None

        self._history_index = 0
        self._history_waiters: dict[
            int, asyncio.Future[list[PocketOptionCandle]]
        ] = {}
        self._history_meta: dict[int, tuple[str, int]] = {}

        self._binary_event: dict[str, Any] | None = None
        self._binary_parts: dict[int, bytes] = {}

    @staticmethod
    def timeframe_to_seconds(timeframe: str) -> int:
        value = str(timeframe).strip().lower()
        mapping = {
            "1m": 60, "3m": 180, "5m": 300, "10m": 600,
            "15m": 900, "30m": 1800, "1h": 3600, "2h": 7200,
            "4h": 14400, "6h": 21600, "12h": 43200, "1d": 86400,
        }
        if value in mapping:
            return mapping[value]
        if value.isdigit() and int(value) > 0:
            return int(value)
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        value = str(symbol).strip()
        if not value:
            raise ValueError("Symbol is empty")
        upper = value.upper()

        if upper.endswith(" OTC"):
            return upper[:-4].replace("/", "") + "_otc"
        if upper.endswith("_OTC"):
            return upper[:-4].replace("/", "") + "_otc"
        if upper.endswith("OTC"):
            return upper[:-3].replace("/", "") + "_otc"
        return value

    @staticmethod
    def _normalize_timestamp(value: Any) -> int | None:
        try:
            value = float(value)
        except (TypeError, ValueError):
            return None
        if value <= 0:
            return None
        if value > 10_000_000_000:
            value /= 1000.0
        return int(value)

    def _server_timestamp(self) -> int:
        return int(time.time() + self.server_time_offset)

    def _next_history_index(self) -> int:
        self._history_index += 1
        return self._history_index

    def _ssid_fingerprint(self) -> str:
        return hashlib.sha256(self.ssid.encode()).hexdigest()[:16]

    def _sync_time_from_headers(self, headers: Any) -> None:
        try:
            value = headers.get("Date")
            if not value:
                return
            server = email.utils.parsedate_to_datetime(value).timestamp()
            self.server_time_offset = server - time.time()
            self.server_time_synced = True
        except Exception:
            pass

    async def connect(self) -> None:
        async with self._connection_lock:
            if self._stop_event.is_set():
                raise PocketOptionWebSocketError("WebSocket client is stopped")
            if self.connected and self.socketio_connected and self.authenticated:
                return

            await self._cleanup_connection(preserve_lifecycle=True)

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
            self.socketio_event.clear()
            self.authenticated = False
            self.socketio_connected = False
            self.connected = False
            self._binary_event = None
            self._binary_parts.clear()

            headers = {
                "Origin": "https://pocketoption.com",
                "User-Agent": (
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) "
                    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                    "Version/18.0 Mobile/15E148 Safari/604.1"
                ),
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }

            self.session = aiohttp.ClientSession(headers=headers)

            try:
                self.ws = await self.session.ws_connect(
                    self.ws_url,
                    heartbeat=None,
                    autoping=False,
                    receive_timeout=None,
                )

                await self._wait_engine_open()

                self.reader_task = asyncio.create_task(self._reader_loop())
                await self._send_raw("40")

                await asyncio.wait_for(
                    self.socketio_event.wait(),
                    timeout=self.ping_timeout,
                )

                await self.send_auth()
                await self.wait_authenticated(timeout=15.0)
                self.connected = True

            except Exception:
                await self._cleanup_connection(
                    preserve_lifecycle=True
                )
                raise

    async def _wait_engine_open(self) -> None:
        if self.ws is None:
            raise PocketOptionWebSocketError("WebSocket is not initialized")

        while True:
            message = await self.ws.receive(timeout=self.ping_timeout)

            if message.type == aiohttp.WSMsgType.TEXT:
                data = message.data
                self.last_message = data

                if not data.startswith("0"):
                    continue

                try:
                    info = json.loads(data[1:])
                except json.JSONDecodeError:
                    info = {}

                self.engine_sid = info.get("sid")
                self.ping_interval = float(
                    info.get("pingInterval", self.PING_INTERVAL * 1000)
                ) / 1000.0
                self.ping_timeout = float(
                    info.get("pingTimeout", self.PING_TIMEOUT * 1000)
                ) / 1000.0
                return

            if message.type in {
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.CLOSED,
                aiohttp.WSMsgType.ERROR,
            }:
                raise PocketOptionWebSocketError(
                    "WebSocket closed before Engine.IO OPEN"
                )

    async def send_auth(self) -> None:
        if self.ws is None:
            raise PocketOptionWebSocketError("WebSocket is not initialized")
        if not self.socketio_connected:
            raise PocketOptionWebSocketError(
                "Socket.IO is not connected before AUTH"
            )
        if not self.ssid:
            raise PocketOptionWebSocketError("POCKET_OPTION_SSID is empty")

        self.auth_event.clear()
        self.authenticated = False
        self.auth_attempt += 1
        self.auth_sent_at = time.monotonic()

        await self._send_event(
            "auth",
            {
                "sessionToken": self.ssid,
                "uid": self.uid,
                "lang": self.lang,
                "currentUrl": self.current_url,
                "isChart": 1 if self.is_chart else 0,
            },
        )

    async def wait_authenticated(self, timeout: float = 20.0) -> bool:
        if self.authenticated:
            return True

        try:
            await asyncio.wait_for(
                self.auth_event.wait(),
                timeout=timeout,
            )
        except asyncio.TimeoutError as exc:
            raise PocketOptionWebSocketError(
                "Pocket Option AUTH timeout: auth/success not received"
            ) from exc

        return self.authenticated

    async def _send_raw(self, data: str) -> None:
        if self.ws is None or self.ws.closed:
            raise PocketOptionWebSocketError(
                "WebSocket is not connected"
            )
        await self.ws.send_str(data)

    async def _send_event(self, name: str, data: Any) -> None:
        payload = json.dumps(
            [name, data],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        await self._send_raw("42" + payload)

    async def _reader_loop(self) -> None:
        if self.ws is None:
            return

        try:
            async for message in self.ws:
                if message.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_message(message.data)

                elif message.type == aiohttp.WSMsgType.BINARY:
                    await self._handle_binary(message.data)

                elif message.type in {
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR,
                }:
                    break

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            self.last_error = str(exc)

        finally:
            self.connected = False
            self.socketio_connected = False
            self.authenticated = False
            self._fail_history_waiters(
                "WebSocket connection lost"
            )

    async def _handle_message(self, data: str) -> None:
        self.last_message = data

        if data == "2":
            await self._send_raw("3")
            return

        if data == "3":
            return

        if data.startswith("0"):
            return

        if data.startswith("40"):
            self.socketio_connected = True
            self.socketio_event.set()

            if len(data) > 2:
                try:
                    info = json.loads(data[2:])
                    if isinstance(info, dict):
                        self.socketio_sid = info.get("sid")
                except json.JSONDecodeError:
                    pass
            return

        if data.startswith("41"):
            self.socketio_connected = False
            self.authenticated = False
            self.last_error = (
                "Pocket Option Socket.IO DISCONNECT (41)"
            )
            self._fail_history_waiters(
                "Socket.IO DISCONNECT (41)"
            )
            return

        if data.startswith("45"):
            await self._handle_binary_header(data)
            return

        if data.startswith("42"):
            try:
                event = json.loads(data[2:])
            except json.JSONDecodeError:
                return

            if isinstance(event, list) and len(event) >= 2:
                await self._handle_socketio_event(
                    event[0],
                    event[1],
                )
            return

        try:
            raw = json.loads(data)
        except json.JSONDecodeError:
            return

        if isinstance(raw, dict):
            await self._handle_history_result(
                raw,
                "raw-json",
            )

    async def _handle_socketio_event(
        self,
        event_name: str,
        data: Any,
    ) -> None:
        if event_name in {
            "auth/success",
            "successauth",
        }:
            self.authenticated = True
            self.auth_event.set()
            return

        if event_name in {
            "auth/fail",
            "auth/error",
            "NotAuthorized",
        }:
            self.authenticated = False
            self.last_error = (
                f"Authentication failed: {data}"
            )
            self.auth_event.set()
            return

        if event_name == "updateAssets":
            self.assets = data if isinstance(data, list) else []
            return

        if event_name == "updateStream":
            await self._handle_update_stream(data)
            return

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
                data,
            )

    async def _handle_binary_header(self, data: str) -> None:
        body = data[2:]

        if "-" not in body:
            return

        count_text, json_text = body.split("-", 1)

        try:
            count = int(count_text)
            event = json.loads(json_text)
        except (ValueError, json.JSONDecodeError):
            return

        if (
            count <= 0
            or count > self.MAX_BINARY_ATTACHMENTS
            or not isinstance(event, list)
            or len(event) < 2
        ):
            return

        self._binary_event = {
            "count": count,
            "event": event,
        }
        self._binary_parts.clear()

    async def _handle_binary(self, data: bytes) -> None:
        if self._binary_event is None:
            try:
                raw = json.loads(
                    data.decode("utf-8")
                )
            except Exception:
                return

            if isinstance(raw, dict):
                await self._handle_history_result(
                    raw,
                    "binary-json",
                )
            return

        index = len(self._binary_parts)
        self._binary_parts[index] = data

        if len(self._binary_parts) < self._binary_event["count"]:
            return

        event = self._binary_event["event"]

        for part_index, part in self._binary_parts.items():
            event = self._replace_binary(
                event,
                part_index,
                part,
            )

        self._binary_event = None
        self._binary_parts.clear()

        if isinstance(event, list) and len(event) >= 2:
            await self._handle_socketio_event(
                event[0],
                event[1],
            )

    @classmethod
    def _replace_binary(
        cls,
        obj: Any,
        index: int,
        value: bytes,
    ) -> Any:
        if isinstance(obj, dict):
            if (
                obj.get("_placeholder")
                and obj.get("num") == index
            ):
                try:
                    return json.loads(
                        value.decode("utf-8")
                    )
                except Exception:
                    return value

            return {
                key: cls._replace_binary(
                    item,
                    index,
                    value,
                )
                for key, item in obj.items()
            }

        if isinstance(obj, list):
            return [
                cls._replace_binary(
                    item,
                    index,
                    value,
                )
                for item in obj
            ]

        return obj

    async def _handle_update_stream(
        self,
        data: Any,
    ) -> None:
        self.update_stream_count += 1
        self.last_update_stream_at = time.time()

        items = data if isinstance(data, list) else [data]

        for item in items:
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
                raw_time = (
                    item.get("timestamp")
                    if item.get("timestamp") is not None
                    else item.get("time")
                )

                if raw_symbol is not None:
                    symbol = str(raw_symbol)

                try:
                    if raw_price is not None:
                        price = float(raw_price)
                except (TypeError, ValueError):
                    pass

                try:
                    if raw_time is not None:
                        timestamp = float(raw_time)
                except (TypeError, ValueError):
                    pass

            elif (
                isinstance(item, (list, tuple))
                and len(item) >= 3
                and isinstance(item[0], str)
            ):
                symbol = item[0]
                try:
                    timestamp = float(item[1])
                    price = float(item[2])
                except (TypeError, ValueError):
                    pass

            if symbol is None or price is None:
                continue

            if timestamp is None:
                timestamp = time.time()

            self.last_update_stream_symbol = symbol
            self.last_update_stream_price = price
            self.last_update_stream_timestamp = timestamp

            await self._store_tick(
                symbol,
                price,
                timestamp,
            )

    async def _store_tick(
        self,
        symbol: str,
        price: float,
        timestamp: float,
    ) -> None:
        normalized = self.normalize_symbol(symbol)

        self.ticks.setdefault(
            normalized,
            deque(maxlen=self.MAX_TICKS),
        ).append(
            (timestamp, price)
        )

        if self.tick_callback:
            try:
                await self.tick_callback(
                    normalized,
                    price,
                    timestamp,
                )
            except Exception as exc:
                self.last_error = (
                    f"Tick callback error: {exc}"
                )

    async def wait_for_tick(
        self,
        symbol: str,
        timeout: float = 10.0,
    ) -> tuple[float, float] | None:
        normalized = self.normalize_symbol(symbol)

        existing = self.ticks.get(normalized)
        if existing:
            return existing[-1]

        await self.subscribe(
            normalized,
            60,
        )

        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            existing = self.ticks.get(normalized)

            if existing:
                return existing[-1]

            await asyncio.sleep(0.2)

        return None

    def get_ticks(
        self,
        symbol: str,
        limit: int = 100,
    ) -> list[tuple[float, float]]:
        values = list(
            self.ticks.get(
                self.normalize_symbol(symbol),
                [],
            )
        )
        return values[-max(0, int(limit)):]

    async def subscribe(
        self,
        symbol: str,
        period: int = 60,
    ) -> None:
        await self.wait_authenticated()

        normalized = self.normalize_symbol(symbol)
        period = int(period)

        await self._send_event(
            "changeSymbol",
            {
                "asset": normalized,
                "period": period,
            },
        )

        await asyncio.sleep(0.2)

        await self._send_event(
            "subfor",
            normalized,
        )

        self.subscriptions.add(
            (normalized, period)
        )

    async def request_history(
        self,
        symbol: str,
        period: int = 60,
        count: int = 500,
        timeout: float = 30.0,
        offset: int | None = None,
    ) -> list[PocketOptionCandle]:
        await self.wait_authenticated(
            timeout=timeout
        )

        normalized = self.normalize_symbol(symbol)
        period = int(period)
        requested_count = max(1, int(count))

        history_offset = max(
            requested_count,
            (
                int(offset)
                if offset is not None
                else self.DEFAULT_HISTORY_OFFSET
            ),
        )

        index = self._next_history_index()

        loop = asyncio.get_running_loop()
        waiter = loop.create_future()

        self._history_waiters[index] = waiter
        self._history_meta[index] = (
            normalized,
            period,
        )

        try:
            await self.subscribe(
                normalized,
                period,
            )

            await asyncio.sleep(1.0)

            await self._send_event(
                "loadHistoryPeriod",
                {
                    "asset": normalized,
                    "index": index,
                    "time": self._server_timestamp(),
                    "offset": history_offset,
                    "period": period,
                },
            )

            result = await asyncio.wait_for(
                waiter,
                timeout=timeout,
            )

            return result[-requested_count:]

        except asyncio.TimeoutError as exc:
            cached = self.get_cached_candles(
                normalized,
                period,
                requested_count,
            )

            if cached:
                return cached

            raise PocketOptionWebSocketError(
                f"History timeout: {normalized} "
                f"period={period} "
                f"offset={history_offset} "
                f"index={index}"
            ) from exc

        finally:
            self._history_waiters.pop(
                index,
                None,
            )
            self._history_meta.pop(
                index,
                None,
            )

    async def _handle_history_event(
        self,
        event_name: str,
        data: Any,
    ) -> None:
        if isinstance(data, dict) and "index" in data:
            await self._handle_history_result(
                data,
                event_name,
            )
            return

        if isinstance(data, dict):
            for key in (
                "result",
                "data",
            ):
                nested = data.get(key)

                if (
                    isinstance(nested, dict)
                    and "index" in nested
                ):
                    await self._handle_history_result(
                        nested,
                        event_name,
                    )
                    return

        candles = self._extract_candles(
            data
        )

        symbol, period = self._infer_symbol_period(
            data
        )

        if not candles:
            items = self._extract_history_items(
                data
            )

            if items and period:
                candles = self._build_candles_from_history_items(
                    items,
                    period,
                )

        if not candles or symbol is None or period is None:
            return

        candles = self._merge_history_with_recent_ticks(
            symbol,
            period,
            candles,
        )

        self.history[
            (symbol, period)
        ] = candles[-self.MAX_HISTORY:]

        for index, meta in list(
            self._history_meta.items()
        ):
            if meta != (symbol, period):
                continue

            waiter = self._history_waiters.get(index)

            if waiter and not waiter.done():
                waiter.set_result(
                    self.history[
                        (symbol, period)
                    ]
                )

            return

    async def _handle_history_result(
        self,
        result: dict[str, Any],
        event_name: str,
    ) -> None:
        try:
            index = int(
                result.get("index")
            )
        except (TypeError, ValueError):
            index = None

        fallback_symbol = None
        fallback_period = None

        if (
            index is not None
            and index in self._history_meta
        ):
            (
                fallback_symbol,
                fallback_period,
            ) = self._history_meta[index]

        symbol, period = self._infer_symbol_period(
            result,
            fallback_symbol,
            fallback_period,
        )

        period = period or 60

        items = self._extract_history_items(
            result
        )

        candles = self._build_candles_from_history_items(
            items,
            period,
        )

        if not candles:
            candles = self._extract_candles(
                result,
                period,
            )

        if not candles or symbol is None:
            return

        candles = self._merge_history_with_recent_ticks(
            symbol,
            period,
            candles,
        )

        candles = candles[-self.MAX_HISTORY:]

        self.history[
            (symbol, period)
        ] = candles

        if index is not None:
            waiter = self._history_waiters.get(index)

            if waiter and not waiter.done():
                waiter.set_result(candles)

    @staticmethod
    def _extract_history_items(
        data: Any,
    ) -> list[Any]:
        if isinstance(data, list):
            return data

        if not isinstance(data, dict):
            return []

        for key in (
            "data",
            "candles",
            "history",
            "items",
            "values",
        ):
            value = data.get(key)

            if isinstance(value, list):
                return value

        result = data.get("result")

        if isinstance(result, list):
            return result

        if isinstance(result, dict):
            return PocketOptionWebSocketClient._extract_history_items(
                result
            )

        return []

    def _infer_symbol_period(
        self,
        data: Any,
        fallback_symbol: str | None = None,
        fallback_period: int | None = None,
    ) -> tuple[str | None, int | None]:
        symbol = fallback_symbol
        period = fallback_period

        if isinstance(data, dict):
            raw_symbol = (
                data.get("asset")
                or data.get("symbol")
                or data.get("active")
            )

            if raw_symbol:
                try:
                    symbol = self.normalize_symbol(
                        str(raw_symbol)
                    )
                except ValueError:
                    pass

            raw_period = (
                data.get("period")
                or data.get("timeframe")
            )

            if raw_period is not None:
                try:
                    period = int(raw_period)
                except (TypeError, ValueError):
                    pass

            if (
                symbol is None
                or period is None
            ):
                nested = data.get("result")

                if isinstance(nested, dict):
                    return self._infer_symbol_period(
                        nested,
                        symbol,
                        period,
                    )

        if symbol is None and self.subscriptions:
            symbol = next(
                iter(self.subscriptions)
            )[0]

        if period is None and symbol is not None:
            for (
                sub_symbol,
                sub_period,
            ) in self.subscriptions:
                if sub_symbol == symbol:
                    period = sub_period
                    break

        return symbol, period

    @classmethod
    def _build_candles_from_history_items(
        cls,
        items: list[Any],
        period: int,
    ) -> list[PocketOptionCandle]:
        if not items:
            return []

        candles = [
            candle
            for item in items
            if (
                candle := cls._parse_candle(item)
            )
        ]

        if candles:
            unique = {
                candle.timestamp: candle
                for candle in candles
            }

            return sorted(
                unique.values(),
                key=lambda candle: candle.timestamp,
            )

        ticks = [
            tick
            for item in items
            if (
                tick := cls._parse_history_tick(item)
            )
        ]

        return cls._compile_ticks_to_candles(
            ticks,
            period,
        )

    @staticmethod
    def _parse_history_tick(
        item: Any,
    ) -> tuple[int, float] | None:
        if isinstance(item, dict):
            timestamp = item.get("time")

            if timestamp is None:
                timestamp = item.get("timestamp")

            price = item.get("price")

            if price is None:
                price = item.get("value")

        elif (
            isinstance(item, (list, tuple))
            and len(item) >= 2
        ):
            try:
                first = float(item[0])
                second = float(item[1])
            except (TypeError, ValueError):
                return None

            if first > 1_000_000_000:
                timestamp, price = first, second
            elif second > 1_000_000_000:
                timestamp, price = second, first
            else:
                return None

        else:
            return None

        timestamp = PocketOptionWebSocketClient._normalize_timestamp(
            timestamp
        )

        if timestamp is None:
            return None

        try:
            return timestamp, float(price)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _compile_ticks_to_candles(
        ticks: list[tuple[int, float]],
        period: int,
    ) -> list[PocketOptionCandle]:
        if period <= 0:
            return []

        buckets: dict[
            int,
            list[tuple[int, float]],
        ] = {}

        for timestamp, price in ticks:
            bucket = (
                int(timestamp) // period
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

        result: list[PocketOptionCandle] = []

        for bucket, values in sorted(
            buckets.items()
        ):
            values.sort()

            prices = [
                price
                for _, price in values
            ]

            result.append(
                PocketOptionCandle(
                    timestamp=bucket,
                    open=prices[0],
                    high=max(prices),
                    low=min(prices),
                    close=prices[-1],
                    volume=float(len(prices)),
                )
            )

        return result

    @staticmethod
    def _parse_candle(
        item: Any,
    ) -> PocketOptionCandle | None:
        if isinstance(item, dict):
            timestamp = item.get("timestamp")

            if timestamp is None:
                timestamp = item.get("time")

            if timestamp is None:
                timestamp = item.get("from")

            values = (
                item.get("open"),
                item.get("high"),
                item.get("low"),
                item.get("close"),
            )

            if (
                timestamp is None
                or any(value is None for value in values)
            ):
                return None

            try:
                timestamp = PocketOptionWebSocketClient._normalize_timestamp(
                    timestamp
                )

                if timestamp is None:
                    return None

                return PocketOptionCandle(
                    timestamp=timestamp,
                    open=float(values[0]),
                    high=float(values[1]),
                    low=float(values[2]),
                    close=float(values[3]),
                    volume=float(
                        item.get("volume", 0)
                    ),
                )

            except (TypeError, ValueError):
                return None

        if (
            isinstance(item, (list, tuple))
            and len(item) >= 5
        ):
            try:
                timestamp = PocketOptionWebSocketClient._normalize_timestamp(
                    item[0]
                )

                if timestamp is None:
                    return None

                a, b, c, d = (
                    float(item[1]),
                    float(item[2]),
                    float(item[3]),
                    float(item[4]),
                )

            except (TypeError, ValueError):
                return None

            candidates = (
                (a, b, c, d),
                (a, c, d, b),
            )

            valid = [
                candidate
                for candidate in candidates
                if (
                    candidate[1]
                    >= max(
                        candidate[0],
                        candidate[3],
                    )
                    and candidate[2]
                    <= min(
                        candidate[0],
                        candidate[3],
                    )
                    and candidate[1]
                    >= candidate[2]
                )
            ]

            if not valid:
                return None

            open_value, high_value, low_value, close_value = valid[-1]

            try:
                volume = (
                    float(item[5])
                    if len(item) > 5
                    else 0.0
                )
            except (TypeError, ValueError):
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

    def _extract_candles(
        self,
        data: Any,
        period: int | None = None,
    ) -> list[PocketOptionCandle]:
        items = self._extract_history_items(data)

        candles = [
            candle
            for item in items
            if (
                candle := self._parse_candle(item)
            )
        ]

        if candles:
            unique = {
                candle.timestamp: candle
                for candle in candles
            }

            return sorted(
                unique.values(),
                key=lambda candle: candle.timestamp,
            )

        if period:
            ticks = [
                tick
                for item in items
                if (
                    tick := self._parse_history_tick(item)
                )
            ]

            return self._compile_ticks_to_candles(
                ticks,
                period,
            )

        return []

    def _merge_history_with_recent_ticks(
        self,
        symbol: str,
        period: int,
        candles: list[PocketOptionCandle],
    ) -> list[PocketOptionCandle]:
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

        recent: list[tuple[int, float]] = []

        for timestamp, price in values:
            normalized = self._normalize_timestamp(
                timestamp
            )

            if (
                normalized is not None
                and normalized >= history_last
            ):
                recent.append(
                    (
                        normalized,
                        float(price),
                    )
                )

        if not recent:
            return candles

        live = self._compile_ticks_to_candles(
            recent,
            period,
        )

        merged = {
            candle.timestamp: candle
            for candle in candles
        }

        merged.update(
            {
                candle.timestamp: candle
                for candle in live
            }
        )

        return sorted(
            merged.values(),
            key=lambda candle: candle.timestamp,
        )

    def get_cached_candles(
        self,
        symbol: str,
        period: int = 60,
        limit: int = 500,
    ) -> list[PocketOptionCandle]:
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
            -max(0, int(limit)):
        ]

    def get_history(
        self,
        symbol: str,
        period: int = 60,
        limit: int = 500,
    ) -> list[PocketOptionCandle]:
        return self.get_cached_candles(
            symbol,
            period,
            limit,
        )

    def get_assets(self) -> list[Any]:
        return list(self.assets)

    def status(self) -> dict[str, Any]:
        return {
            "connected": self.connected,
            "socketio_connected": self.socketio_connected,
            "authenticated": self.authenticated,
            "engine_sid": self.engine_sid,
            "socketio_sid": self.socketio_sid,
            "known_symbols": list(self.ticks.keys()),
            "assets_count": len(self.assets),
            "history_cached": {
                f"{symbol}:{period}": len(candles)
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
                for symbol, period in self.subscriptions
            ],
            "pending_history_requests": [
                {
                    "index": index,
                    "symbol": meta[0],
                    "period": meta[1],
                }
                for index, meta in self._history_meta.items()
            ],
            "server_time_synced": self.server_time_synced,
            "server_time_offset": self.server_time_offset,
            "update_stream_count": self.update_stream_count,
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

    async def start(self) -> None:
        if self.connected and self.authenticated:
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

    async def _lifecycle_loop(self) -> None:
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
                        self.last_error = str(exc)

                if self._stop_event.is_set():
                    return

                await self._cleanup_connection(
                    preserve_lifecycle=True
                )

                if self._stop_event.is_set():
                    return

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
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.last_error = str(exc)
                    delay = min(
                        delay * 2,
                        self.RECONNECT_MAX,
                    )

        except asyncio.CancelledError:
            raise

    async def _cleanup_connection(
        self,
        preserve_lifecycle: bool = True,
    ) -> None:
        self.connected = False
        self.socketio_connected = False
        self.authenticated = False
        self.socketio_event.clear()

        self._fail_history_waiters(
            "connection cleanup"
        )

        current = asyncio.current_task()

        if (
            self.reader_task is not None
            and self.reader_task is not current
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

        if not preserve_lifecycle:
            self.lifecycle_task = None

    def _fail_history_waiters(
        self,
        reason: str,
    ) -> None:
        for waiter in self._history_waiters.values():
            if not waiter.done():
                waiter.set_exception(
                    PocketOptionWebSocketError(
                        f"History request failed: {reason}"
                    )
                )

        self._history_waiters.clear()
        self._history_meta.clear()

    async def stop(self) -> None:
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

        self.lifecycle_task = None

        await self._cleanup_connection(
            preserve_lifecycle=False
        )

        self.auth_event.clear()

    async def close(self) -> None:
        """Совместимость с ForexService.stop()."""
        await self.stop()

    async def run_forever(
        self,
        reconnect: bool = True,
    ) -> None:
        self._stop_event.clear()

        if not reconnect:
            await self.connect()

            if self.reader_task:
                await self.reader_task

            return

        await self.start()

        if self.lifecycle_task:
            await self.lifecycle_task
