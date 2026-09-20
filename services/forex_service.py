import asyncio
from typing import Any
from data.models import Candle, MarketData
from data.provider import MarketDataError
from pocket_option.pocket_option.websocket_client import (
    PocketOptionCandle,
    PocketOptionWebSocketClient,
    PocketOptionWebSocketError,
)
class ForexService:
    """
    Сервис получения рыночных данных.
    Источник данных:
        Pocket Option WebSocket
    Автоматическое открытие сделок здесь отсутствует.
    """
    DEFAULT_SOURCE = "pocket_otc"
    DEFAULT_SYMBOL = "EUR/USD OTC"
    DEFAULT_TIMEFRAME = "1m"
    DEFAULT_LIMIT = 500
    def __init__(
        self,
        client: PocketOptionWebSocketClient | None = None,
    ) -> None:
        self.client = client or PocketOptionWebSocketClient()
        self._started = False
        self._start_lock = asyncio.Lock()
    async def start(self) -> None:
        """Запустить Pocket Option WebSocket."""
        async with self._start_lock:
            if self._started:
                return
            await self.client.connect()
            self._started = True
    async def stop(self) -> None:
        """Остановить Pocket Option WebSocket."""
        async with self._start_lock:
            if not self._started:
                return
            await self.client.close()
            self._started = False
    async def _ensure_started(self) -> None:
        """Убедиться, что WebSocket запущен."""
        if not self._started:
            await self.start()
    @staticmethod
    def _timeframe_to_seconds(timeframe: str) -> int:
        """
        Преобразовать таймфрейм в секунды.
        Поддерживаемые варианты:
            1m, 5m, 15m, 30m, 1h, 4h, 1d
        """
        value = str(timeframe).strip().lower()
        mapping = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
            "4h": 14400,
            "1d": 86400,
        }
        if value in mapping:
            return mapping[value]
        if value.endswith("m"):
            try:
                minutes = int(value[:-1])
                if minutes > 0:
                    return minutes * 60
            except ValueError:
                pass
        if value.endswith("h"):
            try:
                hours = int(value[:-1])
                if hours > 0:
                    return hours * 3600
            except ValueError:
                pass
        if value.endswith("d"):
            try:
                days = int(value[:-1])
                if days > 0:
                    return days * 86400
            except ValueError:
                pass
        raise MarketDataError(
            f"Неподдерживаемый таймфрейм: {timeframe}"
        )
    async def get_market(
        self,
        symbol: str | None = None,
        timeframe: str | None = None,
        limit: int = DEFAULT_LIMIT,
        source: str = DEFAULT_SOURCE,
    ) -> MarketData:
        """
        Получить свечи рынка из Pocket Option.
        Важно:
        offset намеренно не передаётся в request_history().
        Поэтому используется DEFAULT_HISTORY_OFFSET клиента
        PocketOptionWebSocketClient, сейчас это 9000.
        """
        await self._ensure_started()
        if source != self.DEFAULT_SOURCE:
            raise MarketDataError(
                f"Источник {source!r} не поддерживается. "
                "Используется только Pocket Option."
            )
        symbol = symbol or self.DEFAULT_SYMBOL
        timeframe = timeframe or self.DEFAULT_TIMEFRAME
        try:
            limit = int(limit)
        except (TypeError, ValueError) as exc:
            raise MarketDataError(
                f"Некорректный limit: {limit!r}"
            ) from exc
        if limit <= 0:
            raise MarketDataError(
                f"limit должен быть больше 0: {limit}"
            )
        try:
            period = self._timeframe_to_seconds(timeframe)
            provider_symbol = self.client.normalize_symbol(symbol)
            # ВАЖНО:
            # offset здесь НЕ передаём.
            #
            # request_history() сам использует:
            # PocketOptionWebSocketClient.DEFAULT_HISTORY_OFFSET = 9000
            #
            # Раньше здесь было:
            # offset=max(int(limit), 100)
            #
            # При limit=500 это отправляло offset=500.
            candles = await self.client.request_history(
                provider_symbol,
                period,
                timeout=20.0,
            )
            # Если WebSocket вернул меньше требуемого количества,
            # дополнительно проверяем локальный cache.
            if len(candles) < limit:
                cached = self.client.get_cached_candles(
                    provider_symbol,
                    period,
                )
                if cached and len(cached) > len(candles):
                    candles = cached
            if not candles:
                raise MarketDataError(
                    f"Pocket Option не вернул свечи: "
                    f"{provider_symbol}, period={period}"
                )
            # Берём последние limit свечей.
            candles = candles[-limit:]
            market_candles = [
                self._convert_candle(candle)
                for candle in candles
            ]
            self._validate_market_candles(market_candles)
            return MarketData(
                symbol=symbol,
                timeframe=timeframe,
                candles=market_candles,
                source=self.DEFAULT_SOURCE,
            )
        except MarketDataError:
            raise
        except PocketOptionWebSocketError as exc:
            raise MarketDataError(
                f"Ошибка Pocket Option WebSocket: {exc}"
            ) from exc
        except Exception as exc:
            raise MarketDataError(
                f"Ошибка получения рыночных данных: {exc}"
            ) from exc
    async def get_last_prices(
        self,
        symbol: str | None = None,
        timeframe: str | None = None,
    ) -> dict[str, Any]:
        """
        Получить последние данные по инструменту.
        """
        await self._ensure_started()
        symbol = symbol or self.DEFAULT_SYMBOL
        timeframe = timeframe or self.DEFAULT_TIMEFRAME
        period = self._timeframe_to_seconds(timeframe)
        provider_symbol = self.client.normalize_symbol(symbol)
        try:
            await self.client.subscribe(
                provider_symbol,
                period,
            )
            tick = await self.client.wait_for_tick(
                provider_symbol,
                timeout=10.0,
            )
            if tick is None:
                raise MarketDataError(
                    f"Pocket Option не вернул последний тик: "
                    f"{provider_symbol}"
                )
            return {
                "symbol": symbol,
                "timeframe": timeframe,
                "timestamp": tick[0],
                "price": tick[1],
            }
        except MarketDataError:
            raise
        except PocketOptionWebSocketError as exc:
            raise MarketDataError(
                f"Ошибка Pocket Option WebSocket: {exc}"
            ) from exc
        except Exception as exc:
            raise MarketDataError(
                f"Ошибка получения последней цены: {exc}"
            ) from exc
    def status(self) -> dict[str, Any]:
        """Вернуть диагностический статус WebSocket."""
        try:
            return self.client.status()
        except Exception as exc:
            return {
                "started": self._started,
                "error": str(exc),
            }
    @staticmethod
    def _convert_candle(
        candle: PocketOptionCandle,
    ) -> Candle:
        """Преобразовать свечу Pocket Option в Candle."""
        return Candle(
            timestamp=int(candle.timestamp),
            open=float(candle.open),
            high=float(candle.high),
            low=float(candle.low),
            close=float(candle.close),
            volume=float(candle.volume),
        )
    @staticmethod
    def _validate_market_candles(
        candles: list[Candle],
    ) -> None:
        """Проверить корректность последовательности свечей."""
        if not candles:
            raise MarketDataError(
                "Получен пустой список свечей."
            )
        previous_timestamp: int | None = None
        for index, candle in enumerate(candles):
            if candle.open <= 0:
                raise MarketDataError(
                    f"Некорректный open у свечи #{index}: "
                    f"{candle.open}"
                )
            if candle.high <= 0:
                raise MarketDataError(
                    f"Некорректный high у свечи #{index}: "
                    f"{candle.high}"
                )
            if candle.low <= 0:
                raise MarketDataError(
                    f"Некорректный low у свечи #{index}: "
                    f"{candle.low}"
                )
            if candle.close <= 0:
                raise MarketDataError(
                    f"Некорректный close у свечи #{index}: "
                    f"{candle.close}"
                )
            if candle.high < candle.low:
                raise MarketDataError(
                    f"high < low у свечи #{index}: "
                    f"{candle.high} < {candle.low}"
                )
            if candle.volume < 0:
                raise MarketDataError(
                    f"Некорректный volume у свечи #{index}: "
                    f"{candle.volume}"
                )
            if previous_timestamp is not None:
                if candle.timestamp <= previous_timestamp:
                    raise MarketDataError(
                        "Временные метки свечей должны "
                        "строго возрастать: "
                        f"{previous_timestamp} -> "
                        f"{candle.timestamp}"
                    )
            previous_timestamp = candle.timestamp
    async def main(self) -> None:
        """Простой self-check сервиса."""
        await self.start()
        try:
            market = await self.get_market(
                symbol=self.DEFAULT_SYMBOL,
                timeframe=self.DEFAULT_TIMEFRAME,
                limit=100,
            )
            print(
                f"Получено свечей: {len(market.candles)}"
            )
        finally:
            await self.stop()
if __name__ == "__main__":
    asyncio.run(ForexService().main())
