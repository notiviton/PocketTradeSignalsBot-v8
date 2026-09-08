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
    Единый сервис рыночных данных проекта.

    Источник данных:
        Pocket Option WebSocket

    Цепочка:
        Pocket Option WebSocket
            ↓
        History / realtime ticks
            ↓
        Candle aggregation
            ↓
        MarketData
            ↓
        IndicatorEngine

    ВАЖНО:
    - Twelve Data не используется;
    - старый REST Pocket OTC provider не используется;
    - торговых операций нет;
    - сервис предназначен только для аналитики.
    """

    DEFAULT_SOURCE = "pocket_otc"
    DEFAULT_SYMBOL = "EUR/USD OTC"
    DEFAULT_TIMEFRAME = "1m"
    DEFAULT_LIMIT = 500

    def __init__(
        self,
        websocket_client: PocketOptionWebSocketClient | None = None,
    ) -> None:
        self.client = (
            websocket_client
            or PocketOptionWebSocketClient()
        )

        self._started = False
        self._start_lock = asyncio.Lock()

    # ==========================================================================
    # LIFECYCLE
    # ==========================================================================

    async def start(self) -> None:
        """
        Запустить WebSocket background lifecycle.
        """
        async with self._start_lock:
            if self._started:
                return

            await self.client.start()

            self._started = True

    async def stop(self) -> None:
        """
        Корректно остановить WebSocket.
        """
        async with self._start_lock:
            if not self._started:
                return

            await self.client.stop()

            self._started = False

    async def _ensure_started(self) -> None:
        if not self._started:
            await self.start()

    # ==========================================================================
    # MARKET DATA
    # ==========================================================================

    async def get_market(
        self,
        source: str = DEFAULT_SOURCE,
        symbol: str = DEFAULT_SYMBOL,
        timeframe: str = DEFAULT_TIMEFRAME,
        limit: int = DEFAULT_LIMIT,
    ) -> MarketData:
        """
        Получить MarketData для IndicatorEngine.

        `source` оставлен только для совместимости со старым
        интерфейсом. Фактически разрешён только Pocket Option.
        """
        normalized_source = (
            source or self.DEFAULT_SOURCE
        ).strip().lower()

        if normalized_source not in {
            self.DEFAULT_SOURCE,
            "pocketoption",
            "pocket_option",
            "pocket",
        }:
            raise MarketDataError(
                "Unsupported market data source: "
                f"{normalized_source}. "
                "Only Pocket Option is supported."
            )

        if not symbol:
            raise MarketDataError(
                "Market symbol is required."
            )

        if not timeframe:
            raise MarketDataError(
                "Market timeframe is required."
            )

        if limit <= 0:
            raise MarketDataError(
                "Market data limit must be greater than zero."
            )

        try:
            await self._ensure_started()

            period = (
                self.client.timeframe_to_seconds(
                    timeframe
                )
            )

            provider_symbol = (
                self.client.normalize_symbol(
                    symbol
                )
            )

            candles = (
                await self.client.request_history(
                    provider_symbol,
                    period,
                    offset=max(
                        int(limit),
                        100,
                    ),
                    timeout=20.0,
                )
            )

            if len(candles) < limit:
                cached = (
                    self.client.get_cached_candles(
                        provider_symbol,
                        period,
                        limit=limit,
                    )
                )

                if len(cached) > len(candles):
                    candles = cached

            candles = candles[-limit:]

            if not candles:
                raise MarketDataError(
                    "Pocket Option returned no candle data "
                    f"for {symbol} {timeframe}."
                )

            converted = [
                self._convert_candle(
                    candle
                )
                for candle in candles
            ]

            self._validate_market_candles(
                converted
            )

            return MarketData(
                source=self.DEFAULT_SOURCE,
                symbol=symbol,
                timeframe=timeframe,
                candles=converted,
            )

        except MarketDataError:
            raise

        except PocketOptionWebSocketError as exc:
            raise MarketDataError(
                f"Pocket Option WebSocket error: {exc}"
            ) from exc

        except ValueError as exc:
            raise MarketDataError(
                f"Invalid market parameters: {exc}"
            ) from exc

        except Exception as exc:
            raise MarketDataError(
                f"Pocket Option market data error: {exc}"
            ) from exc

    # ==========================================================================
    # LAST PRICE
    # ==========================================================================

    async def get_last_prices(
        self,
        symbol: str = DEFAULT_SYMBOL,
        timeframe: str = DEFAULT_TIMEFRAME,
        source: str = DEFAULT_SOURCE,
    ) -> str:
        """
        Диагностический метод для /test_forex.

        Возвращает понятный текст вместо старого Twelve Data response.
        """
        normalized_source = (
            source or self.DEFAULT_SOURCE
        ).strip().lower()

        if normalized_source not in {
            self.DEFAULT_SOURCE,
            "pocketoption",
            "pocket_option",
            "pocket",
        }:
            raise MarketDataError(
                "Only Pocket Option is supported."
            )

        try:
            await self._ensure_started()

            period = (
                self.client.timeframe_to_seconds(
                    timeframe
                )
            )

            provider_symbol = (
                self.client.normalize_symbol(
                    symbol
                )
            )

            await self.client.subscribe(
                provider_symbol,
                period,
            )

            # Даем realtime потоку короткое время
            # получить актуальный tick.
            last_tick = (
                self.client.get_last_tick(
                    provider_symbol
                )
            )

            if last_tick is None:
                try:
                    await asyncio.wait_for(
                        self._wait_for_tick(
                            provider_symbol
                        ),
                        timeout=10.0,
                    )
                except asyncio.TimeoutError:
                    pass

                last_tick = (
                    self.client.get_last_tick(
                        provider_symbol
                    )
                )

            if last_tick is None:
                raise MarketDataError(
                    "No realtime price received from "
                    f"Pocket Option for {symbol}."
                )

            return (
                "Pocket Option WebSocket: ONLINE\n"
                f"Instrument: {symbol}\n"
                f"Provider symbol: {provider_symbol}\n"
                f"Timeframe: {timeframe}\n"
                f"Last price: {last_tick.price:.6f}\n"
                f"Tick timestamp: {last_tick.timestamp:.3f}\n"
                "Analytics only: YES\n"
                "Trade execution: DISABLED"
            )

        except MarketDataError:
            raise

        except PocketOptionWebSocketError as exc:
            raise MarketDataError(
                f"Pocket Option WebSocket error: {exc}"
            ) from exc

        except Exception as exc:
            raise MarketDataError(
                f"Pocket Option price error: {exc}"
            ) from exc

    async def _wait_for_tick(
        self,
        symbol: str,
    ) -> None:
        """
        Небольшой polling без создания второго WebSocket reader.
        """
        provider_symbol = (
            self.client.normalize_symbol(
                symbol
            )
        )

        while True:
            if (
                self.client.get_last_tick(
                    provider_symbol
                )
                is not None
            ):
                return

            await asyncio.sleep(0.25)

    # ==========================================================================
    # STATUS
    # ==========================================================================

    def status(self) -> dict[str, Any]:
        """
        Состояние источника рыночных данных.
        """
        client_status = self.client.status()

        return {
            "source": self.DEFAULT_SOURCE,
            "started": self._started,
            "connected": client_status.get(
                "connected",
                False,
            ),
            "authenticated": client_status.get(
                "authenticated",
                False,
            ),
            "analytics_only": True,
            "trade_execution": False,
            "last_error": client_status.get(
                "last_error"
            ),
            "known_symbols": client_status.get(
                "known_symbols",
                [],
            ),
            "subscriptions": client_status.get(
                "subscriptions",
                {},
            ),
        }

    # ==========================================================================
    # CONVERSION
    # ==========================================================================

    @staticmethod
    def _convert_candle(
        candle: PocketOptionCandle,
    ) -> Candle:
        return Candle(
            timestamp=int(
                candle.timestamp
            ),
            open=float(
                candle.open
            ),
            high=float(
                candle.high
            ),
            low=float(
                candle.low
            ),
            close=float(
                candle.close
            ),
            volume=float(
                candle.volume
            ),
        )

    # ==========================================================================
    # VALIDATION
    # ==========================================================================

    @staticmethod
    def _validate_market_candles(
        candles: list[Candle],
    ) -> None:
        if not candles:
            raise MarketDataError(
                "No candles available."
            )

        previous_timestamp = 0

        for candle in candles:
            if candle.timestamp <= 0:
                raise MarketDataError(
                    "Invalid candle timestamp."
                )

            if candle.open <= 0:
                raise MarketDataError(
                    "Invalid candle open price."
                )

            if candle.high <= 0:
                raise MarketDataError(
                    "Invalid candle high price."
                )

            if candle.low <= 0:
                raise MarketDataError(
                    "Invalid candle low price."
                )

            if candle.close <= 0:
                raise MarketDataError(
                    "Invalid candle close price."
                )

            if candle.high < candle.low:
                raise MarketDataError(
                    "Candle high is lower than low."
                )

            if candle.volume < 0:
                raise MarketDataError(
                    "Invalid candle volume."
                )

            if (
                previous_timestamp > 0
                and candle.timestamp
                <= previous_timestamp
            ):
                raise MarketDataError(
                    "Candle timestamps are not "
                    "strictly increasing."
                )

            previous_timestamp = (
                candle.timestamp
            )


# ==============================================================================
# MODULE SELF-CHECK
# ==============================================================================

async def main() -> None:
    """
    Локальная проверка сервиса.

    Торговых операций здесь нет.
    """
    service = ForexService()

    try:
        await service.start()

        print(
            "SERVICE STATUS:"
        )
        print(
            service.status()
        )

        market = await service.get_market(
            symbol="EUR/USD OTC",
            timeframe="1m",
            limit=100,
        )

        print(
            f"Market source: {market.source}"
        )

        print(
            f"Symbol: {market.symbol}"
        )

        print(
            f"Timeframe: {market.timeframe}"
        )

        print(
            f"Candles: {len(market.candles)}"
        )

        if market.candles:
            candle = market.candles[-1]

            print(
                "Last candle:",
                {
                    "timestamp": candle.timestamp,
                    "open": candle.open,
                    "high": candle.high,
                    "low": candle.low,
                    "close": candle.close,
                    "volume": candle.volume,
                },
            )

    finally:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())
