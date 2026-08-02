from data.forex_provider import ForexProvider
from data.pocket_otc_provider import PocketOptionOTCProvider
from data.provider import MarketDataError


class ForexService:
    def __init__(self):
        self.providers = {
            "twelvedata": ForexProvider(),
            "pocket_otc": PocketOptionOTCProvider(),
        }

    def get_market(
        self,
        source: str,
        symbol: str,
        timeframe: str,
        limit: int = 500,
    ):
        source = (source or "twelvedata").lower()

        provider = self.providers.get(source)

        if provider is None:
            raise MarketDataError(
                f"Unsupported market data source: {source}"
            )

        return provider.get_candles(
            symbol=symbol,
            timeframe=timeframe,
            limit=limit,
        )

    def get_last_prices(
        self,
        symbol="EUR/USD",
        timeframe="1m",
        source="twelvedata",
    ) -> str:
        try:
            market = self.get_market(
                source=source,
                symbol=symbol,
                timeframe=timeframe,
                limit=500,
            )

            if not market.candles:
                return "❌ Источник не вернул свечи."

            last = market.candles[-1]

            return (
                "✅ Рыночные данные получены\n\n"
                f"📡 Источник: {market.source}\n"
                f"💱 Инструмент: {market.symbol}\n"
                f"⏱ Таймфрейм: {market.timeframe}\n"
                f"🕯 Свечей: {len(market.candles)}\n\n"
                f"Open: {last.open:.5f}\n"
                f"High: {last.high:.5f}\n"
                f"Low: {last.low:.5f}\n"
                f"Close: {last.close:.5f}"
            )

        except MarketDataError as exc:
            return (
                "❌ Ошибка получения данных.\n\n"
                f"Причина: {exc}"
            )

        except Exception as exc:
            return (
                "❌ Неожиданная ошибка.\n\n"
                f"Причина: {exc}"
            )
