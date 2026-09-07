from data.pocket_otc_provider import PocketOptionOTCProvider
from data.provider import MarketDataError

class ForexService:
“””
Единый сервис получения рыночных данных.

Единственный источник данных проекта:
Pocket Option OTC
Twelve Data полностью удалён из цепочки.
"""
DEFAULT_SOURCE = "pocket_otc"
def __init__(self):
    self.provider = PocketOptionOTCProvider()
def get_market(
    self,
    source: str = DEFAULT_SOURCE,
    symbol: str = "EUR/USD",
    timeframe: str = "1m",
    limit: int = 500,
):
    """
    Получить рыночные данные из Pocket Option.
    Параметр source временно сохраняется для совместимости
    с текущими вызывающими участками проекта, но фактически
    используется только Pocket Option OTC.
    """
    normalized_source = (source or self.DEFAULT_SOURCE).lower()
    if normalized_source != self.DEFAULT_SOURCE:
        raise MarketDataError(
            f"Unsupported market data source: {normalized_source}. "
            f"Only '{self.DEFAULT_SOURCE}' is supported."
        )
    if not symbol:
        raise MarketDataError("Market symbol is required.")
    if not timeframe:
        raise MarketDataError("Market timeframe is required.")
    if limit <= 0:
        raise MarketDataError(
            "Market data limit must be greater than zero."
        )
    try:
        return self.provider.get_candles(
            symbol=symbol,
            timeframe=timeframe,
            limit=limit,
        )
    except MarketDataError:
        raise
    except Exception as exc:
        raise MarketDataError(
            f"Pocket Option market data error: {exc}"
        ) from exc
def get_last_prices(
    self,
    symbol: str = "EUR/USD",
    timeframe: str = "1m",
    source: str = DEFAULT_SOURCE,
) -> str:
    """
    Получить последние доступные свечи и сформировать
    диагностическое сообщение для Telegram.
    """
    try:
        market = self.get_market(
            source=source,
            symbol=symbol,
            timeframe=timeframe,
            limit=500,
        )
        if market is None:
            return "❌ Pocket Option не вернул рыночные данные."
        candles = getattr(market, "candles", None)
        if not candles:
            return "❌ Pocket Option не вернул свечи."
        last = candles[-1]
        return (
            "✅ Рыночные данные получены\n\n"
            f"📡 Источник: {market.source}\n"
            f"💱 Инструмент: {market.symbol}\n"
            f"⏱ Таймфрейм: {market.timeframe}\n"
            f"🕯 Свечей: {len(candles)}\n\n"
            f"Open: {last.open:.5f}\n"
            f"High: {last.high:.5f}\n"
            f"Low: {last.low:.5f}\n"
            f"Close: {last.close:.5f}"
        )
    except MarketDataError as exc:
        return (
            "❌ Ошибка получения данных Pocket Option.\n\n"
            f"Причина: {exc}"
        )
    except Exception as exc:
        return (
            "❌ Неожиданная ошибка.\n\n"
            f"Причина: {exc}"
        )
