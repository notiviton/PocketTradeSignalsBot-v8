from typing import Optional

from data.models import Candle, MarketData
from data.provider import MarketDataError, validate_candles
from pocket_option.pocket_option.websocket_client import (
    PocketOptionWebSocketClient,
)


class ForexService:
    """
    Единый сервис получения рыночных данных.

    Единственный источник данных проекта:
        Pocket Option WebSocket.

    Сервис отвечает только за получение и подготовку
    рыночных данных.

    Торговых операций здесь нет.
    """

    DEFAULT_SYMBOL = "EURUSD_otc"
    DEFAULT_TIMEFRAME = "1m"

    def __init__(
        self,
        websocket_client: Optional[
            PocketOptionWebSocketClient
        ] = None,
    ) -> None:
        self.client = (
            websocket_client
            if websocket_client is not None
            else PocketOptionWebSocketClient()
        )

    async def start(self) -> None:
        """
        Запустить WebSocket-клиент.

        Подключение выполняется отдельно от получения данных,
        чтобы жизненный цикл WebSocket контролировался сервисом.
        """
        await self.client.start()

    async def stop(self) -> None:
        """
        Корректно остановить WebSocket-клиент.
        """
        await self.client.stop()

    async def get_market(
        self,
        symbol: str = DEFAULT_SYMBOL,
        timeframe: str = DEFAULT_TIMEFRAME,
        limit: int = 500,
    ) -> MarketData:
        """
        Получить рыночные данные Pocket Option.

        Args:
            symbol:
                Идентификатор инструмента Pocket Option.
                Например: EURUSD_otc.

            timeframe:
                Таймфрейм свечей.
                Например: 1m, 5m, 15m.

            limit:
                Максимальное количество свечей.

        Returns:
            MarketData.

        Raises:
            MarketDataError:
                Если параметры или рыночные данные некорректны.
        """

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
            candles = await self.client.get_candles(
                symbol=symbol,
                timeframe=timeframe,
                limit=limit,
            )

            if not candles:
                raise MarketDataError(
                    "Pocket Option returned no candle data."
                )

            validate_candles(
                candles,
                minimum_required=min(50, limit),
            )

            return MarketData(
                source="pocket_option_websocket",
                symbol=symbol,
                timeframe=timeframe,
                candles=candles,
            )

        except MarketDataError:
            raise

        except Exception as exc:
            raise MarketDataError(
                f"Pocket Option WebSocket market data error: {exc}"
            ) from exc

    async def get_last_prices(
        self,
        symbol: str = DEFAULT_SYMBOL,
        timeframe: str = DEFAULT_TIMEFRAME,
    ) -> str:
        """
        Получить последние доступные свечи и сформировать
        диагностическое сообщение для Telegram.
        """

        try:
            market = await self.get_market(
                symbol=symbol,
                timeframe=timeframe,
                limit=500,
            )

            candles = market.candles

            if not candles:
                return (
                    "❌ Pocket Option не вернул свечи."
                )

            last = candles[-1]

            return (
                "✅ Рыночные данные получены\n\n"
                "📡 Источник: Pocket Option WebSocket\n"
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

    def status(self) -> dict:
        """
        Текущий статус рыночного сервиса.

        Только диагностика.
        Торговых функций нет.
        """
        client_status = self.client.status()

        return {
            "service": "ForexService",
            "market_data_source": "Pocket Option WebSocket",
            "analytics_only": True,
            "trade_execution": False,
            "websocket": client_status,
        }
