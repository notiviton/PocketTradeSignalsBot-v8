from abc import ABC, abstractmethod
from typing import List

from data.models import Candle, MarketData


class MarketDataError(Exception):
    """Ошибка получения или проверки рыночных данных."""


class MarketDataProvider(ABC):
    """
    Базовый асинхронный интерфейс источника рыночных данных.

    Основной источник проекта:
        Pocket Option WebSocket

    Провайдер только получает рыночные данные.
    Торговых операций здесь нет и быть не должно.
    """

    @abstractmethod
    async def get_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 200,
    ) -> MarketData:
        """
        Получить последние свечи.

        Args:
            symbol: идентификатор инструмента Pocket Option.
            timeframe: таймфрейм, например "1m", "5m", "15m".
            limit: максимальное количество свечей.

        Returns:
            MarketData с отсортированными по времени свечами.

        Raises:
            MarketDataError: если данные недоступны или некорректны.
        """
        raise NotImplementedError


def validate_candles(
    candles: List[Candle],
    minimum_required: int = 50,
) -> None:
    """
    Проверяет целостность и корректность набора свечей.

    Свечи должны идти строго по возрастанию timestamp.
    """

    if not isinstance(candles, list):
        raise MarketDataError(
            "Candles must be provided as a list"
        )

    if len(candles) < minimum_required:
        raise MarketDataError(
            f"Insufficient candle data: "
            f"{len(candles)} < {minimum_required}"
        )

    previous_timestamp = 0

    for index, candle in enumerate(candles):
        if not isinstance(candle, Candle):
            raise MarketDataError(
                f"Invalid candle object at index {index}"
            )

        if candle.timestamp <= 0:
            raise MarketDataError(
                f"Invalid candle timestamp at index {index}"
            )

        if candle.open <= 0:
            raise MarketDataError(
                f"Invalid candle open price at index {index}"
            )

        if candle.high <= 0:
            raise MarketDataError(
                f"Invalid candle high price at index {index}"
            )

        if candle.low <= 0:
            raise MarketDataError(
                f"Invalid candle low price at index {index}"
            )

        if candle.close <= 0:
            raise MarketDataError(
                f"Invalid candle close price at index {index}"
            )

        if candle.volume < 0:
            raise MarketDataError(
                f"Invalid candle volume at index {index}"
            )

        # OHLC consistency.
        if candle.high < candle.low:
            raise MarketDataError(
                f"Candle high is lower than low "
                f"at index {index}"
            )

        if candle.high < candle.open:
            raise MarketDataError(
                f"Candle high is lower than open "
                f"at index {index}"
            )

        if candle.high < candle.close:
            raise MarketDataError(
                f"Candle high is lower than close "
                f"at index {index}"
            )

        if candle.low > candle.open:
            raise MarketDataError(
                f"Candle low is higher than open "
                f"at index {index}"
            )

        if candle.low > candle.close:
            raise MarketDataError(
                f"Candle low is higher than close "
                f"at index {index}"
            )

        if (
            previous_timestamp > 0
            and candle.timestamp <= previous_timestamp
        ):
            raise MarketDataError(
                "Candle timestamps are not strictly increasing"
            )

        previous_timestamp = candle.timestamp
