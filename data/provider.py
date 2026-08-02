from abc import ABC, abstractmethod
from typing import List

from data.models import Candle, MarketData


class MarketDataError(Exception):
    pass


class MarketDataProvider(ABC):

    @abstractmethod
    def get_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 200,
    ) -> MarketData:
        raise NotImplementedError


def validate_candles(
    candles: List[Candle],
    minimum_required: int = 50,
) -> None:
    if len(candles) < minimum_required:
        raise MarketDataError(
            f"Insufficient candle data: "
            f"{len(candles)} < {minimum_required}"
        )

    previous_timestamp = 0

    for candle in candles:
        if candle.timestamp <= 0:
            raise MarketDataError(
                "Invalid candle timestamp"
            )

        if candle.open <= 0:
            raise MarketDataError(
                "Invalid candle open price"
            )

        if candle.high <= 0:
            raise MarketDataError(
                "Invalid candle high price"
            )

        if candle.low <= 0:
            raise MarketDataError(
                "Invalid candle low price"
            )

        if candle.close <= 0:
            raise MarketDataError(
                "Invalid candle close price"
            )

        if candle.high < candle.low:
            raise MarketDataError(
                "Candle high is lower than low"
            )

        if candle.volume < 0:
            raise MarketDataError(
                "Invalid candle volume"
            )

        if (
            previous_timestamp > 0
            and candle.timestamp <= previous_timestamp
        ):
            raise MarketDataError(
                "Candle timestamps are not strictly increasing"
            )

        previous_timestamp = candle.timestamp
