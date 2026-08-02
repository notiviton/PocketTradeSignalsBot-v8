from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class Candle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class MarketData:
    source: str
    symbol: str
    timeframe: str
    candles: List[Candle]
