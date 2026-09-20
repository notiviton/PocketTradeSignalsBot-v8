from dataclasses import dataclass


@dataclass(slots=True)
class PocketOptionCandle:
    """
    Нормализованная свеча Pocket Option.
    """

    timestamp: float
    open: float
    high: float
    low: float
    close: float

    @property
    def time(self) -> int:
        """Время открытия свечи в Unix timestamp."""
        return int(self.timestamp)

    def as_dict(self) -> dict[str, float | int]:
        """Представление свечи в виде словаря."""
        return {
            "timestamp": self.timestamp,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
        }
