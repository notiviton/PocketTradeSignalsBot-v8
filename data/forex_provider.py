import os
from typing import Dict

import requests

from data.models import Candle, MarketData
from data.provider import MarketDataError, MarketDataProvider, validate_candles


class ForexProvider(MarketDataProvider):
    """
    Провайдер реальных Forex-котировок через Twelve Data.

    API-ключ хранится только в переменной окружения:
    TWELVE_DATA_API_KEY
    """

    BASE_URL = "https://api.twelvedata.com/time_series"

    TIMEFRAME_MAP: Dict[str, str] = {
        "1m": "1min",
        "2m": "2min",
        "3m": "3min",
        "5m": "5min",
        "10m": "10min",
        "15m": "15min",
        "30m": "30min",
        "1h": "1h",
        "2h": "2h",
        "4h": "4h",
        "6h": "6h",
        "12h": "12h",
        "1D": "1day",
        "1W": "1week",
        "1M": "1month",
    }

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("TWELVE_DATA_API_KEY")

        if not self.api_key:
            raise MarketDataError(
                "TWELVE_DATA_API_KEY is not configured"
            )

    def get_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 200,
    ) -> MarketData:
        if not symbol:
            raise MarketDataError(
                "Forex symbol is required"
            )

        if timeframe not in self.TIMEFRAME_MAP:
            raise MarketDataError(
                f"Unsupported timeframe: {timeframe}"
            )

        if limit < 50:
            raise MarketDataError(
                "At least 50 candles are required"
            )

        interval = self.TIMEFRAME_MAP[timeframe]

        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "outputsize": min(limit, 5000),
            "apikey": self.api_key,
            "format": "JSON",
        }

        try:
            response = requests.get(
                self.BASE_URL,
                params=params,
                timeout=15,
            )
        except requests.RequestException as exc:
            raise MarketDataError(
                f"Forex data request failed: {exc}"
            ) from exc

        if response.status_code != 200:
            raise MarketDataError(
                f"Forex provider HTTP error: "
                f"{response.status_code}"
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise MarketDataError(
                "Forex provider returned invalid JSON"
            ) from exc

        if payload.get("status") == "error":
            message = payload.get(
                "message",
                "Unknown provider error",
            )
            raise MarketDataError(
                f"Forex provider error: {message}"
            )

        values = payload.get("values")

        if not isinstance(values, list):
            raise MarketDataError(
                "Forex provider returned no candle data"
            )

        candles = []

        for item in reversed(values):
            try:
                timestamp = self._parse_timestamp(
                    item["datetime"]
                )

                candle = Candle(
                    timestamp=timestamp,
                    open=float(item["open"]),
                    high=float(item["high"]),
                    low=float(item["low"]),
                    close=float(item["close"]),
                    volume=float(
                        item.get("volume", 0) or 0
                    ),
                )

            except (KeyError, TypeError, ValueError) as exc:
                raise MarketDataError(
                    "Invalid candle received from "
                    "Forex provider"
                ) from exc

            candles.append(candle)

        validate_candles(
            candles,
            minimum_required=50,
        )

        return MarketData(
            source="twelvedata",
            symbol=symbol.upper(),
            timeframe=timeframe,
            candles=candles,
        )

    @staticmethod
    def _parse_timestamp(value: str) -> int:
        from datetime import datetime, timezone

        if not value:
            raise ValueError(
                "Empty candle timestamp"
            )

        dt = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        if dt.tzinfo is None:
            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return int(
            dt.timestamp()
        )
