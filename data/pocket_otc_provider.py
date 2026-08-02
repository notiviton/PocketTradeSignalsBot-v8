import os
from typing import Any

import requests

from data.models import Candle, MarketData
from data.provider import (
    MarketDataError,
    MarketDataProvider,
    validate_candles,
)


class PocketOptionOTCProvider(MarketDataProvider):
    """
    Pocket Option OTC adapter.

    ВАЖНО:
    Pocket Option не предоставляет стабильный официальный
    публичный REST API для получения OTC-свечей.

    Поэтому провайдер работает только через заранее
    настроенный проверенный bridge.

    Переменные окружения:

    POCKET_OPTION_OTC_API_URL
    POCKET_OPTION_OTC_API_KEY

    Ожидаемый JSON:

    {
        "values": [
            {
                "timestamp": 1234567890,
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.05,
                "volume": 0
            }
        ]
    }
    """

    def __init__(self, api_url: str | None = None):
        self.api_url = (
            api_url
            or os.getenv("POCKET_OPTION_OTC_API_URL")
        )

        self.api_key = os.getenv(
            "POCKET_OPTION_OTC_API_KEY"
        )

    def get_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 200,
    ) -> MarketData:

        if not self.api_url:
            raise MarketDataError(
                "Pocket Option OTC источник не настроен. "
                "Укажите POCKET_OPTION_OTC_API_URL "
                "для проверенного bridge/adapter."
            )

        if not symbol:
            raise MarketDataError(
                "Pocket Option OTC symbol is required"
            )

        if not timeframe:
            raise MarketDataError(
                "Pocket Option OTC timeframe is required"
            )

        if limit < 50:
            raise MarketDataError(
                "At least 50 candles are required"
            )

        headers = {}

        if self.api_key:
            headers["Authorization"] = (
                f"Bearer {self.api_key}"
            )

        params = {
            "symbol": symbol,
            "timeframe": timeframe,
            "limit": min(limit, 5000),
        }

        try:
            response = requests.get(
                self.api_url,
                params=params,
                headers=headers,
                timeout=15,
            )

            response.raise_for_status()

        except requests.RequestException as exc:
            raise MarketDataError(
                f"Pocket OTC bridge request failed: {exc}"
            ) from exc

        try:
            payload: Any = response.json()

        except ValueError as exc:
            raise MarketDataError(
                "Pocket OTC bridge returned invalid JSON"
            ) from exc

        values = (
            payload.get("values")
            if isinstance(payload, dict)
            else None
        )

        if not isinstance(values, list):
            raise MarketDataError(
                "Pocket OTC bridge returned no "
                "normalized candle list"
            )

        candles = []

        for item in values:
            try:
                timestamp = int(
                    item["timestamp"]
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

            except (
                KeyError,
                TypeError,
                ValueError,
            ) as exc:
                raise MarketDataError(
                    "Invalid candle in "
                    "Pocket OTC bridge response"
                ) from exc

            candles.append(candle)

        candles.sort(
            key=lambda candle: candle.timestamp
        )

        validate_candles(
            candles,
            minimum_required=50,
        )

        return MarketData(
            source="pocket_otc",
            symbol=symbol.upper(),
            timeframe=timeframe,
            candles=candles,
        )
