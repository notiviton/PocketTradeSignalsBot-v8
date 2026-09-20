from __future__ import annotations

from typing import Any

from .models import PocketOptionCandle


class PocketOptionHistoryParser:
    """
    Разбирает исторические данные Pocket Option
    и преобразует их в PocketOptionCandle.
    """

    def parse(self, payload: Any) -> list[PocketOptionCandle]:
        """
        Извлечь свечи из ответа Pocket Option.

        Поддерживаются:
        - список свечей;
        - словари с ключами candles/history/data/result/items/values;
        - одна свеча в виде словаря;
        - массивы Pocket Option:
          [timestamp, open, close, high, low]
        - стандартный формат:
          [timestamp, open, high, low, close]
        """
        candles = self._extract_candles(payload)

        result: list[PocketOptionCandle] = []

        for item in candles:
            candle = self._parse_candle(item)

            if candle is not None:
                result.append(candle)

        result.sort(key=lambda candle: candle.timestamp)

        return result

    def _extract_candles(self, payload: Any) -> list[Any]:
        if payload is None:
            return []

        if isinstance(payload, dict):
            for key in (
                "candles",
                "history",
                "data",
                "result",
                "items",
                "values",
            ):
                if key in payload:
                    value = payload[key]

                    if isinstance(value, list):
                        return value

                    if isinstance(value, dict):
                        nested = self._extract_candles(value)

                        if nested:
                            return nested

            if self._looks_like_candle_dict(payload):
                return [payload]

            return []

        if isinstance(payload, list):
            return payload

        return []

    def _parse_candle(self, item: Any) -> PocketOptionCandle | None:
        if isinstance(item, dict):
            return self._parse_dict_candle(item)

        if isinstance(item, (list, tuple)):
            return self._parse_array_candle(item)

        return None

    def _parse_dict_candle(
        self,
        item: dict[str, Any],
    ) -> PocketOptionCandle | None:
        timestamp = self._first_number(
            item,
            "timestamp",
            "time",
            "from",
            "at",
            "t",
        )

        open_price = self._first_number(
            item,
            "open",
            "o",
        )

        high_price = self._first_number(
            item,
            "high",
            "h",
        )

        low_price = self._first_number(
            item,
            "low",
            "l",
        )

        close_price = self._first_number(
            item,
            "close",
            "c",
        )

        if None in (
            timestamp,
            open_price,
            high_price,
            low_price,
            close_price,
        ):
            return None

        return self._build_candle(
            timestamp,
            open_price,
            high_price,
            low_price,
            close_price,
        )

    def _parse_array_candle(
        self,
        item: list[Any] | tuple[Any, ...],
    ) -> PocketOptionCandle | None:
        if len(item) < 5:
            return None

        values: list[float] = []

        try:
            for value in item[:5]:
                values.append(float(value))
        except (TypeError, ValueError):
            return None

        timestamp = values[0]
        open_price = values[1]

        # Pocket Option commonly sends:
        # [timestamp, open, close, high, low]
        close_price = values[2]
        high_price = values[3]
        low_price = values[4]

        pocket_candle = self._build_candle(
            timestamp,
            open_price,
            high_price,
            low_price,
            close_price,
        )

        if pocket_candle is not None:
            return pocket_candle

        # Fallback for standard OHLC ordering:
        # [timestamp, open, high, low, close]
        high_price = values[2]
        low_price = values[3]
        close_price = values[4]

        return self._build_candle(
            timestamp,
            open_price,
            high_price,
            low_price,
            close_price,
        )

    def _build_candle(
        self,
        timestamp: float,
        open_price: float,
        high_price: float,
        low_price: float,
        close_price: float,
    ) -> PocketOptionCandle | None:
        if timestamp <= 0:
            return None

        if min(
            open_price,
            high_price,
            low_price,
            close_price,
        ) <= 0:
            return None

        if high_price < max(open_price, close_price):
            return None

        if low_price > min(open_price, close_price):
            return None

        if low_price > high_price:
            return None

        return PocketOptionCandle(
            timestamp=timestamp,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
        )

    def _looks_like_candle_dict(self, item: dict[str, Any]) -> bool:
        has_time = any(
            key in item
            for key in ("timestamp", "time", "from", "at", "t")
        )

        has_open = any(
            key in item
            for key in ("open", "o")
        )

        has_close = any(
            key in item
            for key in ("close", "c")
        )

        return has_time and has_open and has_close

    def _first_number(
        self,
        item: dict[str, Any],
        *keys: str,
    ) -> float | None:
        for key in keys:
            if key not in item:
                continue

            try:
                return float(item[key])
            except (TypeError, ValueError):
                continue

        return None
