from __future__ import annotations

from typing import Any, Awaitable, Callable

from .models import PocketOptionCandle
from .parser import PocketOptionHistoryParser


class PocketOptionHistoryService:
    """
    Отдельный сервис работы с историческими свечами.

    WebSocket-клиент передаётся снаружи.
    Сам сервис не занимается:
    - Telegram;
    - авторизацией;
    - созданием WebSocket-соединения.
    """

    def __init__(
        self,
        websocket_client: Any,
    ) -> None:
        self.websocket_client = websocket_client
        self.parser = PocketOptionHistoryParser()

    async def get_history(
        self,
        symbol: str,
        period: int,
        count: int = 500,
        offset: int = 9000,
        timeout: float = 20.0,
    ) -> list[PocketOptionCandle]:
        """
        Получить исторические свечи через существующий
        Pocket Option WebSocket-клиент.

        На этом этапе метод использует существующий
        request_history() клиента.
        """

        raw_history = await self.websocket_client.request_history(
            symbol=symbol,
            period=period,
            count=count,
            offset=offset,
            timeout=timeout,
        )

        candles = self.parser.parse(raw_history)

        if not candles:
            raise RuntimeError(
                f"Pocket Option history is empty: "
                f"{symbol} period={period}"
            )

        if count > 0 and len(candles) > count:
            candles = candles[-count:]

        return candles

    async def get_latest(
        self,
        symbol: str,
        period: int,
        count: int = 500,
        offset: int = 9000,
        timeout: float = 20.0,
    ) -> list[PocketOptionCandle]:
        """
        Алиас для получения последних исторических свечей.
        """

        return await self.get_history(
            symbol=symbol,
            period=period,
            count=count,
            offset=offset,
            timeout=timeout,
        )

    def candles_to_dicts(
        self,
        candles: list[PocketOptionCandle],
    ) -> list[dict[str, float | int]]:
        """
        Преобразовать нормализованные свечи в словари.
        """

        return [
            candle.as_dict()
            for candle in candles
        ]
