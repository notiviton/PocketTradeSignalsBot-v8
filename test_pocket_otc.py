import asyncio
import os

from pocketoptionapi_async import AsyncPocketOptionClient


async def main():
    ssid = os.getenv("POCKET_OPTION_SSID")

    if not ssid:
        print("❌ POCKET_OPTION_SSID не задан")
        return

    client = AsyncPocketOptionClient(
        ssid=ssid,
        is_demo=True,
        enable_logging=True,
    )

    try:
        print("🔌 Подключение к Pocket Option...")

        await client.connect()

        print("✅ Соединение установлено")

        asset = "CHF NOK_otc"

        print(f"📡 Запрашиваем данные: {asset}")

        candles = await client.get_candles(
            asset=asset,
            timeframe=60,
        )

        if not candles:
            print("❌ Pocket Option не вернул свечи")
            return

        print(f"✅ Получено свечей: {len(candles)}")

        for candle in candles[-10:]:
            print(
                f"time={candle.timestamp} "
                f"open={candle.open} "
                f"high={candle.high} "
                f"low={candle.low} "
                f"close={candle.close} "
                f"volume={candle.volume}"
            )

    except Exception as exc:
        print(f"❌ Ошибка Pocket Option: {type(exc).__name__}: {exc}")

    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
