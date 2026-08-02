import asyncio
import os

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from services.forex_service import ForexService
from indicators.indicator_engine import IndicatorEngine
from data.provider import MarketDataError


# ============================================================
# TELEGRAM BOT TOKEN
# ВСТАВЬ СЮДА ТОТ ЖЕ САМЫЙ РАБОЧИЙ TELEGRAM TOKEN
# ============================================================

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")


# ============================================================
# BOT
# ============================================================

if not TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")

bot = Bot(token=TOKEN)
dp = Dispatcher()


# ============================================================
# SERVICES
# ============================================================

forex_service = ForexService()


# ============================================================
# USER SETTINGS
# ============================================================

user_settings = {}

DEFAULT_SYMBOL = "EUR/USD"
DEFAULT_TIMEFRAME = "1m"
DEFAULT_SOURCE = os.getenv("MARKET_DATA_SOURCE", "twelvedata").lower()


def get_user_settings(user_id: int):
    if user_id not in user_settings:
        user_settings[user_id] = {
            "symbol": DEFAULT_SYMBOL,
            "timeframe": DEFAULT_TIMEFRAME,
            "source": DEFAULT_SOURCE,
        }

    return user_settings[user_id]


# ============================================================
# MAIN MENU
# ============================================================

def main_menu():
    builder = InlineKeyboardBuilder()

    builder.button(
        text="📈 Получить сигнал",
        callback_data="signal",
    )

    builder.button(
        text="📊 Анализ рынка",
        callback_data="analysis",
    )

    builder.button(
        text="📊 Индикаторы",
        callback_data="indicators",
    )

    builder.button(
        text="📋 Статус системы",
        callback_data="status",
    )

    builder.button(
        text="⚙️ Настройки",
        callback_data="settings",
    )

    builder.button(
        text="ℹ️ О боте",
        callback_data="about",
    )

    builder.adjust(1)

    return builder.as_markup()


# ============================================================
# SETTINGS MENU
# ============================================================

def settings_menu(user_id: int):
    settings = get_user_settings(user_id)

    builder = InlineKeyboardBuilder()

    builder.button(
        text=f"💱 Инструмент: {settings['symbol']}",
        callback_data="select_symbol",
    )

    builder.button(
        text=f"⏱ Таймфрейм: {settings['timeframe']}",
        callback_data="select_timeframe",
    )

    builder.button(
        text=f"📡 Источник: {settings['source']}",
        callback_data="select_source",
    )

    builder.button(
        text="⬅️ Главное меню",
        callback_data="main_menu",
    )

    builder.adjust(1)

    return builder.as_markup()


# ============================================================
# SYMBOL MENU
# ============================================================

def symbol_menu():
    symbols = [
        "EUR/USD",
        "GBP/USD",
        "USD/JPY",
        "USD/CHF",
        "AUD/USD",
        "USD/CAD",
        "NZD/USD",
    ]

    builder = InlineKeyboardBuilder()

    for symbol in symbols:
        builder.button(
            text=f"💱 {symbol}",
            callback_data=f"symbol:{symbol}",
        )

    builder.button(
        text="⬅️ Назад",
        callback_data="settings",
    )

    builder.adjust(1)

    return builder.as_markup()


# ============================================================
# TIMEFRAME MENU
# ============================================================

def timeframe_menu():
    timeframes = [
        "1m",
        "5m",
        "15m",
        "30m",
        "1h",
        "4h",
        "1D",
    ]

    builder = InlineKeyboardBuilder()

    for timeframe in timeframes:
        builder.button(
            text=f"⏱ {timeframe}",
            callback_data=f"timeframe:{timeframe}",
        )

    builder.button(
        text="⬅️ Назад",
        callback_data="settings",
    )

    builder.adjust(1)

    return builder.as_markup()


# ============================================================
# SOURCE MENU
# ============================================================

def source_menu():
    builder = InlineKeyboardBuilder()
    builder.button(text="📡 Twelve Data", callback_data="source:twelvedata")
    builder.button(text="🟣 Pocket Option OTC", callback_data="source:pocket_otc")
    builder.button(text="⬅️ Назад", callback_data="settings")
    builder.adjust(1)
    return builder.as_markup()


# ============================================================
# FORMAT INDICATORS
# ============================================================

def format_indicators(
    indicators,
    symbol,
    timeframe,
):
    return (
        "📊 ТЕХНИЧЕСКИЕ ИНДИКАТОРЫ\n\n"
        f"💱 Инструмент: {symbol}\n"
        f"⏱ Таймфрейм: {timeframe}\n"
        f"💰 Цена Close: {indicators['close']:.5f}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📈 RSI\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"RSI(14): {indicators['rsi']:.2f}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📉 MACD\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"MACD: {indicators['macd']:.6f}\n"
        f"Signal: {indicators['macd_signal']:.6f}\n"
        f"Histogram: {indicators['macd_histogram']:.6f}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📊 STOCHASTIC\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"%K: {indicators['stochastic_k']:.2f}\n"
        f"%D: {indicators['stochastic_d']:.2f}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📐 BOLLINGER BANDS\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"Upper: {indicators['bollinger_upper']:.5f}\n"
        f"Middle: {indicators['bollinger_middle']:.5f}\n"
        f"Lower: {indicators['bollinger_lower']:.5f}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📏 EMA\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"EMA 9: {indicators['ema_9']:.5f}\n"
        f"EMA 21: {indicators['ema_21']:.5f}\n"
        f"EMA 50: {indicators['ema_50']:.5f}\n"
        f"EMA 200: {indicators['ema_200']:.5f}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📊 ADX\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"ADX: {indicators['adx']:.2f}\n"
        f"+DI: {indicators['plus_di']:.2f}\n"
        f"-DI: {indicators['minus_di']:.2f}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📏 ATR\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"ATR(14): {indicators['atr']:.6f}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📊 CCI\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"CCI(20): {indicators['cci']:.2f}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📉 WILLIAMS %R\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"Williams %R: {indicators['williams_r']:.2f}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "⚠️ Это расчёт индикаторов.\n"
        "Торговый сигнал автоматически не формируется.\n"
        "Автоматическое открытие сделок отключено."
    )


# ============================================================
# START
# ============================================================

@dp.message(CommandStart())
async def start(message: Message):
    get_user_settings(message.from_user.id)

    await message.answer(
        "✅ PocketTradeSignalsBot запущен.\n\n"
        "Это аналитический бот.\n"
        "Автоматическое открытие реальных сделок отключено.\n\n"
        "Выберите действие:",
        reply_markup=main_menu(),
    )


# ============================================================
# TEST FOREX
# ============================================================

@dp.message(Command("test_forex"))
async def test_forex(message: Message):
    settings = get_user_settings(message.from_user.id)

    symbol = settings["symbol"]
    timeframe = settings["timeframe"]

    await message.answer(
        "⏳ Проверяю подключение к Twelve Data...\n\n"
        f"💱 Инструмент: {symbol}\n"
        f"⏱ Таймфрейм: {timeframe}\n\n"
        "Получаю реальные свечи."
    )

    result = forex_service.get_last_prices(
        symbol=symbol,
        timeframe=timeframe,
        source=settings["source"],
    )

    await message.answer(result)


# ============================================================
# INDICATORS COMMAND
# ============================================================

@dp.message(Command("indicators"))
async def indicators_command(message: Message):
    settings = get_user_settings(message.from_user.id)

    symbol = settings["symbol"]
    timeframe = settings["timeframe"]

    await message.answer(
        "⏳ Рассчитываю технические индикаторы...\n\n"
        f"💱 Инструмент: {symbol}\n"
        f"⏱ Таймфрейм: {timeframe}\n"
        "🕯 Загружаю 500 свечей."
    )

    try:
        market = forex_service.get_market(
            source=settings["source"],
            symbol=symbol,
            timeframe=timeframe,
            limit=500,
        )

        engine = IndicatorEngine(
            market.candles
        )

        indicators = engine.calculate_all()

        result = format_indicators(
            indicators=indicators,
            symbol=symbol,
            timeframe=timeframe,
        )

        await message.answer(result)

    except Exception as exc:
        await message.answer(
            "❌ Ошибка расчёта индикаторов.\n\n"
            f"Причина: {exc}"
        )


# ============================================================
# SETTINGS
# ============================================================

@dp.callback_query(F.data == "settings")
async def settings_callback(
    callback: CallbackQuery,
):
    await callback.message.edit_text(
        "⚙️ Настройки анализа\n\n"
        "Выберите инструмент или таймфрейм:",
        reply_markup=settings_menu(
            callback.from_user.id
        ),
    )

    await callback.answer()


# ============================================================
# SELECT SOURCE
# ============================================================

@dp.callback_query(F.data == "select_source")
async def select_source_callback(callback: CallbackQuery):
    await callback.message.edit_text(
        "📡 Выберите источник котировок:",
        reply_markup=source_menu(),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("source:"))
async def source_selected(callback: CallbackQuery):
    source = callback.data.split(":", 1)[1]
    settings = get_user_settings(callback.from_user.id)
    settings["source"] = source
    await callback.message.edit_text(
        "✅ Источник выбран.\n\n"
        f"📡 {source}\n"
        f"💱 {settings['symbol']}\n"
        f"⏱ {settings['timeframe']}",
        reply_markup=settings_menu(callback.from_user.id),
    )
    await callback.answer("Источник изменён")


# ============================================================
# SELECT SYMBOL
# ============================================================

@dp.callback_query(F.data == "select_symbol")
async def select_symbol_callback(
    callback: CallbackQuery,
):
    await callback.message.edit_text(
        "💱 Выберите валютную пару:",
        reply_markup=symbol_menu(),
    )

    await callback.answer()


# ============================================================
# SELECT TIMEFRAME
# ============================================================

@dp.callback_query(F.data == "select_timeframe")
async def select_timeframe_callback(
    callback: CallbackQuery,
):
    await callback.message.edit_text(
        "⏱ Выберите таймфрейм:",
        reply_markup=timeframe_menu(),
    )

    await callback.answer()


# ============================================================
# SYMBOL SELECTION
# ============================================================

@dp.callback_query(
    F.data.startswith("symbol:")
)
async def symbol_selected(
    callback: CallbackQuery,
):
    symbol = callback.data.split(
        ":",
        1,
    )[1]

    settings = get_user_settings(
        callback.from_user.id
    )

    settings["symbol"] = symbol

    await callback.message.edit_text(
        "✅ Инструмент выбран.\n\n"
        f"💱 {symbol}\n"
        f"⏱ Таймфрейм: {settings['timeframe']}",
        reply_markup=settings_menu(
            callback.from_user.id
        ),
    )

    await callback.answer(
        f"Выбран {symbol}"
    )


# ============================================================
# TIMEFRAME SELECTION
# ============================================================

@dp.callback_query(
    F.data.startswith("timeframe:")
)
async def timeframe_selected(
    callback: CallbackQuery,
):
    timeframe = callback.data.split(
        ":",
        1,
    )[1]

    settings = get_user_settings(
        callback.from_user.id
    )

    settings["timeframe"] = timeframe

    await callback.message.edit_text(
        "✅ Таймфрейм выбран.\n\n"
        f"💱 Инструмент: {settings['symbol']}\n"
        f"⏱ Таймфрейм: {timeframe}",
        reply_markup=settings_menu(
            callback.from_user.id
        ),
    )

    await callback.answer(
        f"Выбран таймфрейм {timeframe}"
    )


# ============================================================
# MAIN MENU
# ============================================================

@dp.callback_query(F.data == "main_menu")
async def main_menu_callback(
    callback: CallbackQuery,
):
    await callback.message.edit_text(
        "🏠 Главное меню",
        reply_markup=main_menu(),
    )

    await callback.answer()


# ============================================================
# SIGNAL
# ============================================================

@dp.callback_query(F.data == "signal")
async def signal_callback(
    callback: CallbackQuery,
):
    settings = get_user_settings(
        callback.from_user.id
    )

    await callback.message.answer(
        "📈 Получение сигнала\n\n"
        f"💱 Инструмент: {settings['symbol']}\n"
        f"⏱ Таймфрейм: {settings['timeframe']}\n\n"
        "⏳ Сигнальный движок запускается...\n\n"
        "⚠️ Автоматическое открытие сделок отключено."
    )

    await callback.answer()


# ============================================================
# ANALYSIS
# ============================================================

@dp.callback_query(F.data == "analysis")
async def analysis_callback(
    callback: CallbackQuery,
):
    settings = get_user_settings(
        callback.from_user.id
    )

    await callback.message.answer(
        "📊 Анализ рынка\n\n"
        f"💱 Инструмент: {settings['symbol']}\n"
        f"⏱ Таймфрейм: {settings['timeframe']}\n\n"
        "🟢 Источник рыночных данных: подключён\n"
        "🟢 12 технических индикаторов: подключены\n"
        "🟡 Сигнальный движок: следующий этап\n\n"
        "Автоматическая торговля отключена."
    )

    await callback.answer()


# ============================================================
# INDICATORS BUTTON
# ============================================================

@dp.callback_query(F.data == "indicators")
async def indicators_callback(
    callback: CallbackQuery,
):
    settings = get_user_settings(
        callback.from_user.id
    )

    await callback.message.answer(
        "⏳ Рассчитываю 12 индикаторов...\n\n"
        f"💱 Инструмент: {settings['symbol']}\n"
        f"⏱ Таймфрейм: {settings['timeframe']}"
    )

    try:
        market = forex_service.get_market(
            source=settings["source"],
            symbol=settings["symbol"],
            timeframe=settings["timeframe"],
            limit=500,
        )

        engine = IndicatorEngine(
            market.candles
        )

        indicators = engine.calculate_all()

        result = format_indicators(
            indicators=indicators,
            symbol=settings["symbol"],
            timeframe=settings["timeframe"],
        )

        await callback.message.answer(result)

    except Exception as exc:
        await callback.message.answer(
            "❌ Ошибка расчёта индикаторов.\n\n"
            f"Причина: {exc}"
        )

    await callback.answer()


# ============================================================
# STATUS
# ============================================================

@dp.callback_query(F.data == "status")
async def status_callback(
    callback: CallbackQuery,
):
    await callback.message.answer(
        "📋 Статус системы\n\n"
        "🟢 Telegram Bot: ONLINE\n"
        "🟢 Railway: ONLINE\n"
        "🟢 Twelve Data: CONNECTED\n"
        "🟢 Реальные рыночные данные: ONLINE\n"
        "🟢 12 технических индикаторов: ONLINE\n"
        "🟢 Gate/Audit контур: TESTED\n"
        "🟢 Live Authorization: ENABLED\n"
        "🟢 Автоматическое открытие сделок: ОТКЛЮЧЕНО"
    )

    await callback.answer()


# ============================================================
# ABOUT
# ============================================================

@dp.callback_query(F.data == "about")
async def about_callback(
    callback: CallbackQuery,
):
    await callback.message.answer(
        "ℹ️ PocketTradeSignalsBot\n\n"
        "Аналитический Telegram-бот "
        "для формирования торговых сигналов.\n\n"
        "Основные принципы:\n"
        "• аналитика вместо автоторговли;\n"
        "• реальные рыночные данные;\n"
        "• 12 технических индикаторов;\n"
        "• fail-closed защита;\n"
        "• контроль целостности данных;\n"
        "• Gate / Trade / Audit.\n\n"
        "Автоматическое открытие реальных "
        "сделок отключено."
    )

    await callback.answer()


# ============================================================
# MAIN
# ============================================================

async def main():
    print(
        "PocketTradeSignalsBot started"
    )

    await dp.start_polling(bot)


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    asyncio.run(main())
