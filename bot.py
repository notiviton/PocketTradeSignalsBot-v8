import asyncio
import os

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from indicators.indicator_engine import IndicatorEngine
from services.forex_service import ForexService
from signals.signal_engine import SignalEngine
from data.assets import FOREX_ASSETS, POCKET_OTC_ASSETS


# ============================================================
# TELEGRAM BOT TOKEN
# ============================================================

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

if not TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")


# ============================================================
# BOT
# ============================================================

bot = Bot(token=TOKEN)
dp = Dispatcher()


# ============================================================
# SERVICES
# ============================================================

forex_service = ForexService()
signal_engine = SignalEngine()


# ============================================================
# USER SETTINGS
# ============================================================

user_settings = {}

DEFAULT_SYMBOL = "EUR/USD"
DEFAULT_TIMEFRAME = "1m"

DEFAULT_SOURCE = os.getenv(
    "MARKET_DATA_SOURCE",
    "twelvedata",
).lower()


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

    builder.button(
        text="📡 Twelve Data",
        callback_data="source:twelvedata",
    )

    builder.button(
        text="🟣 Pocket Option OTC",
        callback_data="source:pocket_otc",
    )

    builder.button(
        text="⬅️ Назад",
        callback_data="settings",
    )

    builder.adjust(1)

    return builder.as_markup()


# ============================================================
# FORMAT SIGNAL
# ============================================================

def format_signal(
    result,
    symbol,
    timeframe,
):

    signal_name = {
        "CALL": "🟢 CALL",
        "PUT": "🔴 PUT",
        "FLAT": "🟡 FLAT",
    }.get(
        result.signal,
        result.signal,
    )

    text = (

        "📈 ТОРГОВЫЙ СИГНАЛ\n\n"

        f"💱 Инструмент: {symbol}\n"
        f"⏱ Таймфрейм: {timeframe}\n\n"

        f"Сигнал: {signal_name}\n"

        f"Уверенность: {result.confidence:.2f}%\n\n"

        f"🟢 Бычий счёт: {result.bullish_score}\n"

        f"🔴 Медвежий счёт: {result.bearish_score}\n\n"

        "Причины:\n"
    )


    for reason in result.reasons:

        text += f"• {reason}\n"


    if result.warnings:

        text += "\n⚠️ Предупреждения:\n"

        for warning in result.warnings:

            text += f"• {warning}\n"


    text += (

        "\n⚠️ Только аналитика.\n"

        "Автоматическое открытие сделок отключено."

    )


    return text


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

        f"Lower: {indicators['bollinger_lower']:.5f}\n"

    )


# ============================================================
# START
# ============================================================

@dp.message(CommandStart())
async def start(message: Message):

    get_user_settings(
        message.from_user.id
    )

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

    settings = get_user_settings(
        message.from_user.id
    )

    await message.answer(
        "⏳ Проверяю подключение к источнику данных..."
    )

    result = forex_service.get_last_prices(
        symbol=settings["symbol"],
        timeframe=settings["timeframe"],
        source=settings["source"],
    )

    await message.answer(result)


# ============================================================
# COMMAND: SIGNAL
# ============================================================

@dp.message(Command("signal"))
async def signal_command(message: Message):

    settings = get_user_settings(
        message.from_user.id
    )

    await message.answer(
        "📈 Чтобы получить сигнал, "
        "используйте кнопку «📈 Получить сигнал» "
        "в главном меню."
    )


# ============================================================
# COMMAND: ANALYSIS
# ============================================================

@dp.message(Command("analysis"))
async def analysis_command(message: Message):

    await message.answer(
        "📊 Анализ рынка\n\n"
        "🟢 Market Data Layer: ONLINE\n"
        "🟢 Indicator Engine: ONLINE\n"
        "🟢 Signal Engine: ONLINE\n\n"
        "Автоматическая торговля отключена."
    )


# ============================================================
# COMMAND: INDICATORS
# ============================================================

@dp.message(Command("indicators"))
async def indicators_command(message: Message):

    await message.answer(
        "📊 Чтобы посмотреть технические индикаторы, "
        "используйте кнопку «📊 Индикаторы» "
        "в главном меню."
    )


# ============================================================
# COMMAND: STATUS
# ============================================================

@dp.message(Command("status"))
async def status_command(message: Message):

    await message.answer(
        "📋 Статус системы\n\n"
        "🟢 Telegram Bot: ONLINE\n"
        "🟢 Market Data Layer: ONLINE\n"
        "🟢 Indicator Engine: ONLINE\n"
        "🟢 Signal Engine: ONLINE\n\n"
        "⚠️ Автоматическое открытие сделок отключено."
    )


# ============================================================
# COMMAND: SETTINGS
# ============================================================

@dp.message(Command("settings"))
async def settings_command(message: Message):

    await message.answer(
        "⚙️ Настройки\n\n"
        "Используйте кнопку «⚙️ Настройки» "
        "в главном меню."
    )


# ============================================================
# COMMAND: ABOUT
# ============================================================

@dp.message(Command("about"))
async def about_command(message: Message):

    await message.answer(
        "ℹ️ PocketTradeSignalsBot\n\n"
        "Аналитический Telegram-бот "
        "для анализа рыночных данных.\n\n"
        "Бот рассчитывает технические индикаторы "
        "и формирует сигналы:\n"
        "🟢 CALL\n"
        "🔴 PUT\n"
        "🟡 FLAT\n\n"
        "⚠️ Бот не открывает сделки автоматически.\n"
        "Все решения принимает пользователь."
    )


# ============================================================
# SIGNAL
# ============================================================

@dp.callback_query(F.data == "signal")
async def signal_callback(callback: CallbackQuery):

    settings = get_user_settings(
        callback.from_user.id
    )


    await callback.message.answer(

        "⏳ Получаю рыночные данные...\n\n"

        f"💱 {settings['symbol']}\n"

        f"⏱ {settings['timeframe']}\n\n"

        "🕯 Загружаю 500 свечей..."

    )


    try:

        market = forex_service.get_market(

            source=settings["source"],

            symbol=settings["symbol"],

            timeframe=settings["timeframe"],

            limit=500,

        )


        await callback.message.answer(

            "🧮 Рассчитываю индикаторы..."

        )


        indicator_engine = IndicatorEngine(

            market.candles

        )


        indicators = indicator_engine.calculate_all()



        await callback.message.answer(

            "🤖 Анализирую сигнал..."

        )



        result = signal_engine.analyze(

            indicators

        )



        await callback.message.answer(

            format_signal(

                result=result,

                symbol=settings["symbol"],

                timeframe=settings["timeframe"],

            )

        )


    except Exception as exc:

        await callback.message.answer(

            "❌ Ошибка формирования сигнала.\n\n"

            f"Причина: {exc}"

        )


    await callback.answer()



# ============================================================
# ANALYSIS
# ============================================================

@dp.callback_query(F.data == "analysis")
async def analysis_callback(callback: CallbackQuery):

    await callback.message.answer(

        "📊 Анализ рынка\n\n"

        "🟢 Market Data Layer: ONLINE\n"

        "🟢 Indicator Engine: ONLINE\n"

        "🟢 Signal Engine: ONLINE\n\n"

        "Автоматическая торговля отключена."

    )

    await callback.answer()



# ============================================================
# STATUS
# ============================================================

@dp.callback_query(F.data == "status")
async def status_callback(callback: CallbackQuery):

    await callback.message.answer(

        "📋 Статус системы\n\n"

        "🟢 Telegram Bot: ONLINE\n"

        "🟢 Market Data Layer: ONLINE\n"

        "🟢 Indicator Engine: ONLINE\n"

        "🟢 Signal Engine: ONLINE\n\n"

        "⚠️ Автоматическое открытие сделок отключено."

    )

    await callback.answer()


# ============================================================
# INDICATORS
# ============================================================

@dp.callback_query(F.data == "indicators")
async def indicators_callback(callback: CallbackQuery):

    settings = get_user_settings(
        callback.from_user.id
    )

    await callback.message.answer(
        "⏳ Получаю рыночные данные...\n\n"
        f"💱 {settings['symbol']}\n"
        f"⏱ {settings['timeframe']}\n\n"
        "🕯 Загружаю 500 свечей..."
    )

    try:

        market = forex_service.get_market(
            source=settings["source"],
            symbol=settings["symbol"],
            timeframe=settings["timeframe"],
            limit=500,
        )

        await callback.message.answer(
            "🧮 Рассчитываю технические индикаторы..."
        )

        indicator_engine = IndicatorEngine(
            market.candles
        )

        indicators = indicator_engine.calculate_all()

        await callback.message.answer(
            format_indicators(
                indicators=indicators,
                symbol=settings["symbol"],
                timeframe=settings["timeframe"],
            )
        )

    except Exception as exc:

        await callback.message.answer(
            "❌ Ошибка расчёта индикаторов.\n\n"
            f"Причина: {exc}"
        )

    await callback.answer()


# ============================================================
# SETTINGS
# ============================================================

@dp.callback_query(F.data == "settings")
async def settings_callback(callback: CallbackQuery):

    settings = get_user_settings(
        callback.from_user.id
    )

    await callback.message.answer(
        "⚙️ НАСТРОЙКИ\n\n"
        f"💱 Инструмент: {settings['symbol']}\n"
        f"⏱ Таймфрейм: {settings['timeframe']}\n"
        f"📡 Источник: {settings['source']}\n\n"
        "Выберите параметр для изменения:",
        reply_markup=settings_menu(
            callback.from_user.id
        ),
    )

    await callback.answer()


# ============================================================
# SELECT SYMBOL
# ============================================================

@dp.callback_query(F.data == "select_symbol")
async def select_symbol_callback(callback: CallbackQuery):

    await callback.message.answer(
        "💱 Выберите инструмент:",
        reply_markup=symbol_menu(),
    )

    await callback.answer()


# ============================================================
# SAVE SYMBOL
# ============================================================

@dp.callback_query(F.data.startswith("symbol:"))
async def save_symbol_callback(callback: CallbackQuery):

    symbol = callback.data.split(":", 1)[1]

    settings = get_user_settings(callback.from_user.id)
    settings["symbol"] = symbol

    await callback.message.answer(
        f"✅ Инструмент изменён на {symbol}",
        reply_markup=settings_menu(callback.from_user.id),
    )

    await callback.answer()


# ============================================================
# SELECT TIMEFRAME
# ============================================================

@dp.callback_query(F.data == "select_timeframe")
async def select_timeframe_callback(callback: CallbackQuery):

    await callback.message.answer(
        "⏱ Выберите таймфрейм:",
        reply_markup=timeframe_menu(),
    )

    await callback.answer()


# ============================================================
# SAVE TIMEFRAME
# ============================================================

@dp.callback_query(F.data.startswith("timeframe:"))
async def save_timeframe_callback(callback: CallbackQuery):

    timeframe = callback.data.split(":", 1)[1]

    settings = get_user_settings(callback.from_user.id)
    settings["timeframe"] = timeframe

    await callback.message.answer(
        f"✅ Таймфрейм изменён на {timeframe}",
        reply_markup=settings_menu(callback.from_user.id),
    )

    await callback.answer()


# ============================================================
# SELECT SOURCE
# ============================================================

@dp.callback_query(F.data == "select_source")
async def select_source_callback(callback: CallbackQuery):

    await callback.message.answer(
        "📡 Выберите источник данных:",
        reply_markup=source_menu(),
    )

    await callback.answer()


# ============================================================
# SAVE SOURCE
# ============================================================

@dp.callback_query(F.data.startswith("source:"))
async def save_source_callback(callback: CallbackQuery):

    source = callback.data.split(":", 1)[1]

    settings = get_user_settings(callback.from_user.id)
    settings["source"] = source

    await callback.message.answer(
        f"✅ Источник изменён на {source}",
        reply_markup=settings_menu(callback.from_user.id),
    )

    await callback.answer()


# ============================================================
# MAIN MENU
# ============================================================

@dp.callback_query(F.data == "main_menu")
async def main_menu_callback(callback: CallbackQuery):

    await callback.message.answer(
        "Главное меню:",
        reply_markup=main_menu(),
    )

    await callback.answer()


# ============================================================
# ABOUT
# ============================================================

@dp.callback_query(F.data == "about")
async def about_callback(callback: CallbackQuery):

    await callback.message.answer(
        "ℹ️ PocketTradeSignalsBot\n\n"
        "Аналитический Telegram-бот.\n\n"
        "Возможности:\n"
        "• Получение рыночных данных\n"
        "• Расчёт технических индикаторов\n"
        "• Формирование сигналов CALL / PUT / FLAT\n\n"
        "⚠️ Автоматическое открытие сделок отключено."
    )

    await callback.answer()


# ============================================================
# MAIN
# ============================================================

async def main():

    print(
        "PocketTradeSignalsBot started"
    )
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(
        bot
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    asyncio.run(main())
