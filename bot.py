import asyncio
import os

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from data.assets import POCKET_OTC_ASSETS
from indicators.indicator_engine import IndicatorEngine
from services.forex_service import ForexService
from signals.signal_engine import SignalEngine


# ============================================================
# TELEGRAM BOT TOKEN
# ============================================================

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

if not TOKEN:
    raise RuntimeError(
        "TELEGRAM_BOT_TOKEN is not configured"
    )


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

user_settings: dict[int, dict[str, str]] = {}

DEFAULT_SYMBOL = os.getenv(
    "POCKET_OPTION_DEFAULT_SYMBOL",
    "EURUSD_otc",
)

DEFAULT_TIMEFRAME = os.getenv(
    "POCKET_OPTION_DEFAULT_TIMEFRAME",
    "1m",
)


# ============================================================
# TIMEFRAMES
# ============================================================

TIMEFRAMES = [
    "1m",
    "5m",
    "15m",
    "30m",
    "1h",
    "4h",
    "1D",
]


# ============================================================
# USER SETTINGS
# ============================================================

def get_user_settings(
    user_id: int,
) -> dict[str, str]:

    if user_id not in user_settings:

        user_settings[user_id] = {
            "symbol": DEFAULT_SYMBOL,
            "timeframe": DEFAULT_TIMEFRAME,
        }

    return user_settings[user_id]


# ============================================================
# SYMBOLS
# ============================================================

def get_pocket_symbols() -> list[str]:

    symbols: list[str] = []

    for _, asset_list in POCKET_OTC_ASSETS.items():

        for symbol in asset_list:

            if symbol not in symbols:
                symbols.append(symbol)

    return symbols


# ============================================================
# SYMBOL DISPLAY
# ============================================================

def display_symbol(
    symbol: str,
) -> str:

    return symbol


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

def settings_menu(
    user_id: int,
):

    settings = get_user_settings(user_id)

    builder = InlineKeyboardBuilder()

    builder.button(
        text=(
            "💱 Инструмент: "
            f"{display_symbol(settings['symbol'])}"
        ),
        callback_data="select_symbol",
    )

    builder.button(
        text=(
            "⏱ Таймфрейм: "
            f"{settings['timeframe']}"
        ),
        callback_data="select_timeframe",
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

    builder = InlineKeyboardBuilder()

    symbols = get_pocket_symbols()

    for symbol in symbols:

        builder.button(
            text=f"🟣 {display_symbol(symbol)}",
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

    builder = InlineKeyboardBuilder()

    for timeframe in TIMEFRAMES:

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
# FORMAT SIGNAL
# ============================================================

def format_signal(
    result,
    symbol: str,
    timeframe: str,
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
        f"🟢 Бычий счёт: "
        f"{result.bullish_score}\n"
        f"🔴 Медвежий счёт: "
        f"{result.bearish_score}\n\n"
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
        "Автоматическое открытие сделок "
        "отключено."
    )

    return text


# ============================================================
# FORMAT INDICATORS
# ============================================================

def format_indicators(
    indicators,
    symbol: str,
    timeframe: str,
):

    return (
        "📊 ТЕХНИЧЕСКИЕ ИНДИКАТОРЫ\n\n"
        f"💱 Инструмент: {symbol}\n"
        f"⏱ Таймфрейм: {timeframe}\n"
        f"💰 Цена Close: "
        f"{indicators['close']:.5f}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📈 RSI\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"RSI(14): "
        f"{indicators['rsi']:.2f}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📉 MACD\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"MACD: "
        f"{indicators['macd']:.6f}\n"
        f"Signal: "
        f"{indicators['macd_signal']:.6f}\n"
        f"Histogram: "
        f"{indicators['macd_histogram']:.6f}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📊 STOCHASTIC\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"%K: "
        f"{indicators['stochastic_k']:.2f}\n"
        f"%D: "
        f"{indicators['stochastic_d']:.2f}\n\n"

        "━━━━━━━━━━━━━━━━━━\n"
        "📐 BOLLINGER BANDS\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"Upper: "
        f"{indicators['bollinger_upper']:.5f}\n"
        f"Middle: "
        f"{indicators['bollinger_middle']:.5f}\n"
        f"Lower: "
        f"{indicators['bollinger_lower']:.5f}\n"
    )


# ============================================================
# START
# ============================================================

@dp.message(CommandStart())
async def start(
    message: Message,
):

    get_user_settings(
        message.from_user.id
    )

    await message.answer(
        "✅ PocketTradeSignalsBot запущен.\n\n"
        "📡 Источник данных:\n"
        "Pocket Option WebSocket\n\n"
        "Это аналитический бот.\n"
        "Автоматическое открытие реальных "
        "сделок отключено.\n\n"
        "Выберите действие:",
        reply_markup=main_menu(),
    )


# ============================================================
# TEST MARKET DATA
# ============================================================

@dp.message(Command("test_forex"))
async def test_forex(
    message: Message,
):

    settings = get_user_settings(
        message.from_user.id
    )

    await message.answer(
        "⏳ Проверяю подключение к "
        "Pocket Option WebSocket..."
    )

    try:

        result = await forex_service.get_last_prices(
            symbol=settings["symbol"],
            timeframe=settings["timeframe"],
        )

        await message.answer(result)

    except Exception as exc:

        await message.answer(
            "❌ Ошибка проверки рыночных данных.\n\n"
            f"Причина: {exc}"
        )


# ============================================================
# COMMAND: SIGNAL
# ============================================================

@dp.message(Command("signal"))
async def signal_command(
    message: Message,
):

    await message.answer(
        "📈 Чтобы получить сигнал, "
        "используйте кнопку "
        "«📈 Получить сигнал» "
        "в главном меню."
    )


# ============================================================
# COMMAND: ANALYSIS
# ============================================================

@dp.message(Command("analysis"))
async def analysis_command(
    message: Message,
):

    status = forex_service.status()

    websocket_status = status.get(
        "websocket",
        {},
    )

    connected = websocket_status.get(
        "connected",
        False,
    )

    authenticated = websocket_status.get(
        "authenticated",
        False,
    )

    market_status = (
        "🟢 ONLINE"
        if connected and authenticated
        else "🟡 CONNECTING / OFFLINE"
    )

    await message.answer(
        "📊 Анализ рынка\n\n"
        f"📡 Market Data Layer: "
        f"{market_status}\n"
        "🟢 Indicator Engine: ONLINE\n"
        "🟢 Signal Engine: ONLINE\n\n"
        "Источник: Pocket Option WebSocket\n"
        "Автоматическая торговля отключена."
    )


# ============================================================
# COMMAND: INDICATORS
# ============================================================

@dp.message(Command("indicators"))
async def indicators_command(
    message: Message,
):

    await message.answer(
        "📊 Чтобы посмотреть технические "
        "индикаторы, используйте кнопку "
        "«📊 Индикаторы» "
        "в главном меню."
    )


# ============================================================
# COMMAND: STATUS
# ============================================================

@dp.message(Command("status"))
async def status_command(
    message: Message,
):

    status = forex_service.status()

    websocket_status = status.get(
        "websocket",
        {},
    )

    connected = websocket_status.get(
        "connected",
        False,
    )

    authenticated = websocket_status.get(
        "authenticated",
        False,
    )

    running = websocket_status.get(
        "running",
        False,
    )

    market_status = (
        "🟢 ONLINE"
        if connected and authenticated
        else "🔴 OFFLINE"
    )

    auth_status = (
        "🟢 AUTHENTICATED"
        if authenticated
        else "🔴 NOT AUTHENTICATED"
    )

    runtime_status = (
        "🟢 RUNNING"
        if running
        else "🔴 STOPPED"
    )

    await message.answer(
        "📋 СТАТУС СИСТЕМЫ\n\n"

        "🤖 Telegram Bot: 🟢 ONLINE\n\n"

        "📡 Pocket Option WebSocket\n"
        f"Connection: {market_status}\n"
        f"Authentication: {auth_status}\n"
        f"Runtime: {runtime_status}\n\n"

        "🧮 Indicator Engine: 🟢 ONLINE\n"
        "🤖 Signal Engine: 🟢 ONLINE\n\n"

        f"📈 Ticks в буфере: "
        f"{websocket_status.get('ticks_buffered', 0)}\n\n"

        "⚠️ Аналитический режим.\n"
        "Автоматическое открытие сделок "
        "отключено."
    )


# ============================================================
# COMMAND: SETTINGS
# ============================================================

@dp.message(Command("settings"))
async def settings_command(
    message: Message,
):

    await message.answer(
        "⚙️ Чтобы изменить настройки, "
        "используйте кнопку "
        "«⚙️ Настройки» "
        "в главном меню."
    )


# ============================================================
# COMMAND: ABOUT
# ============================================================

@dp.message(Command("about"))
async def about_command(
    message: Message,
):

    await message.answer(
        "ℹ️ PocketTradeSignalsBot\n\n"

        "Аналитический Telegram-бот "
        "для анализа рыночных данных.\n\n"

        "📡 Источник:\n"
        "Pocket Option WebSocket\n\n"

        "Возможности:\n"
        "• Получение рыночных данных\n"
        "• Построение свечей\n"
        "• Расчёт технических индикаторов\n"
        "• Анализ Price Action\n"
        "• Формирование сигналов "
        "CALL / PUT / FLAT\n\n"

        "⚠️ Бот не открывает сделки автоматически.\n"
        "Все торговые решения принимает пользователь."
    )


# ============================================================
# SIGNAL CALLBACK
# ============================================================

@dp.callback_query(F.data == "signal")
async def signal_callback(
    callback: CallbackQuery,
):

    settings = get_user_settings(
        callback.from_user.id
    )

    symbol = settings["symbol"]
    timeframe = settings["timeframe"]

    await callback.message.answer(
        "⏳ Получаю рыночные данные...\n\n"
        f"💱 {symbol}\n"
        f"⏱ {timeframe}\n\n"
        "🕯 Загружаю 500 свечей..."
    )

    try:

        market = await forex_service.get_market(
            symbol=symbol,
            timeframe=timeframe,
            limit=500,
        )

        await callback.message.answer(
            "🧮 Рассчитываю индикаторы..."
        )

        indicator_engine = IndicatorEngine(
            market.candles
        )

        indicators = (
            indicator_engine.calculate_all()
        )

        await callback.message.answer(
            "🤖 Анализирую сигнал..."
        )

        result = signal_engine.analyze(
            indicators
        )

        await callback.message.answer(
            format_signal(
                result=result,
                symbol=symbol,
                timeframe=timeframe,
            )
        )

    except Exception as exc:

        await callback.message.answer(
            "❌ Ошибка формирования сигнала.\n\n"
            f"Причина: {exc}"
        )

    await callback.answer()


# ============================================================
# ANALYSIS CALLBACK
# ============================================================

@dp.callback_query(F.data == "analysis")
async def analysis_callback(
    callback: CallbackQuery,
):

    status = forex_service.status()

    websocket_status = status.get(
        "websocket",
        {},
    )

    connected = websocket_status.get(
        "connected",
        False,
    )

    authenticated = websocket_status.get(
        "authenticated",
        False,
    )

    market_status = (
        "🟢 ONLINE"
        if connected and authenticated
        else "🟡 CONNECTING / OFFLINE"
    )

    await callback.message.answer(
        "📊 Анализ рынка\n\n"
        f"📡 Market Data Layer: "
        f"{market_status}\n"
        "🟢 Indicator Engine: ONLINE\n"
        "🟢 Signal Engine: ONLINE\n\n"
        "Источник: Pocket Option WebSocket\n\n"
        "Автоматическая торговля отключена."
    )

    await callback.answer()


# ============================================================
# STATUS CALLBACK
# ============================================================

@dp.callback_query(F.data == "status")
async def status_callback(
    callback: CallbackQuery,
):

    status = forex_service.status()

    websocket_status = status.get(
        "websocket",
        {},
    )

    connected = websocket_status.get(
        "connected",
        False,
    )

    authenticated = websocket_status.get(
        "authenticated",
        False,
    )

    market_status = (
        "🟢 ONLINE"
        if connected and authenticated
        else "🔴 OFFLINE"
    )

    await callback.message.answer(
        "📋 СТАТУС СИСТЕМЫ\n\n"

        "🤖 Telegram Bot: 🟢 ONLINE\n"
        f"📡 Pocket Option WebSocket: "
        f"{market_status}\n"
        f"🔐 Authentication: "
        f"{'🟢 OK' if authenticated else '🔴 NO'}\n"
        "🧮 Indicator Engine: 🟢 ONLINE\n"
        "🤖 Signal Engine: 🟢 ONLINE\n\n"

        f"📈 Ticks в буфере: "
        f"{websocket_status.get('ticks_buffered', 0)}\n\n"

        "⚠️ Автоматическое открытие "
        "сделок отключено."
    )

    await callback.answer()


# ============================================================
# INDICATORS CALLBACK
# ============================================================

@dp.callback_query(F.data == "indicators")
async def indicators_callback(
    callback: CallbackQuery,
):

    settings = get_user_settings(
        callback.from_user.id
    )

    symbol = settings["symbol"]
    timeframe = settings["timeframe"]

    await callback.message.answer(
        "⏳ Получаю рыночные данные...\n\n"
        f"💱 {symbol}\n"
        f"⏱ {timeframe}\n\n"
        "🕯 Загружаю 500 свечей..."
    )

    try:

        market = await forex_service.get_market(
            symbol=symbol,
            timeframe=timeframe,
            limit=500,
        )

        await callback.message.answer(
            "🧮 Рассчитываю технические индикаторы..."
        )

        indicator_engine = IndicatorEngine(
            market.candles
        )

        indicators = (
            indicator_engine.calculate_all()
        )

        await callback.message.answer(
            format_indicators(
                indicators=indicators,
                symbol=symbol,
                timeframe=timeframe,
            )
        )

    except Exception as exc:

        await callback.message.answer(
            "❌ Ошибка расчёта индикаторов.\n\n"
            f"Причина: {exc}"
        )

    await callback.answer()


# ============================================================
# SETTINGS CALLBACK
# ============================================================

@dp.callback_query(F.data == "settings")
async def settings_callback(
    callback: CallbackQuery,
):

    settings = get_user_settings(
        callback.from_user.id
    )

    await callback.message.answer(
        "⚙️ НАСТРОЙКИ\n\n"
        f"💱 Инструмент: "
        f"{settings['symbol']}\n"
        f"⏱ Таймфрейм: "
        f"{settings['timeframe']}\n\n"
        "Источник данных:\n"
        "📡 Pocket Option WebSocket\n\n"
        "Выберите параметр для изменения:",
        reply_markup=settings_menu(
            callback.from_user.id
        ),
    )

    await callback.answer()


# ============================================================
# SELECT SYMBOL
# ============================================================

@dp.callback_query(
    F.data == "select_symbol"
)
async def select_symbol_callback(
    callback: CallbackQuery,
):

    await callback.message.answer(
        "💱 Выберите инструмент Pocket Option:",
        reply_markup=symbol_menu(),
    )

    await callback.answer()


# ============================================================
# SAVE SYMBOL
# ============================================================

@dp.callback_query(
    F.data.startswith("symbol:")
)
async def save_symbol_callback(
    callback: CallbackQuery,
):

    symbol = callback.data.split(
        ":",
        1,
    )[1]

    allowed_symbols = get_pocket_symbols()

    if symbol not in allowed_symbols:

        await callback.answer(
            "❌ Недопустимый инструмент.",
            show_alert=True,
        )

        return

    settings = get_user_settings(
        callback.from_user.id
    )

    settings["symbol"] = symbol

    await callback.message.answer(
        f"✅ Инструмент изменён:\n"
        f"💱 {symbol}",
        reply_markup=settings_menu(
            callback.from_user.id
        ),
    )

    await callback.answer()


# ============================================================
# SELECT TIMEFRAME
# ============================================================

@dp.callback_query(
    F.data == "select_timeframe"
)
async def select_timeframe_callback(
    callback: CallbackQuery,
):

    await callback.message.answer(
        "⏱ Выберите таймфрейм:",
        reply_markup=timeframe_menu(),
    )

    await callback.answer()


# ============================================================
# SAVE TIMEFRAME
# ============================================================

@dp.callback_query(
    F.data.startswith("timeframe:")
)
async def save_timeframe_callback(
    callback: CallbackQuery,
):

    timeframe = callback.data.split(
        ":",
        1,
    )[1]

    if timeframe not in TIMEFRAMES:

        await callback.answer(
            "❌ Недопустимый таймфрейм.",
            show_alert=True,
        )

        return

    settings = get_user_settings(
        callback.from_user.id
    )

    settings["timeframe"] = timeframe

    await callback.message.answer(
        f"✅ Таймфрейм изменён:\n"
        f"⏱ {timeframe}",
        reply_markup=settings_menu(
            callback.from_user.id
        ),
    )

    await callback.answer()


# ============================================================
# MAIN MENU CALLBACK
# ============================================================

@dp.callback_query(
    F.data == "main_menu"
)
async def main_menu_callback(
    callback: CallbackQuery,
):

    await callback.message.answer(
        "Главное меню:",
        reply_markup=main_menu(),
    )

    await callback.answer()


# ============================================================
# ABOUT CALLBACK
# ============================================================

@dp.callback_query(
    F.data == "about"
)
async def about_callback(
    callback: CallbackQuery,
):

    await callback.message.answer(
        "ℹ️ PocketTradeSignalsBot\n\n"

        "Аналитический Telegram-бот.\n\n"

        "Источник данных:\n"
        "📡 Pocket Option WebSocket\n\n"

        "Возможности:\n"
        "• Рыночные данные\n"
        "• Свечи\n"
        "• Индикаторы\n"
        "• Price Action\n"
        "• CALL / PUT / FLAT\n\n"

        "⚠️ Автоматическое открытие "
        "сделок отключено."
    )

    await callback.answer()


# ============================================================
# UNKNOWN CALLBACK SAFETY
# ============================================================

@dp.callback_query()
async def unknown_callback(
    callback: CallbackQuery,
):

    try:

        await callback.answer(
            "⚠️ Неизвестная команда.",
            show_alert=False,
        )

    except Exception:
        pass


# ============================================================
# MAIN
# ============================================================

async def main():

    print(
        "========================================"
    )

    print(
        "PocketTradeSignalsBot started"
    )

    print(
        "========================================"
    )

    print(
        "Market data source: "
        "Pocket Option WebSocket"
    )

    print(
        f"DEFAULT_SYMBOL={DEFAULT_SYMBOL}"
    )

    print(
        f"DEFAULT_TIMEFRAME={DEFAULT_TIMEFRAME}"
    )

    print(
        "Automatic trading: DISABLED"
    )

    print(
        "========================================"
    )

    try:

        # --------------------------------------------------------
        # Запускаем WebSocket data layer.
        # --------------------------------------------------------

        await forex_service.start()

        # --------------------------------------------------------
        # Telegram polling.
        # --------------------------------------------------------

        await bot.delete_webhook(
            drop_pending_updates=True
        )

        await dp.start_polling(
            bot
        )

    finally:

        # --------------------------------------------------------
        # Корректно закрываем WebSocket.
        # --------------------------------------------------------

        await forex_service.stop()

        await bot.session.close()


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    asyncio.run(main())
