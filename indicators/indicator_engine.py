import pandas as pd
import numpy as np


class IndicatorEngine:
    """
    Расчёт 12 технических индикаторов
    на едином наборе свечных данных.

    Индикаторы:
    1. RSI
    2. MACD
    3. Stochastic
    4. Bollinger Bands
    5. EMA 9
    6. EMA 21
    7. EMA 50
    8. EMA 200
    9. ADX
    10. ATR
    11. CCI
    12. Williams %R
    """

    def __init__(self, candles):
        self.df = self._prepare_dataframe(candles)

    # ========================================================
    # PREPARE DATA
    # ========================================================

    @staticmethod
    def _prepare_dataframe(candles):

        rows = []

        for candle in candles:
            rows.append(
                {
                    "open": float(candle.open),
                    "high": float(candle.high),
                    "low": float(candle.low),
                    "close": float(candle.close),
                }
            )

        if not rows:
            raise ValueError(
                "Нет свечных данных для расчёта индикаторов."
            )

        df = pd.DataFrame(rows)

        required_columns = [
            "open",
            "high",
            "low",
            "close",
        ]

        for column in required_columns:
            if column not in df.columns:
                raise ValueError(
                    f"Отсутствует колонка: {column}"
                )

        df = df.replace(
            [np.inf, -np.inf],
            np.nan,
        )

        df = df.dropna()

        if len(df) < 200:
            raise ValueError(
                "Недостаточно свечей для расчёта EMA 200. "
                f"Получено: {len(df)}, необходимо минимум 200."
            )

        return df.reset_index(drop=True)

    # ========================================================
    # RSI
    # ========================================================

    def calculate_rsi(self, period=14):

        delta = self.df["close"].diff()

        gain = delta.clip(
            lower=0
        )

        loss = -delta.clip(
            upper=0
        )

        average_gain = gain.ewm(
            alpha=1 / period,
            adjust=False,
            min_periods=period,
        ).mean()

        average_loss = loss.ewm(
            alpha=1 / period,
            adjust=False,
            min_periods=period,
        ).mean()

        rs = average_gain / average_loss.replace(
            0,
            np.nan,
        )

        rsi = 100 - (
            100 / (1 + rs)
        )

        return rsi

    # ========================================================
    # EMA
    # ========================================================

    def calculate_ema(self, period):

        return self.df["close"].ewm(
            span=period,
            adjust=False,
            min_periods=period,
        ).mean()

    # ========================================================
    # MACD
    # ========================================================

    def calculate_macd(self):

        ema_fast = self.calculate_ema(
            12
        )

        ema_slow = self.calculate_ema(
            26
        )

        macd = (
            ema_fast
            - ema_slow
        )

        signal = macd.ewm(
            span=9,
            adjust=False,
            min_periods=9,
        ).mean()

        histogram = (
            macd
            - signal
        )

        return {
            "macd": macd,
            "signal": signal,
            "histogram": histogram,
        }

    # ========================================================
    # STOCHASTIC
    # ========================================================

    def calculate_stochastic(
        self,
        period=14,
        smooth_k=3,
        smooth_d=3,
    ):

        lowest_low = (
            self.df["low"]
            .rolling(
                window=period,
                min_periods=period,
            )
            .min()
        )

        highest_high = (
            self.df["high"]
            .rolling(
                window=period,
                min_periods=period,
            )
            .max()
        )

        denominator = (
            highest_high
            - lowest_low
        ).replace(
            0,
            np.nan,
        )

        raw_k = (
            (
                self.df["close"]
                - lowest_low
            )
            / denominator
        ) * 100

        k = raw_k.rolling(
            window=smooth_k,
            min_periods=smooth_k,
        ).mean()

        d = k.rolling(
            window=smooth_d,
            min_periods=smooth_d,
        ).mean()

        return {
            "k": k,
            "d": d,
        }

    # ========================================================
    # BOLLINGER BANDS
    # ========================================================

    def calculate_bollinger(
        self,
        period=20,
        std_multiplier=2,
    ):

        middle = (
            self.df["close"]
            .rolling(
                window=period,
                min_periods=period,
            )
            .mean()
        )

        std = (
            self.df["close"]
            .rolling(
                window=period,
                min_periods=period,
            )
            .std()
        )

        upper = (
            middle
            + std_multiplier * std
        )

        lower = (
            middle
            - std_multiplier * std
        )

        return {
            "upper": upper,
            "middle": middle,
            "lower": lower,
        }

    # ========================================================
    # ATR
    # ========================================================

    def calculate_atr(
        self,
        period=14,
    ):

        previous_close = (
            self.df["close"]
            .shift(1)
        )

        tr1 = (
            self.df["high"]
            - self.df["low"]
        )

        tr2 = (
            self.df["high"]
            - previous_close
        ).abs()

        tr3 = (
            self.df["low"]
            - previous_close
        ).abs()

        true_range = pd.concat(
            [
                tr1,
                tr2,
                tr3,
            ],
            axis=1,
        ).max(
            axis=1
        )

        atr = true_range.ewm(
            alpha=1 / period,
            adjust=False,
            min_periods=period,
        ).mean()

        return atr

    # ========================================================
    # ADX
    # ========================================================

    def calculate_adx(
        self,
        period=14,
    ):

        high = self.df["high"]
        low = self.df["low"]
        close = self.df["close"]

        previous_high = high.shift(1)
        previous_low = low.shift(1)
        previous_close = close.shift(1)

        plus_move = (
            high
            - previous_high
        )

        minus_move = (
            previous_low
            - low
        )

        plus_dm = pd.Series(
            np.where(
                (
                    (plus_move > minus_move)
                    & (plus_move > 0)
                ),
                plus_move,
                0,
            ),
            index=self.df.index,
        )

        minus_dm = pd.Series(
            np.where(
                (
                    (minus_move > plus_move)
                    & (minus_move > 0)
                ),
                minus_move,
                0,
            ),
            index=self.df.index,
        )

        tr1 = high - low

        tr2 = (
            high
            - previous_close
        ).abs()

        tr3 = (
            low
            - previous_close
        ).abs()

        true_range = pd.concat(
            [
                tr1,
                tr2,
                tr3,
            ],
            axis=1,
        ).max(
            axis=1
        )

        atr = true_range.ewm(
            alpha=1 / period,
            adjust=False,
            min_periods=period,
        ).mean()

        plus_di = (
            100
            * plus_dm.ewm(
                alpha=1 / period,
                adjust=False,
                min_periods=period,
            ).mean()
            / atr
        )

        minus_di = (
            100
            * minus_dm.ewm(
                alpha=1 / period,
                adjust=False,
                min_periods=period,
            ).mean()
            / atr
        )

        di_sum = (
            plus_di
            + minus_di
        ).replace(
            0,
            np.nan,
        )

        dx = (
            100
            * (
                plus_di
                - minus_di
            ).abs()
            / di_sum
        )

        adx = dx.ewm(
            alpha=1 / period,
            adjust=False,
            min_periods=period,
        ).mean()

        return {
            "adx": adx,
            "plus_di": plus_di,
            "minus_di": minus_di,
        }

    # ========================================================
    # CCI
    # ========================================================

    def calculate_cci(
        self,
        period=20,
    ):

        typical_price = (
            self.df["high"]
            + self.df["low"]
            + self.df["close"]
        ) / 3

        sma = (
            typical_price
            .rolling(
                window=period,
                min_periods=period,
            )
            .mean()
        )

        mean_deviation = (
            typical_price
            .rolling(
                window=period,
                min_periods=period,
            )
            .apply(
                lambda values: np.mean(
                    np.abs(
                        values
                        - np.mean(values)
                    )
                ),
                raw=True,
            )
        )

        denominator = (
            0.015
            * mean_deviation
        ).replace(
            0,
            np.nan,
        )

        cci = (
            typical_price
            - sma
        ) / denominator

        return cci

    # ========================================================
    # WILLIAMS %R
    # ========================================================

    def calculate_williams_r(
        self,
        period=14,
    ):

        highest_high = (
            self.df["high"]
            .rolling(
                window=period,
                min_periods=period,
            )
            .max()
        )

        lowest_low = (
            self.df["low"]
            .rolling(
                window=period,
                min_periods=period,
            )
            .min()
        )

        denominator = (
            highest_high
            - lowest_low
        ).replace(
            0,
            np.nan,
        )

        williams_r = (
            (
                highest_high
                - self.df["close"]
            )
            / denominator
        ) * -100

        return williams_r

    # ========================================================
    # ALL INDICATORS
    # ========================================================

    def calculate_all(self):

        macd = self.calculate_macd()

        stochastic = (
            self.calculate_stochastic()
        )

        bollinger = (
            self.calculate_bollinger()
        )

        adx = self.calculate_adx()

        latest = {
            "rsi": self.calculate_rsi().iloc[-1],

            "macd": macd["macd"].iloc[-1],
            "macd_signal": macd["signal"].iloc[-1],
            "macd_histogram": macd[
                "histogram"
            ].iloc[-1],

            "stochastic_k": stochastic[
                "k"
            ].iloc[-1],

            "stochastic_d": stochastic[
                "d"
            ].iloc[-1],

            "bollinger_upper": bollinger[
                "upper"
            ].iloc[-1],

            "bollinger_middle": bollinger[
                "middle"
            ].iloc[-1],

            "bollinger_lower": bollinger[
                "lower"
            ].iloc[-1],

            "ema_9": self.calculate_ema(
                9
            ).iloc[-1],

            "ema_21": self.calculate_ema(
                21
            ).iloc[-1],

            "ema_50": self.calculate_ema(
                50
            ).iloc[-1],

            "ema_200": self.calculate_ema(
                200
            ).iloc[-1],

            "adx": adx[
                "adx"
            ].iloc[-1],

            "plus_di": adx[
                "plus_di"
            ].iloc[-1],

            "minus_di": adx[
                "minus_di"
            ].iloc[-1],

            "atr": self.calculate_atr().iloc[-1],

            "cci": self.calculate_cci().iloc[-1],

            "williams_r": self.calculate_williams_r().iloc[-1],

            "close": self.df[
                "close"
            ].iloc[-1],
        }

        return latest
