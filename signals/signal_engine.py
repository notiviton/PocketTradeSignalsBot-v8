from dataclasses import dataclass
from typing import Dict, List


@dataclass
class SignalResult:
    signal: str
    confidence: float
    bullish_score: int
    bearish_score: int
    reasons: List[str]
    warnings: List[str]


class SignalEngine:
    """
    Детерминированный сигнальный движок.

    Возможные результаты:
    CALL
    PUT
    FLAT

    Автоматическое открытие сделок отсутствует.
    """

    def __init__(
        self,
        min_score: int = 7,
        min_confidence: float = 65.0,
    ):
        self.min_score = min_score
        self.min_confidence = min_confidence

    # ========================================================
    # MAIN ANALYSIS
    # ========================================================

    def analyze(
        self,
        indicators: Dict,
    ) -> SignalResult:

        bullish_score = 0
        bearish_score = 0

        reasons = []
        warnings = []

        # ====================================================
        # RSI
        # ====================================================

        rsi = indicators["rsi"]

        if rsi > 55:
            bullish_score += 1
            reasons.append(
                f"RSI {rsi:.2f}: бычье преимущество"
            )

        elif rsi < 45:
            bearish_score += 1
            reasons.append(
                f"RSI {rsi:.2f}: медвежье преимущество"
            )

        else:
            warnings.append(
                f"RSI {rsi:.2f}: нейтральная зона"
            )

        # ====================================================
        # MACD
        # ====================================================

        macd = indicators["macd"]
        macd_signal = indicators["macd_signal"]

        if macd > macd_signal:
            bullish_score += 1
            reasons.append(
                "MACD выше сигнальной линии"
            )

        elif macd < macd_signal:
            bearish_score += 1
            reasons.append(
                "MACD ниже сигнальной линии"
            )

        # ====================================================
        # STOCHASTIC
        # ====================================================

        stochastic_k = indicators["stochastic_k"]
        stochastic_d = indicators["stochastic_d"]

        if stochastic_k > stochastic_d:
            bullish_score += 1
            reasons.append(
                "Stochastic: %K выше %D"
            )

        elif stochastic_k < stochastic_d:
            bearish_score += 1
            reasons.append(
                "Stochastic: %K ниже %D"
            )

        # ====================================================
        # BOLLINGER BANDS
        # ====================================================

        close = indicators["close"]

        upper = indicators["bollinger_upper"]
        middle = indicators["bollinger_middle"]
        lower = indicators["bollinger_lower"]

        if close > middle:
            bullish_score += 1
            reasons.append(
                "Цена выше средней Bollinger Bands"
            )

        elif close < middle:
            bearish_score += 1
            reasons.append(
                "Цена ниже средней Bollinger Bands"
            )

        # ====================================================
        # EMA 9 / 21
        # ====================================================

        ema_9 = indicators["ema_9"]
        ema_21 = indicators["ema_21"]

        if ema_9 > ema_21:
            bullish_score += 1
            reasons.append(
                "EMA 9 выше EMA 21"
            )

        elif ema_9 < ema_21:
            bearish_score += 1
            reasons.append(
                "EMA 9 ниже EMA 21"
            )

        # ====================================================
        # EMA 50 / 200
        # ====================================================

        ema_50 = indicators["ema_50"]
        ema_200 = indicators["ema_200"]

        if ema_50 > ema_200:
            bullish_score += 1
            reasons.append(
                "EMA 50 выше EMA 200"
            )

        elif ema_50 < ema_200:
            bearish_score += 1
            reasons.append(
                "EMA 50 ниже EMA 200"
            )

        # ====================================================
        # ADX + DI
        # ====================================================

        adx = indicators["adx"]
        plus_di = indicators["plus_di"]
        minus_di = indicators["minus_di"]

        if adx >= 20:

            if plus_di > minus_di:
                bullish_score += 2
                reasons.append(
                    f"ADX {adx:.2f}: сильный бычий тренд"
                )

            elif minus_di > plus_di:
                bearish_score += 2
                reasons.append(
                    f"ADX {adx:.2f}: сильный медвежий тренд"
                )

        else:
            warnings.append(
                f"ADX {adx:.2f}: тренд недостаточно сильный"
            )

        # ====================================================
        # CCI
        # ====================================================

        cci = indicators["cci"]

        if cci > 50:
            bullish_score += 1
            reasons.append(
                f"CCI {cci:.2f}: бычье преимущество"
            )

        elif cci < -50:
            bearish_score += 1
            reasons.append(
                f"CCI {cci:.2f}: медвежье преимущество"
            )

        # ====================================================
        # WILLIAMS %R
        # ====================================================

        williams_r = indicators["williams_r"]

        if williams_r > -50:
            bullish_score += 1
            reasons.append(
                f"Williams %R {williams_r:.2f}: бычье преимущество"
            )

        elif williams_r < -50:
            bearish_score += 1
            reasons.append(
                f"Williams %R {williams_r:.2f}: медвежье преимущество"
            )

        # ====================================================
        # TOTAL SCORE
        # ====================================================

        total_score = (
            bullish_score
            + bearish_score
        )

        if total_score == 0:

            return SignalResult(
                signal="FLAT",
                confidence=0.0,
                bullish_score=0,
                bearish_score=0,
                reasons=reasons,
                warnings=[
                    "Недостаточно данных для формирования сигнала"
                ],
            )

        dominant_score = max(
            bullish_score,
            bearish_score,
        )

        confidence = (
            dominant_score
            / total_score
        ) * 100

        # ====================================================
        # SIGNAL DECISION
        # ====================================================

        signal = "FLAT"

        if (
            bullish_score >= self.min_score
            and bullish_score > bearish_score
            and confidence >= self.min_confidence
        ):
            signal = "CALL"

        elif (
            bearish_score >= self.min_score
            and bearish_score > bullish_score
            and confidence >= self.min_confidence
        ):
            signal = "PUT"

        else:
            signal = "FLAT"

            warnings.append(
                "Недостаточно подтверждений для безопасного сигнала"
            )

        # ====================================================
        # CONFLICT CHECK
        # ====================================================

        score_difference = abs(
            bullish_score
            - bearish_score
        )

        if score_difference <= 2:
            warnings.append(
                "Индикаторы находятся в конфликтной зоне"
            )

            signal = "FLAT"

        # ====================================================
        # RETURN
        # ====================================================

        return SignalResult(
            signal=signal,
            confidence=round(
                confidence,
                2,
            ),
            bullish_score=bullish_score,
            bearish_score=bearish_score,
            reasons=reasons,
            warnings=warnings,
        )
