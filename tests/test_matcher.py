import pytest

from core.contracts import TradeContract
from matching.matcher import TradeMatcher
from matching.validation import TradeValidationException


def make_trade(
    trade_id,
    decision_hash="decision-1",
    direction="CALL",
):
    return TradeContract(
        trade_id=trade_id,
        decision_hash=decision_hash,
        direction=direction,
        execution_price=100.0,
        size=1.0,
        fee=0.1,
        pnl=None,
        slippage=0.01,
        timestamp=1700000000,
    )


def test_match_and_build_set_returns_trades_and_hash():
    oos = [make_trade("oos-1")]
    paper = [make_trade("paper-1")]

    trades, trade_set_hash = TradeMatcher.match_and_build_set(
        oos_trades=oos,
        paper_trades=paper,
        expected_decision_hash="decision-1",
        expected_direction="CALL",
    )

    assert len(trades) == 2
    assert isinstance(trade_set_hash, str)
    assert len(trade_set_hash) == 64


def test_matcher_fails_on_invalid_trade():
    with pytest.raises(TradeValidationException, match="FAIL_DIRECTION"):
        TradeMatcher.match_and_build_set(
            oos_trades=[make_trade("oos-1", direction="PUT")],
            paper_trades=[],
            expected_decision_hash="decision-1",
            expected_direction="CALL",
        )


def test_trade_set_hash_is_order_independent():
    oos_1 = [make_trade("trade-1"), make_trade("trade-2")]
    oos_2 = [make_trade("trade-2"), make_trade("trade-1")]

    _, hash_1 = TradeMatcher.match_and_build_set(
        oos_trades=oos_1,
        paper_trades=[],
        expected_decision_hash="decision-1",
        expected_direction="CALL",
    )

    _, hash_2 = TradeMatcher.match_and_build_set(
        oos_trades=oos_2,
        paper_trades=[],
        expected_decision_hash="decision-1",
        expected_direction="CALL",
    )

    assert hash_1 == hash_2
