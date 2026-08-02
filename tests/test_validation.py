import pytest

from core.contracts import TradeContract
from matching.validation import TradeValidationException, validate_trades


def make_trade(
    trade_id="trade-1",
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


def test_validate_trades_accepts_valid_trades():
    trades = [
        make_trade("trade-1"),
        make_trade("trade-2"),
    ]

    result = validate_trades(
        oos_trades=trades[:1],
        paper_trades=trades[1:],
        expected_decision_hash="decision-1",
        expected_direction="CALL",
    )

    assert len(result) == 2


def test_validate_trades_fails_without_trades():
    with pytest.raises(TradeValidationException, match="FAIL_MISSING_TRADE"):
        validate_trades(
            oos_trades=[],
            paper_trades=[],
            expected_decision_hash="decision-1",
            expected_direction="CALL",
        )


def test_validate_trades_fails_on_wrong_decision():
    with pytest.raises(TradeValidationException, match="FAIL_DECISION"):
        validate_trades(
            oos_trades=[make_trade(decision_hash="wrong-decision")],
            paper_trades=[],
            expected_decision_hash="decision-1",
            expected_direction="CALL",
        )


def test_validate_trades_fails_on_wrong_direction():
    with pytest.raises(TradeValidationException, match="FAIL_DIRECTION"):
        validate_trades(
            oos_trades=[make_trade(direction="PUT")],
            paper_trades=[],
            expected_decision_hash="decision-1",
            expected_direction="CALL",
        )


def test_validate_trades_fails_on_invalid_outcome():
    invalid_trade = make_trade()
    invalid_trade = TradeContract(
        trade_id=invalid_trade.trade_id,
        decision_hash=invalid_trade.decision_hash,
        direction=invalid_trade.direction,
        execution_price=0.0,
        size=invalid_trade.size,
        fee=invalid_trade.fee,
        pnl=invalid_trade.pnl,
        slippage=invalid_trade.slippage,
        timestamp=invalid_trade.timestamp,
    )

    with pytest.raises(TradeValidationException, match="FAIL_OUTCOME"):
        validate_trades(
            oos_trades=[invalid_trade],
            paper_trades=[],
            expected_decision_hash="decision-1",
            expected_direction="CALL",
        )
