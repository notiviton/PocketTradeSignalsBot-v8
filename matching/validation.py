from typing import List
from core.contracts import TradeContract

class TradeValidationException(Exception):
    pass

def validate_trades(oos_trades: List[TradeContract], paper_trades: List[TradeContract],
                    expected_decision_hash: str, expected_direction: str) -> List[TradeContract]:
    combined = list(oos_trades) + list(paper_trades)
    if not combined:
        raise TradeValidationException("FAIL_MISSING_TRADE")

    seen = set()
    for trade in combined:
        if trade.trade_id in seen:
            raise TradeValidationException("FAIL_DUPLICATE_TRADE_ID")
        seen.add(trade.trade_id)
        if trade.decision_hash != expected_decision_hash:
            raise TradeValidationException("FAIL_DECISION")
        if trade.direction != expected_direction:
            raise TradeValidationException("FAIL_DIRECTION")
        if trade.execution_price <= 0 or trade.size <= 0:
            raise TradeValidationException("FAIL_OUTCOME")
        if trade.timestamp <= 0:
            raise TradeValidationException("FAIL_TIMESTAMP")
    return combined
