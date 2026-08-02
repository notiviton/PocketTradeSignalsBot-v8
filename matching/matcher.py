from typing import List, Tuple
from core.contracts import TradeContract
from matching.validation import validate_trades
from storage.serializers import compute_trade_set_hash

class TradeMatcher:
    @staticmethod
    def match_and_build_set(oos_trades: List[TradeContract], paper_trades: List[TradeContract],
                            expected_decision_hash: str, expected_direction: str) -> Tuple[List[TradeContract], str]:
        matched = validate_trades(oos_trades, paper_trades, expected_decision_hash, expected_direction)
        return matched, compute_trade_set_hash(matched)
