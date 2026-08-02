from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass(frozen=True)
class PredictionContract:
    prediction_hash: str
    model_version: str
    feature_version: str
    config_hash: str
    data_snapshot_hash: str
    raw_prediction: float
    probability: float

@dataclass(frozen=True)
class DecisionContract:
    decision_hash: str
    prediction_hash: str
    direction: str
    target_size: float
    confidence: float

@dataclass(frozen=True)
class TradeContract:
    trade_id: str
    decision_hash: str
    direction: str
    execution_price: float
    size: float
    fee: float
    pnl: Optional[float]
    slippage: float
    timestamp: int

@dataclass(frozen=True)
class GateContract:
    gate_run_id: str
    model_version: str
    feature_version: str
    config_hash: str
    data_snapshot_hash: str
    gate_config_hash: str
    trade_set_hash: str
    status: str
    fail_reason: Optional[str]
    created_at: datetime
    audit_hash: str
