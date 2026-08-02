import hashlib
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any, List
from core.contracts import GateContract, TradeContract

def _normalize(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("Datetime must be timezone-aware")
        return value.astimezone(timezone.utc).isoformat()
    if is_dataclass(value):
        return _normalize(asdict(value))
    if isinstance(value, dict):
        return {str(k): _normalize(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_normalize(v) for v in value]
    return value

def canonical_json(obj: Any) -> str:
    return json.dumps(_normalize(obj), ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def serialize_contract(obj: Any) -> str:
    return canonical_json(obj)

def contract_hash(obj: Any) -> str:
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()

def gate_payload_hash(gate: GateContract) -> str:
    return contract_hash({
        "gate_run_id": gate.gate_run_id,
        "model_version": gate.model_version,
        "feature_version": gate.feature_version,
        "config_hash": gate.config_hash,
        "data_snapshot_hash": gate.data_snapshot_hash,
        "gate_config_hash": gate.gate_config_hash,
        "trade_set_hash": gate.trade_set_hash,
        "status": gate.status,
        "fail_reason": gate.fail_reason,
        "created_at": gate.created_at,
    })

def compute_gate_audit_hash(gate: GateContract) -> str:
    return contract_hash({"gate_payload_hash": gate_payload_hash(gate), "gate_run_id": gate.gate_run_id})

def compute_trade_set_hash(trades: List[TradeContract]) -> str:
    return contract_hash(sorted(contract_hash(t) for t in trades))

def deserialize_gate(payload: str) -> GateContract:
    data = json.loads(payload)
    created_at = datetime.fromisoformat(data["created_at"])
    if created_at.tzinfo is None:
        raise ValueError("GateContract.created_at must be timezone-aware")
    return GateContract(
        gate_run_id=data["gate_run_id"], model_version=data["model_version"],
        feature_version=data["feature_version"], config_hash=data["config_hash"],
        data_snapshot_hash=data["data_snapshot_hash"], gate_config_hash=data["gate_config_hash"],
        trade_set_hash=data["trade_set_hash"], status=data["status"],
        fail_reason=data["fail_reason"], created_at=created_at.astimezone(timezone.utc),
        audit_hash=data["audit_hash"],
    )
