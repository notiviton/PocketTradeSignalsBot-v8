from datetime import datetime, timezone

import pytest

from core.contracts import GateContract, TradeContract
from storage.serializers import (
    canonical_json,
    compute_gate_audit_hash,
    compute_trade_set_hash,
    contract_hash,
    deserialize_gate,
    serialize_contract,
)


def make_trade(trade_id):
    return TradeContract(
        trade_id=trade_id,
        decision_hash="decision-1",
        direction="CALL",
        execution_price=100.0,
        size=1.0,
        fee=0.1,
        pnl=None,
        slippage=0.01,
        timestamp=1700000000,
    )


def make_gate(trade_set_hash="trade-set-1"):
    return GateContract(
        gate_run_id="gate-1",
        model_version="model-v1",
        feature_version="features-v1",
        config_hash="config-1",
        data_snapshot_hash="snapshot-1",
        gate_config_hash="gate-config-1",
        trade_set_hash=trade_set_hash,
        status="PASS",
        fail_reason=None,
        created_at=datetime(
            2026,
            1,
            1,
            12,
            0,
            tzinfo=timezone.utc,
        ),
        audit_hash="audit-1",
    )


def test_canonical_json_is_deterministic():
    first = {"b": 2, "a": 1}
    second = {"a": 1, "b": 2}

    assert canonical_json(first) == canonical_json(second)
    assert contract_hash(first) == contract_hash(second)


def test_naive_datetime_is_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        canonical_json(
            {
                "created_at": datetime(
                    2026,
                    1,
                    1,
                    12,
                    0,
                )
            }
        )


def test_gate_round_trip():
    gate = make_gate()

    payload = serialize_contract(gate)
    restored = deserialize_gate(payload)

    assert restored == gate


def test_trade_set_hash_is_deterministic():
    trades = [
        make_trade("trade-1"),
        make_trade("trade-2"),
    ]

    assert compute_trade_set_hash(trades) == compute_trade_set_hash(
        list(reversed(trades))
    )


def test_gate_audit_hash_is_deterministic():
    gate = make_gate()

    first = compute_gate_audit_hash(gate)
    second = compute_gate_audit_hash(gate)

    assert first == second
    assert len(first) == 64
