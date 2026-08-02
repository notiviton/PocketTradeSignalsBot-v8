from datetime import datetime, timezone

import pytest

from core.contracts import (
    DecisionContract,
    GateContract,
    PredictionContract,
    TradeContract,
)
from storage.database import AuditIntegrityError, ImmutableAuditStorage
from storage.serializers import compute_trade_set_hash


def make_prediction():
    return PredictionContract(
        prediction_hash="prediction-1",
        model_version="model-v1",
        feature_version="features-v1",
        config_hash="config-1",
        data_snapshot_hash="snapshot-1",
        raw_prediction=0.8,
        probability=0.8,
    )


def make_decision():
    return DecisionContract(
        decision_hash="decision-1",
        prediction_hash="prediction-1",
        direction="CALL",
        target_size=1.0,
        confidence=0.8,
    )


def make_trade(trade_id="trade-1"):
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


def make_gate(trade_set_hash):
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
        created_at=datetime.now(timezone.utc),
        audit_hash="audit-placeholder",
    )


def test_database_saves_and_reads_gate():
    storage = ImmutableAuditStorage()

    try:
        prediction = make_prediction()
        decision = make_decision()
        trade = make_trade()

        storage.save_prediction(prediction)
        storage.save_decision(decision)
        storage.save_trade(trade)

        trade_set_hash = compute_trade_set_hash([trade])
        gate = make_gate(trade_set_hash)

        storage.save_gate(gate, [trade])

        loaded = storage.get_gate("gate-1")

        assert loaded is not None
        assert loaded.gate_run_id == "gate-1"
        assert loaded.trade_set_hash == trade_set_hash
        assert storage.verify_gate_integrity(loaded) is True

    finally:
        storage.close()


def test_database_rejects_duplicate_prediction():
    storage = ImmutableAuditStorage()

    try:
        prediction = make_prediction()

        storage.save_prediction(prediction)

        with pytest.raises(AuditIntegrityError):
            storage.save_prediction(prediction)

    finally:
        storage.close()


def test_database_rejects_gate_with_wrong_trade_set_hash():
    storage = ImmutableAuditStorage()

    try:
        prediction = make_prediction()
        decision = make_decision()
        trade = make_trade()

        storage.save_prediction(prediction)
        storage.save_decision(decision)
        storage.save_trade(trade)

        gate = make_gate("incorrect-hash")

        with pytest.raises(AuditIntegrityError, match="Trade set hash mismatch"):
            storage.save_gate(gate, [trade])

    finally:
        storage.close()


def test_database_rejects_trade_without_decision():
    storage = ImmutableAuditStorage()

    try:
        trade = make_trade()

        with pytest.raises(AuditIntegrityError):
            storage.save_trade(trade)

    finally:
        storage.close()
