import pytest
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone

from core.contracts import (
    PredictionContract,
    DecisionContract,
    TradeContract,
    GateContract,
)


def test_prediction_contract_is_created():
    prediction = PredictionContract(
        prediction_hash="prediction-1",
        model_version="model-v1",
        feature_version="features-v1",
        config_hash="config-1",
        data_snapshot_hash="snapshot-1",
        raw_prediction=0.75,
        probability=0.80,
    )

    assert prediction.prediction_hash == "prediction-1"
    assert prediction.model_version == "model-v1"
    assert prediction.probability == 0.80


def test_decision_contract_is_created():
    decision = DecisionContract(
        decision_hash="decision-1",
        prediction_hash="prediction-1",
        direction="CALL",
        target_size=1.0,
        confidence=0.85,
    )

    assert decision.decision_hash == "decision-1"
    assert decision.prediction_hash == "prediction-1"
    assert decision.direction == "CALL"
    assert decision.confidence == 0.85


def test_trade_contract_is_created():
    trade = TradeContract(
        trade_id="trade-1",
        decision_hash="decision-1",
        direction="CALL",
        execution_price=1.12345,
        size=1.0,
        fee=0.01,
        pnl=None,
        slippage=0.0001,
        timestamp=1700000000,
    )

    assert trade.trade_id == "trade-1"
    assert trade.decision_hash == "decision-1"
    assert trade.direction == "CALL"
    assert trade.execution_price > 0


def test_gate_contract_is_created():
    created_at = datetime.now(timezone.utc)

    gate = GateContract(
        gate_run_id="gate-1",
        model_version="model-v1",
        feature_version="features-v1",
        config_hash="config-1",
        data_snapshot_hash="snapshot-1",
        gate_config_hash="gate-config-1",
        trade_set_hash="trade-set-1",
        status="PASS",
        fail_reason=None,
        created_at=created_at,
        audit_hash="audit-1",
    )

    assert gate.gate_run_id == "gate-1"
    assert gate.status == "PASS"
    assert gate.fail_reason is None
    assert gate.created_at.tzinfo is not None


@pytest.mark.parametrize(
    "factory",
    [
        lambda: PredictionContract(
            prediction_hash="p",
            model_version="m",
            feature_version="f",
            config_hash="c",
            data_snapshot_hash="d",
            raw_prediction=0.5,
            probability=0.5,
        ),
        lambda: DecisionContract(
            decision_hash="d",
            prediction_hash="p",
            direction="CALL",
            target_size=1.0,
            confidence=0.5,
        ),
        lambda: TradeContract(
            trade_id="t",
            decision_hash="d",
            direction="CALL",
            execution_price=1.0,
            size=1.0,
            fee=0.0,
            pnl=None,
            slippage=0.0,
            timestamp=1700000000,
        ),
        lambda: GateContract(
            gate_run_id="g",
            model_version="m",
            feature_version="f",
            config_hash="c",
            data_snapshot_hash="d",
            gate_config_hash="gc",
            trade_set_hash="ts",
            status="PASS",
            fail_reason=None,
            created_at=datetime.now(timezone.utc),
            audit_hash="a",
        ),
    ],
)
def test_contracts_are_immutable(factory):
    contract = factory()

    with pytest.raises(FrozenInstanceError):
        if isinstance(contract, PredictionContract):
            contract.probability = 0.99
        elif isinstance(contract, DecisionContract):
            contract.confidence = 0.99
        elif isinstance(contract, TradeContract):
            contract.size = 999.0
        elif isinstance(contract, GateContract):
            contract.status = "FAIL"
