from datetime import datetime, timezone

from core.contracts import (
    DecisionContract,
    GateContract,
    PredictionContract,
    TradeContract,
)
from monitoring.authorization import (
    LiveAuthorizationRequest,
    LiveAuthorizationService,
)
from storage.database import ImmutableAuditStorage
from storage.serializers import compute_trade_set_hash


def setup_storage(gate_status="PASS"):
    storage = ImmutableAuditStorage()

    prediction = PredictionContract(
        prediction_hash="prediction-1",
        model_version="model-v1",
        feature_version="features-v1",
        config_hash="config-1",
        data_snapshot_hash="snapshot-1",
        raw_prediction=0.8,
        probability=0.8,
    )

    decision = DecisionContract(
        decision_hash="decision-1",
        prediction_hash="prediction-1",
        direction="CALL",
        target_size=1.0,
        confidence=0.8,
    )

    trade = TradeContract(
        trade_id="trade-1",
        decision_hash="decision-1",
        direction="CALL",
        execution_price=100.0,
        size=1.0,
        fee=0.1,
        pnl=None,
        slippage=0.01,
        timestamp=1700000000,
    )

    storage.save_prediction(prediction)
    storage.save_decision(decision)
    storage.save_trade(trade)

    trade_set_hash = compute_trade_set_hash([trade])

    gate = GateContract(
        gate_run_id="gate-1",
        model_version="model-v1",
        feature_version="features-v1",
        config_hash="config-1",
        data_snapshot_hash="snapshot-1",
        gate_config_hash="gate-config-1",
        trade_set_hash=trade_set_hash,
        status=gate_status,
        fail_reason=None if gate_status == "PASS" else "TEST_FAILURE",
        created_at=datetime.now(timezone.utc),
        audit_hash="audit-placeholder",
    )

    storage.save_gate(gate, [trade])

    request = LiveAuthorizationRequest(
        model_version="model-v1",
        feature_version="features-v1",
        config_hash="config-1",
        data_snapshot_hash="snapshot-1",
        gate_config_hash="gate-config-1",
        trade_set_hash=trade_set_hash,
        gate_run_id="gate-1",
    )

    return storage, request


def test_authorization_allows_valid_gate():
    storage, request = setup_storage("PASS")

    try:
        service = LiveAuthorizationService(storage)
        result = service.authorize(request)

        assert result.authorized is True
        assert result.reason is None
        assert result.gate_run_id == "gate-1"

    finally:
        storage.close()


def test_authorization_rejects_missing_gate():
    storage, request = setup_storage("PASS")

    try:
        request = LiveAuthorizationRequest(
            model_version=request.model_version,
            feature_version=request.feature_version,
            config_hash=request.config_hash,
            data_snapshot_hash=request.data_snapshot_hash,
            gate_config_hash=request.gate_config_hash,
            trade_set_hash=request.trade_set_hash,
            gate_run_id="missing-gate",
        )

        service = LiveAuthorizationService(storage)
        result = service.authorize(request)

        assert result.authorized is False
        assert result.reason == "GATE_NOT_FOUND"

    finally:
        storage.close()


def test_authorization_rejects_non_pass_gate():
    storage, request = setup_storage("FAIL")

    try:
        service = LiveAuthorizationService(storage)
        result = service.authorize(request)

        assert result.authorized is False
        assert result.reason == "GATE_STATUS_NOT_PASS_FAIL"

    finally:
        storage.close()


def test_authorization_rejects_context_mismatch():
    storage, request = setup_storage("PASS")

    try:
        request = LiveAuthorizationRequest(
            model_version="wrong-model",
            feature_version=request.feature_version,
            config_hash=request.config_hash,
            data_snapshot_hash=request.data_snapshot_hash,
            gate_config_hash=request.gate_config_hash,
            trade_set_hash=request.trade_set_hash,
            gate_run_id=request.gate_run_id,
        )

        service = LiveAuthorizationService(storage)
        result = service.authorize(request)

        assert result.authorized is False
        assert result.reason == "GATE_CONTEXT_MISMATCH"

    finally:
        storage.close()


def test_authorization_rejects_tampered_gate():
    storage, request = setup_storage("PASS")

    try:
        storage.conn.execute(
            """
            UPDATE gates
            SET payload = ?
            WHERE gate_run_id = ?
            """,
            ('{"tampered":true}', "gate-1"),
        )
        storage.conn.commit()

        service = LiveAuthorizationService(storage)
        result = service.authorize(request)

        assert result.authorized is False
        assert result.reason == "GATE_AUDIT_INTEGRITY_FAILURE"

    finally:
        storage.close()
