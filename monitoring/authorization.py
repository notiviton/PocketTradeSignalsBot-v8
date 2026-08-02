from dataclasses import dataclass
from typing import Optional

from storage.database import ImmutableAuditStorage


@dataclass(frozen=True)
class LiveAuthorizationRequest:
    model_version: str
    feature_version: str
    config_hash: str
    data_snapshot_hash: str
    gate_config_hash: str
    trade_set_hash: str
    gate_run_id: str


@dataclass(frozen=True)
class LiveAuthorizationResult:
    authorized: bool
    gate_run_id: str
    reason: Optional[str] = None


class LiveAuthorizationService:
    def __init__(self, storage: ImmutableAuditStorage):
        self.storage = storage

    def authorize(
        self,
        request: LiveAuthorizationRequest,
    ) -> LiveAuthorizationResult:
        gate = self.storage.get_gate(request.gate_run_id)

        if gate is None:
            if self.storage.gate_exists(request.gate_run_id):
                return LiveAuthorizationResult(
                    False,
                    request.gate_run_id,
                    "GATE_AUDIT_INTEGRITY_FAILURE",
                )

            return LiveAuthorizationResult(
                False,
                request.gate_run_id,
                "GATE_NOT_FOUND",
            )

        if not self.storage.verify_gate_integrity(gate):
            return LiveAuthorizationResult(
                False,
                gate.gate_run_id,
                "GATE_AUDIT_INTEGRITY_FAILURE",
            )

        if gate.status != "PASS":
            return LiveAuthorizationResult(
                False,
                gate.gate_run_id,
                f"GATE_STATUS_NOT_PASS_{gate.status}",
            )

        if (
            gate.gate_run_id != request.gate_run_id
            or gate.model_version != request.model_version
            or gate.feature_version != request.feature_version
            or gate.config_hash != request.config_hash
            or gate.data_snapshot_hash != request.data_snapshot_hash
            or gate.gate_config_hash != request.gate_config_hash
            or gate.trade_set_hash != request.trade_set_hash
        ):
            return LiveAuthorizationResult(
                False,
                gate.gate_run_id,
                "GATE_CONTEXT_MISMATCH",
            )

        return LiveAuthorizationResult(
            True,
            gate.gate_run_id,
        )
