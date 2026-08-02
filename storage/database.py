import sqlite3
from typing import List, Optional

from core.contracts import (
    GateContract,
    TradeContract,
    PredictionContract,
    DecisionContract,
)
from storage.serializers import (
    serialize_contract,
    deserialize_gate,
    compute_gate_audit_hash,
    compute_trade_set_hash,
)


class AuditIntegrityError(Exception):
    pass


class ImmutableAuditStorage:
    def __init__(self, db_path: str = ":memory:"):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._init_schema()

    def _init_schema(self):
        with self.conn:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS predictions (
                    prediction_hash TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS decisions (
                    decision_hash TEXT PRIMARY KEY,
                    prediction_hash TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    FOREIGN KEY (prediction_hash)
                        REFERENCES predictions(prediction_hash)
                );

                CREATE TABLE IF NOT EXISTS trades (
                    trade_id TEXT PRIMARY KEY,
                    decision_hash TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    FOREIGN KEY (decision_hash)
                        REFERENCES decisions(decision_hash)
                );

                CREATE TABLE IF NOT EXISTS gates (
                    gate_run_id TEXT PRIMARY KEY,
                    model_version TEXT NOT NULL,
                    feature_version TEXT NOT NULL,
                    config_hash TEXT NOT NULL,
                    data_snapshot_hash TEXT NOT NULL,
                    gate_config_hash TEXT NOT NULL,
                    trade_set_hash TEXT NOT NULL,
                    status TEXT NOT NULL,
                    fail_reason TEXT,
                    audit_hash TEXT NOT NULL,
                    payload TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS gate_trades (
                    gate_run_id TEXT NOT NULL,
                    trade_id TEXT NOT NULL,
                    PRIMARY KEY (gate_run_id, trade_id),
                    FOREIGN KEY (gate_run_id)
                        REFERENCES gates(gate_run_id),
                    FOREIGN KEY (trade_id)
                        REFERENCES trades(trade_id)
                );
                """
            )

    def save_prediction(self, prediction: PredictionContract):
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO predictions VALUES (?, ?)",
                    (
                        prediction.prediction_hash,
                        serialize_contract(prediction),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise AuditIntegrityError(
                f"Prediction insertion failed: {prediction.prediction_hash}"
            ) from exc

    def save_decision(self, decision: DecisionContract):
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO decisions VALUES (?, ?, ?)",
                    (
                        decision.decision_hash,
                        decision.prediction_hash,
                        serialize_contract(decision),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise AuditIntegrityError(
                f"Decision insertion failed: {decision.decision_hash}"
            ) from exc

    def save_trade(self, trade: TradeContract):
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO trades VALUES (?, ?, ?)",
                    (
                        trade.trade_id,
                        trade.decision_hash,
                        serialize_contract(trade),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise AuditIntegrityError(
                f"Trade insertion failed: {trade.trade_id}"
            ) from exc

    def save_gate(
        self,
        gate: GateContract,
        trades: List[TradeContract],
    ):
        if not trades:
            raise AuditIntegrityError(
                "Cannot save gate without trades"
            )

        expected_hash = compute_trade_set_hash(trades)

        if expected_hash != gate.trade_set_hash:
            raise AuditIntegrityError(
                "Trade set hash mismatch"
            )

        for trade in trades:
            row = self.conn.execute(
                """
                SELECT payload
                FROM trades
                WHERE trade_id = ?
                """,
                (trade.trade_id,),
            ).fetchone()

            if row is None:
                raise AuditIntegrityError(
                    f"Trade not persisted: {trade.trade_id}"
                )

            if row[0] != serialize_contract(trade):
                raise AuditIntegrityError(
                    f"Trade payload mismatch: {trade.trade_id}"
                )

        audit_hash = compute_gate_audit_hash(gate)
        payload = serialize_contract(gate)

        try:
            with self.conn:
                self.conn.execute(
                    """
                    INSERT INTO gates (
                        gate_run_id,
                        model_version,
                        feature_version,
                        config_hash,
                        data_snapshot_hash,
                        gate_config_hash,
                        trade_set_hash,
                        status,
                        fail_reason,
                        audit_hash,
                        payload
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        gate.gate_run_id,
                        gate.model_version,
                        gate.feature_version,
                        gate.config_hash,
                        gate.data_snapshot_hash,
                        gate.gate_config_hash,
                        gate.trade_set_hash,
                        gate.status,
                        gate.fail_reason,
                        audit_hash,
                        payload,
                    ),
                )

                for trade in trades:
                    self.conn.execute(
                        """
                        INSERT INTO gate_trades
                        VALUES (?, ?)
                        """,
                        (
                            gate.gate_run_id,
                            trade.trade_id,
                        ),
                    )

        except sqlite3.IntegrityError as exc:
            raise AuditIntegrityError(
                f"Gate insertion failed: {gate.gate_run_id}"
            ) from exc

    def gate_exists(self, gate_run_id: str) -> bool:
        row = self.conn.execute(
            """
            SELECT 1
            FROM gates
            WHERE gate_run_id = ?
            """,
            (gate_run_id,),
        ).fetchone()

        return row is not None

    def get_gate(
        self,
        gate_run_id: str,
    ) -> Optional[GateContract]:
        cursor = self.conn.execute(
            """
            SELECT payload
            FROM gates
            WHERE gate_run_id = ?
            """,
            (gate_run_id,),
        )

        row = cursor.fetchone()

        if row is None:
            return None

        try:
            return deserialize_gate(row[0])
        except (ValueError, KeyError, TypeError):
            return None

    def verify_gate_integrity(
        self,
        gate: GateContract,
    ) -> bool:
        row = self.conn.execute(
            """
            SELECT audit_hash, payload
            FROM gates
            WHERE gate_run_id = ?
            """,
            (gate.gate_run_id,),
        ).fetchone()

        if row is None:
            return False

        stored_audit_hash, stored_payload = row

        if stored_payload != serialize_contract(gate):
            return False

        return compute_gate_audit_hash(gate) == stored_audit_hash

    def close(self):
        self.conn.close()
