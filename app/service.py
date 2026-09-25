"""应用服务门面：摄取、保留、校验、评估（骨架）。"""
from __future__ import annotations

from app.models import (
    EvaluationReport,
    IngestResult,
    VerificationResult,
)
from app.retention import RetentionConfig
from app.rules import RuleDefinition
from app.store import EventStore


class AuditService:
    def __init__(self, retention: RetentionConfig | None = None) -> None:
        raise NotImplementedError

    def ingest_batch(
        self, events: list[dict], declared_source: str | None = None
    ) -> list[IngestResult]:
        raise NotImplementedError

    def run_retention(self, now_ms: int | None = None) -> dict[str, int]:
        raise NotImplementedError

    def verify_source(self, source: str) -> VerificationResult:
        raise NotImplementedError

    def verify_all(self) -> list[VerificationResult]:
        raise NotImplementedError

    def evaluate(self, source: str | None = None,
                 event_ids: list[str] | None = None) -> EvaluationReport:
        raise NotImplementedError

    def upsert_rule(self, definition: RuleDefinition) -> RuleDefinition:
        raise NotImplementedError

    def delete_rule(self, rule_id: str) -> None:
        raise NotImplementedError

    def get_event(self, event_id: str):
        raise NotImplementedError
