"""FastAPI HTTP 层：批量摄取 / 完整性校验 / 保留巡检 / 规则管理 / 合规评估。"""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .errors import RuleNotFoundError
from .models import EventInput
from .service import AuditService


class EventBody(BaseModel):
    event_id: str
    source: str
    event_type: str
    timestamp: str
    payload: dict[str, Any] = Field(default_factory=dict)
    sequence: int | None = None


class IngestRequest(BaseModel):
    source: str = Field(description="投递通道声明的来源（信封），须与每条事件内嵌来源一致")
    events: list[EventBody]


class RuleBody(BaseModel):
    rule_type: str
    params: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True


class EvaluateRequest(BaseModel):
    events: list[EventBody]
    source: str | None = Field(default=None, description="评估用声明来源；缺省取事件体内嵌来源")
    rule_versions: dict[str, int] | None = None


def create_app(service: AuditService | None = None) -> FastAPI:
    app = FastAPI(
        title="Audit Event Integrity Service",
        version="0.1.0",
        description="多来源审计事件摄取、完整性哈希链校验、保留策略与可配置合规规则评估。",
    )
    service = service or AuditService()

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/ingest")
    def ingest(req: IngestRequest) -> dict[str, Any]:
        inputs = [
            EventInput(
                event_id=e.event_id,
                declared_source=req.source,
                source=e.source,
                event_type=e.event_type,
                timestamp=e.timestamp,
                payload=e.payload,
                sequence=e.sequence,
            )
            for e in req.events
        ]
        results = service.ingest_batch(inputs)
        accepted = sum(1 for r in results if r.accepted)
        return {
            "received": len(results),
            "accepted": accepted,
            "rejected": len(results) - accepted,
            "results": [
                {
                    "event_id": r.event_id,
                    "accepted": r.accepted,
                    "duplicate": r.duplicate,
                    "sequence": r.sequence,
                    "reason": r.reason.value if r.reason else None,
                    "detail": r.detail,
                    "record_hash": r.record_hash,
                }
                for r in results
            ],
        }

    @app.post("/retention/run")
    def run_retention() -> dict[str, Any]:
        return {"evicted_by_source": service.apply_retention()}

    @app.get("/verify")
    def verify() -> dict[str, Any]:
        results = service.verify_all()
        return {
            "all_ok": all(r.ok for r in results),
            "results": [
                {
                    "source": r.source,
                    "ok": r.ok,
                    "checked": r.checked,
                    "first_sequence": r.first_sequence,
                    "last_sequence": r.last_sequence,
                    "evicted_count": r.evicted_count,
                    "problem": r.problem,
                    "problem_event_id": r.problem_event_id,
                }
                for r in results
            ],
        }

    @app.get("/rules")
    def list_rules() -> dict[str, Any]:
        return {"rules": [
            {
                "rule_id": s.rule_id,
                "rule_type": s.rule_type,
                "params": s.params,
                "version": s.version,
                "enabled": s.enabled,
            }
            for s in service.registry.active_rules()
        ]}

    @app.put("/rules/{rule_id}")
    def upsert_rule(rule_id: str, body: RuleBody) -> dict[str, Any]:
        try:
            spec = service.upsert_rule(rule_id, body.rule_type, body.params, body.enabled)
        except RuleNotFoundError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {
            "rule_id": spec.rule_id,
            "version": spec.version,
            "enabled": spec.enabled,
        }

    @app.delete("/rules/{rule_id}")
    def delete_rule(rule_id: str) -> dict[str, str]:
        try:
            service.delete_rule(rule_id)
        except RuleNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"rule_id": rule_id, "status": "deleted (tombstone version retained)"}

    @app.post("/evaluate")
    def evaluate(req: EvaluateRequest) -> dict[str, Any]:
        inputs = [
            EventInput(
                event_id=e.event_id,
                declared_source=req.source or e.source,
                source=e.source,
                event_type=e.event_type,
                timestamp=e.timestamp,
                payload=e.payload,
                sequence=e.sequence,
            )
            for e in req.events
        ]
        try:
            evaluated = service.evaluate(inputs, rule_versions=req.rule_versions)
        except RuleNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {
            "results": [
                [
                    {
                        "rule_id": r.rule_id,
                        "rule_version": r.rule_version,
                        "event_id": r.event_id,
                        "matched": r.matched,
                        "status": r.status,
                        "rationale": r.rationale,
                        "error": r.error,
                    }
                    for r in per_event
                ]
                for per_event in evaluated
            ]
        }

    return app
