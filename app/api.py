"""FastAPI 路由：摄取、修复重投、留档查询、保留、校验、规则评估。"""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any

from fastapi import FastAPI, HTTPException, Query

from app.errors import RepairNotAllowed
from app.models import RejectReason
from app.retention import RetentionConfig
from app.rules import RuleDefinition, RuleError
from app.service import AuditService


def _jsonable(obj: Any) -> Any:
    if is_dataclass(obj) and not isinstance(obj, type):
        return {k: _jsonable(v) for k, v in asdict(obj).items()}
    if isinstance(obj, list):
        return [_jsonable(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _jsonable(v) for k, v in obj.items()}
    return obj


def create_app(service: AuditService | None = None) -> FastAPI:
    svc = service or AuditService(
        retention=RetentionConfig(max_events_per_source=1000))
    app = FastAPI(title="audit-event-integrity-service")

    @app.post("/events:ingest")
    def ingest(events: list[dict],
               declared_source: str | None = Query(default=None)):
        return _jsonable(svc.ingest_batch(events, declared_source))

    @app.post("/sources/{source}/events/{event_id}:resubmit")
    def resubmit(source: str, event_id: str, raw: dict):
        try:
            return _jsonable(svc.resubmit(source, event_id, raw))
        except RepairNotAllowed as exc:
            raise HTTPException(status_code=409, detail=exc.detail) from exc

    @app.get("/rejections")
    def list_rejections(source: str | None = Query(default=None),
                        reason: RejectReason | None = Query(default=None),  # noqa: B008 FastAPI 依赖注入惯例
                        event_id: str | None = Query(default=None)):
        return _jsonable(svc.list_rejections(
            source=source, reason=reason, event_id=event_id))

    @app.get("/rejections/{source}/{event_id}")
    def get_rejection(source: str, event_id: str):
        record = svc.get_rejection(source, event_id)
        if record is None:
            raise HTTPException(status_code=404, detail="留档不存在")
        return _jsonable(record)

    @app.post("/retention:run")
    def run_retention(now_ms: int | None = Query(default=None)):
        return svc.run_retention(now_ms)

    @app.get("/verify")
    def verify_all():
        return _jsonable(svc.verify_all())

    @app.get("/verify/{source}")
    def verify_source(source: str):
        return _jsonable(svc.verify_source(source))

    @app.post("/rules")
    def upsert_rule(definition: dict):
        try:
            stored = svc.upsert_rule(RuleDefinition(**definition))
        except (RuleError, TypeError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return _jsonable(stored)

    @app.delete("/rules/{rule_id}")
    def delete_rule(rule_id: str):
        svc.delete_rule(rule_id)
        return {"deleted": rule_id}

    @app.post("/evaluate")
    def evaluate(source: str | None = Query(default=None),
                 event_ids: list[str] | None = None):
        return _jsonable(svc.evaluate(source=source, event_ids=event_ids))

    @app.get("/events/{event_id}")
    def get_event(event_id: str):
        event = svc.get_event(event_id)
        if event is None:
            raise HTTPException(status_code=404, detail="事件不存在")
        return _jsonable(event)

    return app
