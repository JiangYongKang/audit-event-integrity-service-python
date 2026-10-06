"""FastAPI 路由：摄取、保留、校验、规则与评估。"""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.models import IngestStatus, RejectReason
from app.retention import RetentionConfig
from app.rules import RuleDefinition, RuleError
from app.service import AuditService


class IngestRequest(BaseModel):
    events: list[dict[str, Any]]
    declared_source: str | None = None


class RetentionRequest(BaseModel):
    now_ms: int | None = None


def _jsonable(obj: Any) -> Any:
    if isinstance(obj, Enum):
        return obj.value
    if is_dataclass(obj) and not isinstance(obj, type):
        return {k: _jsonable(v) for k, v in asdict(obj).items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _jsonable(v) for k, v in obj.items()}
    return obj


def create_app(service: AuditService | None = None) -> FastAPI:
    svc = service or AuditService(
        retention=RetentionConfig(max_events_per_source=10_000))
    app = FastAPI(title="审计事件完整性服务")

    @app.post("/ingest")
    def ingest(req: IngestRequest) -> dict:
        results = svc.ingest_batch(req.events, req.declared_source)
        return {
            "accepted": sum(1 for r in results if r.status == IngestStatus.ACCEPTED),
            "duplicates": sum(1 for r in results if r.status == IngestStatus.DUPLICATE),
            "rejected": sum(1 for r in results if r.status == IngestStatus.REJECTED),
            "results": _jsonable(results),
        }

    @app.post("/repair")
    def repair(event: dict[str, Any], declared_source: str | None = None) -> dict:
        """修复重投：对曾被拒、且从未成功落库的事件标识投递修正后的内容。"""
        return _jsonable(svc.repair_ingest(event, declared_source))

    @app.get("/rejections")
    def rejections(source: str | None = None, reason: str | None = None,
                   event_id: str | None = None) -> dict:
        """查询被拒投递留档，支持按来源 / 原因 / 事件标识筛选。"""
        parsed_reason = None
        if reason is not None:
            try:
                parsed_reason = RejectReason(reason)
            except ValueError as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"未知拒绝原因: {reason}，可选: "
                           f"{[r.value for r in RejectReason]}") from exc
        records = svc.query_rejections(source=source, reason=parsed_reason,
                                       event_id=event_id)
        return {"total": len(records),
                "archive_size": svc.rejection_archive_size(),
                "records": _jsonable(records)}

    @app.post("/retention")
    def retention(req: RetentionRequest) -> dict:
        return {"evicted": svc.run_retention(req.now_ms)}

    @app.get("/verify")
    def verify_all() -> dict:
        return {"results": _jsonable(svc.verify_all())}

    @app.get("/verify/{source}")
    def verify_source(source: str) -> dict:
        return _jsonable(svc.verify_source(source))

    @app.get("/events/{event_id}")
    def get_event(event_id: str) -> dict:
        event = svc.get_event(event_id)
        if event is None:
            raise HTTPException(status_code=404, detail="事件不存在")
        return _jsonable(event)

    @app.put("/rules")
    def upsert_rule(definition: dict) -> dict:
        try:
            saved = svc.upsert_rule(RuleDefinition(**definition))
        except (RuleError, TypeError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return _jsonable(saved)

    @app.delete("/rules/{rule_id}")
    def delete_rule(rule_id: str) -> dict:
        try:
            svc.delete_rule(rule_id)
        except RuleError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"deleted": rule_id}

    @app.post("/evaluate")
    def evaluate(source: str | None = None,
                 event_ids: list[str] | None = None) -> dict:
        return _jsonable(svc.evaluate(source=source, event_ids=event_ids))

    return app
