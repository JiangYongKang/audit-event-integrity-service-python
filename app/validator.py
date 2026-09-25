"""入站事件校验与规范化（骨架）。"""
from __future__ import annotations

from app.models import AuditEvent
from app.store import EventStore


class EventValidator:
    def __init__(self, store: EventStore) -> None:
        self.store = store

    def normalize_and_validate(
        self, raw: dict, declared_source: str | None = None
    ) -> tuple[AuditEvent | None, object | None]:
        raise NotImplementedError
