"""测试事件构造工具。"""
from __future__ import annotations

from audit_service.models import EventInput


def make_event(
    event_id: str,
    source: str = "src-billing",
    event_type: str = "payment",
    timestamp: str = "2026-09-25T10:00:00+00:00",
    payload=None,
    sequence: int | None = None,
    declared_source: str | None = None,
) -> EventInput:
    return EventInput(
        event_id=event_id,
        declared_source=declared_source if declared_source is not None else source,
        source=source,
        event_type=event_type,
        timestamp=timestamp,
        payload=payload if payload is not None else {"amount": 100},
        sequence=sequence,
    )
