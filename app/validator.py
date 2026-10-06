"""入站事件校验与规范化。

只做"单条事件自身"的校验（字段、时间戳、来源一致性），
不触碰幂等/序号/容量等需要存储状态的全局判定（在 AuditService 中）。
"""
from __future__ import annotations

from typing import Any

from app.errors import IngestRejection
from app.models import AuditEvent, RejectReason
from app.store import EventStore

# 时间戳合法范围：[2000-01-01, 2100-01-01) 的 epoch 毫秒
MIN_TIMESTAMP_MS = 946684800000
MAX_TIMESTAMP_MS = 4102444800000

_REQUIRED_FIELDS = ("event_id", "source", "sequence", "timestamp", "event_type")


class EventValidator:
    def __init__(self, store: EventStore) -> None:
        self.store = store

    def normalize_and_validate(
        self, raw: dict, declared_source: str | None = None
    ) -> tuple[AuditEvent | None, IngestRejection | None]:
        """返回 (规范化事件, None) 或 (None, 拒绝原因)。"""
        if not isinstance(raw, dict):
            return None, IngestRejection(
                RejectReason.MISSING_FIELD, "投递内容不是对象")

        for name in _REQUIRED_FIELDS:
            if raw.get(name) is None:
                return None, IngestRejection(
                    RejectReason.MISSING_FIELD, f"必填字段缺失: {name}")

        event_id = raw["event_id"]
        source = raw["source"]
        event_type = raw["event_type"]
        if not isinstance(event_id, str) or not event_id:
            return None, IngestRejection(
                RejectReason.MISSING_FIELD, "event_id 必须为非空字符串")
        if not isinstance(source, str) or not source:
            return None, IngestRejection(
                RejectReason.MISSING_FIELD, "source 必须为非空字符串")
        if not isinstance(event_type, str) or not event_type:
            return None, IngestRejection(
                RejectReason.MISSING_FIELD, "event_type 必须为非空字符串")

        sequence = raw["sequence"]
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            return None, IngestRejection(
                RejectReason.MISSING_FIELD, "sequence 必须为非负整数")

        timestamp = raw["timestamp"]
        if isinstance(timestamp, bool) or not isinstance(timestamp, int):
            return None, IngestRejection(
                RejectReason.MALFORMED_TIMESTAMP,
                f"timestamp 必须是 epoch 毫秒整数，实际={timestamp!r}")
        if not (MIN_TIMESTAMP_MS <= timestamp < MAX_TIMESTAMP_MS):
            return None, IngestRejection(
                RejectReason.TIMESTAMP_OUT_OF_RANGE,
                f"timestamp={timestamp} 超出合法范围 "
                f"[{MIN_TIMESTAMP_MS}, {MAX_TIMESTAMP_MS})")

        if declared_source is not None and declared_source != source:
            return None, IngestRejection(
                RejectReason.SOURCE_MISMATCH,
                f"声明来源={declared_source} 与事件来源={source} 不一致")

        payload: dict[str, Any] = raw.get("payload") or {}
        if not isinstance(payload, dict):
            return None, IngestRejection(
                RejectReason.MISSING_FIELD, "payload 必须是对象")

        return AuditEvent(
            event_id=event_id,
            source=source,
            sequence=sequence,
            timestamp=timestamp,
            event_type=event_type,
            payload=payload,
        ), None
