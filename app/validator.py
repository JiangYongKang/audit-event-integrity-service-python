"""入站事件校验与规范化。

校验顺序固定，保证同一非法投递每次命中同一原因（可复现）：
1. 必填字段缺失/类型非法 -> MISSING_FIELD
2. 时间戳不可解析        -> MALFORMED_TIMESTAMP
3. 时间戳超出允许范围    -> TIMESTAMP_OUT_OF_RANGE
4. 来源与声明不符        -> SOURCE_MISMATCH
"""
from __future__ import annotations

from typing import Any

from app.models import AuditEvent, RejectReason

REQUIRED_FIELDS = ("event_id", "source", "sequence", "timestamp", "event_type")

# 允许的时间戳范围：[1970-01-01, 2100-01-01) 的 Unix epoch 毫秒
MAX_TIMESTAMP_MS = 4_102_444_800_000

# 校验失败的返回： (原因, 可解释细节)
Rejection = tuple[RejectReason, str]


class EventValidator:
    def __init__(self, store=None) -> None:
        # store 预留给需要上下文的校验（当前不需要），保持构造签名兼容
        self.store = store

    def normalize_and_validate(
        self, raw: dict, declared_source: str | None = None
    ) -> tuple[AuditEvent | None, Rejection | None]:
        if not isinstance(raw, dict):
            return None, (RejectReason.MISSING_FIELD,
                          f"投递内容必须是对象，实际为 {type(raw).__name__}")

        missing = [f for f in REQUIRED_FIELDS
                   if f not in raw or raw[f] is None]
        if missing:
            return None, (RejectReason.MISSING_FIELD,
                          f"必填字段缺失: {', '.join(missing)}")

        event_id = raw["event_id"]
        if not isinstance(event_id, str) or not event_id.strip():
            return None, (RejectReason.MISSING_FIELD,
                          "event_id 必须是非空字符串")

        source = raw["source"]
        if not isinstance(source, str) or not source.strip():
            return None, (RejectReason.MISSING_FIELD,
                          "source 必须是非空字符串")

        sequence = raw["sequence"]
        if isinstance(sequence, bool) or not isinstance(sequence, int):
            return None, (RejectReason.MISSING_FIELD,
                          f"sequence 必须是整数，实际为 {sequence!r}")

        event_type = raw["event_type"]
        if not isinstance(event_type, str) or not event_type.strip():
            return None, (RejectReason.MISSING_FIELD,
                          "event_type 必须是非空字符串")

        timestamp = raw["timestamp"]
        if isinstance(timestamp, bool) or not isinstance(timestamp, int):
            return None, (RejectReason.MALFORMED_TIMESTAMP,
                          f"timestamp 必须是 Unix epoch 毫秒整数，实际为 {timestamp!r}")
        if timestamp < 0 or timestamp >= MAX_TIMESTAMP_MS:
            return None, (RejectReason.TIMESTAMP_OUT_OF_RANGE,
                          f"timestamp={timestamp} 超出允许范围 "
                          f"[0, {MAX_TIMESTAMP_MS})")

        if declared_source is not None and declared_source != source:
            return None, (RejectReason.SOURCE_MISMATCH,
                          f"声明来源 {declared_source} 与事件来源 {source} 不符")

        payload: Any = raw.get("payload", {})
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            return None, (RejectReason.MISSING_FIELD,
                          f"payload 必须是对象，实际为 {type(payload).__name__}")

        return AuditEvent(
            event_id=event_id,
            source=source,
            sequence=sequence,
            timestamp=timestamp,
            event_type=event_type,
            payload=payload,
        ), None
