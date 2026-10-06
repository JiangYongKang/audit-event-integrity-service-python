"""被拒投递留档：事后可查、按来源/原因/事件标识筛选、容量背压。

语义：
- 以 (source, event_id) 为键去重：同一份内容被反复投递只更新
  attempts/last_seen_ms 与最近一次的原因/原始内容，不产生新条目；
- 容量写满时，对新键抛出 ArchiveCapacityExceeded（背压），
  不静默丢弃、不淘汰旧留档；已有键的重复投递不受容量限制；
- 修复成功后调用 mark_resolved 留痕，记录本身保留可查；
- 留档是链外数据：不进入完整性链，也不影响校验结论。
"""
from __future__ import annotations

import threading
from dataclasses import replace

from app.errors import ArchiveCapacityExceeded
from app.models import RejectionRecord, RejectReason

DEFAULT_ARCHIVE_CAPACITY = 10_000


class RejectionArchive:
    def __init__(self, max_records: int = DEFAULT_ARCHIVE_CAPACITY) -> None:
        if max_records <= 0:
            raise ValueError("max_records 必须为正整数")
        self.max_records = max_records
        self._records: dict[tuple[str, str], RejectionRecord] = {}
        self._lock = threading.Lock()

    def record(self, source: str, event_id: str, reason: RejectReason,
               detail: str | None, raw_payload: dict, now_ms: int) -> RejectionRecord:
        """记录一次被拒投递；同键去重合并，新键超容量时抛背压异常。"""
        key = (source, event_id)
        with self._lock:
            existing = self._records.get(key)
            if existing is not None:
                updated = replace(
                    existing,
                    reason=reason,
                    detail=detail,
                    raw_payload=dict(raw_payload),
                    last_seen_ms=now_ms,
                    attempts=existing.attempts + 1,
                )
                self._records[key] = updated
                return updated
            if len(self._records) >= self.max_records:
                raise ArchiveCapacityExceeded(source, event_id, self.max_records)
            record = RejectionRecord(
                source=source,
                event_id=event_id,
                reason=reason,
                detail=detail,
                raw_payload=dict(raw_payload),
                first_seen_ms=now_ms,
                last_seen_ms=now_ms,
            )
            self._records[key] = record
            return record

    def mark_resolved(self, source: str, event_id: str, now_ms: int) -> None:
        """修复重投成功后留痕；记录保留，原因与原始内容仍可查。"""
        key = (source, event_id)
        with self._lock:
            existing = self._records.get(key)
            if existing is not None and not existing.resolved:
                self._records[key] = replace(
                    existing, resolved=True, resolved_at_ms=now_ms)

    def get(self, source: str, event_id: str) -> RejectionRecord | None:
        return self._records.get((source, event_id))

    def query(self, source: str | None = None,
              reason: RejectReason | None = None,
              event_id: str | None = None) -> list[RejectionRecord]:
        """按来源 / 原因 / 事件标识筛选（条件可组合，None 表示不过滤）。"""
        out = []
        for rec in self._records.values():
            if source is not None and rec.source != source:
                continue
            if reason is not None and rec.reason != reason:
                continue
            if event_id is not None and rec.event_id != event_id:
                continue
            out.append(rec)
        return sorted(out, key=lambda r: (r.first_seen_ms, r.source, r.event_id))

    def __len__(self) -> int:
        return len(self._records)
