"""被拒投递留档：去重、容量上限、按条件筛选。

设计要点：
- 去重键 = (event_id, source, content_hash)：同一份内容反复投递只累计
  attempts / last_seen_ms，不产生新条目；
- 容量写满时抛 ArchiveFullError（可区分背压），绝不静默丢弃，也绝不
  挤掉老留档；
- 留档与事件主存储/完整性链完全隔离：不写链、不影响校验结论；
- 修复成功后仅把对应留档标记 resolved=True（留痕），不删除。
"""
from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import replace
from typing import Any

from app.errors import ArchiveFullError
from app.models import RejectionRecord, RejectReason

DEFAULT_MAX_RECORDS = 1_000


def content_hash(raw: Any) -> str:
    """原始投递内容的确定性哈希（作为去重键的一部分）。"""
    body = json.dumps(raw, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, default=str).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


class RejectionArchive:
    def __init__(self, max_records: int = DEFAULT_MAX_RECORDS) -> None:
        if max_records <= 0:
            raise ValueError("max_records 必须为正整数")
        self._max = max_records
        self._records: dict[tuple[str | None, str | None, str], RejectionRecord] = {}
        self._lock = threading.Lock()

    def record(self, event_id: str | None, source: str | None,
               reason: RejectReason, detail: str, raw: dict,
               channel: str, now_ms: int) -> RejectionRecord:
        """写入一条留档；同去重键重复投递时合并（attempts+1）。

        新增条目且容量已满时抛 ArchiveFullError（背压）。
        """
        key = (event_id, source, content_hash(raw))
        with self._lock:
            existing = self._records.get(key)
            if existing is not None:
                updated = replace(
                    existing,
                    attempts=existing.attempts + 1,
                    last_seen_ms=now_ms,
                    detail=detail,  # 细节可能随轮次变化，保留最新
                )
                self._records[key] = updated
                return updated
            if len(self._records) >= self._max:
                raise ArchiveFullError(
                    f"留档容量已满（{self._max} 条），拒绝写入新留档；"
                    "未静默丢弃、未淘汰老留档，请扩容或处理后重试")
            record = RejectionRecord(
                event_id=event_id, source=source, reason=reason,
                detail=detail, raw=raw, content_hash=key[2],
                channel=channel, first_seen_ms=now_ms,
                last_seen_ms=now_ms)
            self._records[key] = record
            return record

    def query(self, source: str | None = None,
              reason: RejectReason | None = None,
              event_id: str | None = None) -> list[RejectionRecord]:
        """按来源 / 原因 / 事件标识筛选（条件为与关系）。"""
        with self._lock:
            records = list(self._records.values())
        if source is not None:
            records = [r for r in records if r.source == source]
        if reason is not None:
            records = [r for r in records if r.reason == reason]
        if event_id is not None:
            records = [r for r in records if r.event_id == event_id]
        return sorted(records, key=lambda r: (r.first_seen_ms, r.event_id or ""))

    def has_rejection_for(self, event_id: str) -> bool:
        with self._lock:
            return any(r.event_id == event_id for r in self._records.values())

    def mark_resolved(self, event_id: str) -> int:
        """事件标识修复成功后，把其全部留档标记为 resolved（留痕不删）。"""
        count = 0
        with self._lock:
            for key, record in self._records.items():
                if record.event_id == event_id and not record.resolved:
                    self._records[key] = replace(record, resolved=True)
                    count += 1
        return count

    def __len__(self) -> int:
        with self._lock:
            return len(self._records)
