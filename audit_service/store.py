"""按来源的事件存储：幂等索引、并发控制、单调序号、保留淘汰。"""
from __future__ import annotations

import threading
from datetime import datetime

from .chain import GENESIS_HASH, IntegrityChain, compute_record_hash
from .models import (
    EventInput,
    IngestionResult,
    RejectReason,
    RetentionPolicy,
    StoredEvent,
    VerifyResult,
)
from .retention import select_evicted

DEFAULT_RETENTION = RetentionPolicy()

_REQUIRED_FIELDS = ("event_id", "declared_source", "source", "event_type", "timestamp")


def _parse_timestamp(raw: str) -> datetime:
    """解析 ISO-8601 时间戳；naive 时间戳按 UTC 解释。非法时抛 ValueError。"""
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("timestamp is empty")
    text = raw.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        from datetime import timezone

        dt = dt.replace(tzinfo=timezone.utc)
    return dt


class SourceStore:
    """单来源存储：序列严格单调、哈希链、event_id 幂等、时间淘汰与容量背压。"""

    def __init__(self, source: str, retention: RetentionPolicy | None = None) -> None:
        self.source = source
        self.retention = retention or DEFAULT_RETENTION
        self._lock = threading.RLock()
        self._chain = IntegrityChain(source)
        self._index: dict[str, StoredEvent] = {}
        self._next_sequence = 1

    # ---- 公共读取 ----
    def verify(self) -> VerifyResult:
        with self._lock:
            return self._chain.verify()

    def get(self, event_id: str) -> StoredEvent | None:
        with self._lock:
            return self._index.get(event_id)

    def count(self) -> int:
        with self._lock:
            return len(self._index)

    def head_hash(self) -> str:
        with self._lock:
            return self._chain.head_hash()

    def apply_retention(self, now=None) -> int:
        """主动应用时间保留策略，返回淘汰条数。"""
        with self._lock:
            return self._evict_expired(now)

    # ---- 摄取 ----
    def ingest(self, event: EventInput, now=None) -> IngestionResult:
        # 1) 必填字段（结构校验无需持锁）
        for name in _REQUIRED_FIELDS:
            value = getattr(event, name, None)
            if value is None or (isinstance(value, str) and not value.strip()):
                return self._reject(
                    event,
                    RejectReason.MISSING_FIELD,
                    f"missing or empty required field: {name}",
                )
        if not isinstance(event.payload, dict):
            return self._reject(event, RejectReason.MISSING_FIELD, "payload must be an object")

        # 2) 时间戳合法性
        try:
            ts = _parse_timestamp(event.timestamp)
        except (ValueError, TypeError) as exc:
            return self._reject(
                event, RejectReason.INVALID_TIMESTAMP, f"invalid timestamp {event.timestamp!r}: {exc}"
            )

        # 3) 来源一致性：声明来源 == 事件体内嵌来源 == 本存储来源
        if event.declared_source != event.source:
            return self._reject(
                event,
                RejectReason.SOURCE_MISMATCH,
                f"declared source {event.declared_source!r} != embedded source {event.source!r}",
            )

        with self._lock:
            if event.source != self.source:
                return self._reject(
                    event,
                    RejectReason.SOURCE_MISMATCH,
                    f"routed to store {self.source!r} but event source is {event.source!r}",
                )

            # 4) 幂等：同 event_id 重放必须返回与首次一致的结果，不产生新记录
            existing = self._index.get(event.event_id)
            if existing is not None:
                return self._replay_result(event, existing)

            # 5) 先应用时间淘汰，再判定容量（淘汰后才有空位）
            self._evict_expired(now)

            # 6) 容量硬上限：背压式拒绝，绝不静默丢弃
            if (
                self.retention.max_count is not None
                and len(self._index) >= self.retention.max_count
            ):
                return self._reject(
                    event,
                    RejectReason.CAPACITY_EXCEEDED,
                    f"source {self.source!r} at capacity {self.retention.max_count}; "
                    "apply backpressure or wait for time-based eviction",
                )

            # 7) 序号：客户端可提供序号但必须严格大于已落库最大序号；否则由服务端分配
            if event.sequence is not None:
                if event.sequence != self._next_sequence:
                    return self._reject(
                        event,
                        RejectReason.SEQUENCE_CONFLICT,
                        f"sequence {event.sequence} out of order; expected {self._next_sequence}",
                    )
                sequence = event.sequence
            else:
                sequence = self._next_sequence

            prev_hash = self._chain.head_hash()
            record_hash = compute_record_hash(
                event.event_id,
                event.source,
                event.event_type,
                ts.isoformat(),
                sequence,
                event.payload,
                prev_hash,
            )
            stored = StoredEvent(
                event_id=event.event_id,
                source=event.source,
                event_type=event.event_type,
                timestamp=ts,
                sequence=sequence,
                payload=event.payload,
                prev_hash=prev_hash,
                record_hash=record_hash,
            )
            self._chain.append(stored)
            self._index[event.event_id] = stored
            self._next_sequence = sequence + 1
            return IngestionResult(
                accepted=True,
                event_id=event.event_id,
                source=event.source,
                sequence=sequence,
                record_hash=record_hash,
            )

    # ---- 内部 ----
    def _evict_expired(self, now) -> int:
        events = self._chain.get_all()
        expired = select_evicted(events, self.retention, now)
        if not expired:
            return 0
        removed = self._chain.truncate_prefix(len(expired))
        for stored in removed:
            self._index.pop(stored.event_id, None)
        # 已淘汰事件的序号不再复用；_next_sequence 保持只增，序号全局单调不重复
        return len(removed)

    def _replay_result(self, event: EventInput, existing: StoredEvent) -> IngestionResult:
        same = (
            existing.source == event.source
            and existing.event_type == event.event_type
            and existing.payload == event.payload
            and existing.timestamp == _safe_parse(event.timestamp)
        )
        if not same:
            return IngestionResult(
                accepted=False,
                event_id=event.event_id,
                source=event.source,
                reason=RejectReason.DUPLICATE_EVENT,
                detail="event_id already exists with different content; replay must be identical",
                duplicate=True,
                record_hash=existing.record_hash,
            )
        return IngestionResult(
            accepted=True,
            event_id=event.event_id,
            source=event.source,
            sequence=existing.sequence,
            detail="idempotent replay: no new record created",
            duplicate=True,
            record_hash=existing.record_hash,
        )

    @staticmethod
    def _reject(event: EventInput, reason: RejectReason, detail: str) -> IngestionResult:
        return IngestionResult(
            accepted=False,
            event_id=getattr(event, "event_id", "") or "",
            source=getattr(event, "source", "") or getattr(event, "declared_source", "") or "",
            reason=reason,
            detail=detail,
        )


def _safe_parse(raw: str):
    try:
        return _parse_timestamp(raw)
    except (ValueError, TypeError):
        return object()  # 与任何 datetime 都不相等，触发内容不一致分支


class EventStore:
    """多来源聚合存储：按来源独立加锁，未知来源显式拒绝。"""

    def __init__(
        self,
        retention: RetentionPolicy | None = None,
        known_sources: set[str] | None = None,
    ) -> None:
        self.retention = retention or DEFAULT_RETENTION
        # None 表示允许任意来源（自动注册）；非空集合为白名单
        self.known_sources: set[str] | None = set(known_sources) if known_sources is not None else None
        self._stores: dict[str, SourceStore] = {}
        self._guard = threading.Lock()

    def _route(self, declared_source: str, embedded_source: str) -> SourceStore | IngestionResult:
        source = embedded_source or declared_source
        with self._guard:
            store = self._stores.get(source)
            if store is None:
                if self.known_sources is not None and source not in self.known_sources:
                    return IngestionResult(
                        accepted=False,
                        event_id="",
                        source=source,
                        reason=RejectReason.UNKNOWN_SOURCE,
                        detail=f"source {source!r} is not registered",
                    )
                store = SourceStore(source, self.retention)
                self._stores[source] = store
            return store

    def ingest_batch(self, events: list[EventInput], now=None) -> list[IngestionResult]:
        results: list[IngestionResult] = []
        for event in events:
            store_or_result = self._route(event.declared_source, event.source)
            if isinstance(store_or_result, IngestionResult):
                # 补充事件标识，便于日志定位
                store_or_result = IngestionResult(
                    accepted=False,
                    event_id=event.event_id or "",
                    source=event.source or event.declared_source,
                    reason=RejectReason.UNKNOWN_SOURCE,
                    detail=store_or_result.detail,
                )
                results.append(store_or_result)
                continue
            results.append(store_or_result.ingest(event, now=now))
        return results

    def get_store(self, source: str) -> SourceStore | None:
        with self._guard:
            return self._stores.get(source)

    def sources(self) -> list[str]:
        with self._guard:
            return sorted(self._stores)

    def verify_all(self) -> list[VerifyResult]:
        with self._guard:
            stores = list(self._stores.values())
        return [store.verify() for store in stores]

    def apply_retention(self, now=None) -> dict[str, int]:
        with self._guard:
            stores = list(self._stores.items())
        return {source: store.apply_retention(now) for source, store in stores}
