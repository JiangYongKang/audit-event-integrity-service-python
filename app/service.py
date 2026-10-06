"""应用服务门面：摄取、保留、校验、评估。

并发约定：摄取（含修复重投）整体在 self._ingest_lock 内串行化，
保证事件标识判重、来源内序号检查与链追加是原子的；
校验与评估为只读操作，不加锁。
"""
from __future__ import annotations

import threading
import time

from app.integrity import IntegrityChain
from app.models import (
    AuditEvent,
    ChainEntry,
    EvaluationReport,
    EventEvaluation,
    IngestResult,
    IngestStatus,
    RejectReason,
    VerificationResult,
)
from app.retention import RetentionConfig, RetentionPolicy
from app.rules import RuleDefinition, RuleEngine
from app.store import EventStore
from app.validator import EventValidator


def _now_ms() -> int:
    return int(time.time() * 1000)


class AuditService:
    def __init__(self, retention: RetentionConfig | None = None) -> None:
        self.store = EventStore()
        self.validator = EventValidator(self.store)
        self.chain = IntegrityChain()
        self.rules = RuleEngine()
        self.retention = retention
        self._ingest_lock = threading.Lock()
        # 每个来源已接收的最大序号（来源内序号单调约束）
        self._last_sequence: dict[str, int] = {}

    # ---- 摄取 ----
    def ingest_batch(
        self, events: list[dict], declared_source: str | None = None
    ) -> list[IngestResult]:
        return [self._ingest_one(raw, declared_source) for raw in events]

    def _ingest_one(self, raw: dict, declared_source: str | None) -> IngestResult:
        event, rejection = self.validator.normalize_and_validate(raw, declared_source)
        if rejection is not None:
            reason, detail = rejection
            return IngestResult(
                status=IngestStatus.REJECTED,
                event_id=raw.get("event_id") if isinstance(raw, dict) else None,
                source=raw.get("source") if isinstance(raw, dict) else None,
                reason=reason, detail=detail)
        assert event is not None
        with self._ingest_lock:
            return self._accept_or_reject(event)

    def _accept_or_reject(self, event: AuditEvent) -> IngestResult:
        """锁内判定：幂等重放 / 冲突 / 序号约束 / 容量背压 / 接收。"""
        existing = self.store.get_event(event.event_id)
        if existing is not None:
            if existing == event:
                return IngestResult(
                    status=IngestStatus.DUPLICATE,
                    event_id=event.event_id, source=event.source,
                    sequence=event.sequence,
                    existing_sequence=existing.sequence,
                    detail="幂等重放：内容与已接收记录一致，未重复落库")
            return IngestResult(
                status=IngestStatus.REJECTED,
                event_id=event.event_id, source=event.source,
                sequence=event.sequence,
                reason=RejectReason.DUPLICATE_EVENT_ID,
                existing_sequence=existing.sequence,
                detail=(f"事件标识 {event.event_id} 已接收过不同内容"
                        f"（原来源 {existing.source}，原序号 {existing.sequence}），"
                        "按冲突拒绝"))

        last_seq = self._last_sequence.get(event.source)
        if last_seq is not None:
            if event.sequence == last_seq:
                return IngestResult(
                    status=IngestStatus.REJECTED,
                    event_id=event.event_id, source=event.source,
                    sequence=event.sequence,
                    reason=RejectReason.DUPLICATE_SEQUENCE,
                    detail=f"来源 {event.source} 序号 {event.sequence} 与已接收序号重复")
            if event.sequence < last_seq:
                return IngestResult(
                    status=IngestStatus.REJECTED,
                    event_id=event.event_id, source=event.source,
                    sequence=event.sequence,
                    reason=RejectReason.SEQUENCE_OUT_OF_ORDER,
                    detail=(f"来源 {event.source} 序号倒退："
                            f"当前 {event.sequence} < 已接收 {last_seq}"))

        if self.retention is not None:
            in_window = self.store.count_for_source(event.source)
            if in_window >= self.retention.max_events_per_source:
                return IngestResult(
                    status=IngestStatus.REJECTED,
                    event_id=event.event_id, source=event.source,
                    sequence=event.sequence,
                    reason=RejectReason.CAPACITY_EXCEEDED,
                    detail=(f"来源 {event.source} 在窗事件已达容量上限 "
                            f"{self.retention.max_events_per_source}，"
                            "请先执行保留策略再重试（背压）"))

        self._commit(event)
        return IngestResult(
            status=IngestStatus.ACCEPTED,
            event_id=event.event_id, source=event.source,
            sequence=event.sequence)

    def _commit(self, event: AuditEvent) -> None:
        """锁内落库：事件 + 完整性链追加 + 序号推进。"""
        chain = self.store.get_chain(event.source)
        prev_hash = chain[-1].entry_hash if chain else None
        event_hash = self.chain.hash_event(event)
        entry = ChainEntry(
            event_id=event.event_id,
            sequence=event.sequence,
            event_hash=event_hash,
            entry_hash=self.chain.derive_entry_hash(
                prev_hash, event_hash, event.sequence),
        )
        self.store.put_event(event)
        self.store.append_entry(event.source, entry)
        self._last_sequence[event.source] = event.sequence

    # ---- 保留策略 ----
    def run_retention(self, now_ms: int | None = None) -> dict[str, int]:
        if self.retention is None:
            return {}
        now = now_ms if now_ms is not None else _now_ms()
        policy = RetentionPolicy(self.retention)
        evicted: dict[str, int] = {}
        with self._ingest_lock:
            for source in self.store.sources():
                events = self.store.get_events_by_source(source)
                to_evict = policy.choose_evictions(events, now)
                if not to_evict:
                    continue
                chain = self.store.get_chain(source)
                new_tail = chain[len(to_evict) - 1].entry_hash
                self.store.truncate_prefix(source, len(to_evict), new_tail)
                evicted[source] = len(to_evict)
        return evicted

    # ---- 完整性校验 ----
    def verify_source(self, source: str) -> VerificationResult:
        return self.chain.verify(
            source=source,
            events=self.store.get_events_by_source(source),
            chain=self.store.get_chain(source),
            anchors=self.store.get_anchors(source),
            checkpoint_tail=self.store.get_checkpoint_tail(source),
        )

    def verify_all(self) -> list[VerificationResult]:
        return [self.verify_source(s) for s in self.store.sources()]

    # ---- 规则评估 ----
    def evaluate(self, source: str | None = None,
                 event_ids: list[str] | None = None) -> EvaluationReport:
        if event_ids is not None:
            events = [e for e in (self.store.get_event(i) for i in event_ids)
                      if e is not None]
        elif source is not None:
            events = self.store.get_events_by_source(source)
        else:
            events = [self.store.get_event(i) for i in
                      sorted(self.store._events)]  # noqa: SLF001
            events = [e for e in events if e is not None]
        evaluations = tuple(
            EventEvaluation(event_id=e.event_id,
                            rule_results=tuple(self.rules.evaluate_event(e)))
            for e in events)
        return EvaluationReport(
            evaluated_at=_now_ms(),
            rule_versions={d.rule_id: d.version for d in self.rules.list_current()},
            results=evaluations)

    def upsert_rule(self, definition: RuleDefinition) -> RuleDefinition:
        return self.rules.upsert_rule(definition)

    def delete_rule(self, rule_id: str) -> None:
        self.rules.delete_rule(rule_id)

    def get_event(self, event_id: str):
        return self.store.get_event(event_id)
