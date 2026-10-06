"""应用服务门面：摄取、留档、修复重投、保留、校验、评估。

摄取判定顺序（每条事件独立得出可区分结果）：
1. 单条校验（字段/时间戳/来源一致性）——失败即拒绝并留档；
2. 幂等：event_id 已接收——内容一致为 DUPLICATE 重放，不一致按
   DUPLICATE_EVENT_ID 冲突拒绝（修复通道同样不得绕过）；
3. 来源内序号：必须严格等于 last+1，否则按重复/乱序拒绝；
4. 容量：在窗事件数达上限——CAPACITY_EXCEEDED 背压拒绝；
5. 接收：写主存、追加完整性链；若该 (source, event_id) 有未解决留档，
   则状态为 REPAIRED 并把留档标记为已解决（留档保留可查）。

留档容量写满时，新键的拒绝结果以 ARCHIVE_CAPACITY_EXCEEDED 返回（背压），
detail 中保留本轮原始拒绝原因；不静默丢弃、不挤占旧留档。
"""
from __future__ import annotations

import threading
import time

from app.archive import DEFAULT_ARCHIVE_CAPACITY, RejectionArchive
from app.errors import ArchiveCapacityExceeded, IngestRejection, RepairNotAllowed
from app.integrity import IntegrityChain
from app.models import (
    ChainEntry,
    EvaluationReport,
    EventEvaluation,
    IngestResult,
    IngestStatus,
    RejectionRecord,
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
    def __init__(self, retention: RetentionConfig | None = None,
                 archive_capacity: int = DEFAULT_ARCHIVE_CAPACITY) -> None:
        self.store = EventStore()
        self.archive = RejectionArchive(max_records=archive_capacity)
        self.validator = EventValidator(self.store)
        self.chain = IntegrityChain()
        self.rules = RuleEngine()
        self.retention = retention
        self._policy = RetentionPolicy(retention) if retention else None
        # 每个来源一把锁，保证来源内序号判定与追加链的原子性
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    # ---- 内部 ----
    def _lock_for(self, source: str) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(source, threading.Lock())

    def _record_rejection(self, source: str, event_id: str,
                          rejection: IngestRejection, raw: dict,
                          now_ms: int) -> IngestResult:
        """拒绝并留档；留档写满时以可区分的背压原因返回。"""
        reason, detail = rejection.reason, rejection.detail
        try:
            self.archive.record(source, event_id, reason, detail, raw, now_ms)
        except ArchiveCapacityExceeded as exc:
            detail = (f"留档容量已满（{exc.capacity}），本条拒绝未留档；"
                      f"原始拒绝原因={reason.value}: {detail}")
            reason = RejectReason.ARCHIVE_CAPACITY_EXCEEDED
        return IngestResult(
            status=IngestStatus.REJECTED,
            event_id=event_id,
            source=source,
            sequence=raw.get("sequence") if isinstance(raw, dict) else None,
            reason=reason,
            detail=detail,
        )

    def _ingest_one(self, raw: dict, declared_source: str | None,
                    now_ms: int) -> IngestResult:
        event, rejection = self.validator.normalize_and_validate(
            raw, declared_source)
        if rejection is not None:
            assert event is None
            event_id = raw.get("event_id") if isinstance(raw, dict) else None
            source = raw.get("source") if isinstance(raw, dict) else None
            return self._record_rejection(
                source or (declared_source or "<unknown>"),
                event_id if isinstance(event_id, str) else "<invalid>",
                rejection, raw if isinstance(raw, dict) else {}, now_ms)

        assert event is not None
        lock = self._lock_for(event.source)
        with lock:
            # 幂等 / 冲突：event_id 已成功接收过（修复通道不得绕过此语义）
            existing = self.store.get_event(event.event_id)
            if existing is not None:
                if existing == event:
                    return IngestResult(
                        status=IngestStatus.DUPLICATE,
                        event_id=event.event_id,
                        source=event.source,
                        sequence=event.sequence,
                        existing_sequence=existing.sequence,
                        detail="幂等重放：内容与已接收记录一致",
                    )
                return self._record_rejection(
                    event.source, event.event_id,
                    IngestRejection(
                        RejectReason.DUPLICATE_EVENT_ID,
                        f"event_id={event.event_id} 已接收且内容不一致，"
                        "按冲突拒绝"),
                    raw, now_ms)

            # 来源内序号：从 0 起严格递增（next = last + 1），
            # 判定基准不受保留策略淘汰影响
            chain = self.store.get_chain(event.source)
            last_seq = self.store.get_last_sequence(event.source)
            expected = 0 if last_seq is None else last_seq + 1
            if event.sequence != expected:
                if last_seq is not None and event.sequence <= last_seq:
                    used = {e.sequence for e in chain}
                    reason = (RejectReason.DUPLICATE_SEQUENCE
                              if event.sequence in used
                              else RejectReason.SEQUENCE_OUT_OF_ORDER)
                    detail = (f"source={event.source} 序号={event.sequence} "
                              f"不大于已接收最大序号 {last_seq}")
                else:
                    reason = RejectReason.SEQUENCE_OUT_OF_ORDER
                    detail = (f"source={event.source} 序号跳变：期望 "
                              f"{expected}，实际 {event.sequence}")
                return self._record_rejection(
                    event.source, event.event_id,
                    IngestRejection(reason, detail), raw, now_ms)

            # 容量背压：摄取路径不做静默淘汰
            if (self.retention is not None
                    and self.store.count_for_source(event.source)
                    >= self.retention.max_events_per_source):
                return self._record_rejection(
                    event.source, event.event_id,
                    IngestRejection(
                        RejectReason.CAPACITY_EXCEEDED,
                        f"source={event.source} 在窗事件数已达上限 "
                        f"{self.retention.max_events_per_source}，"
                        "请先执行保留策略再重试"),
                    raw, now_ms)

            # 接收：写主存 + 追加完整性链
            self.store.put_event(event)
            prev_hash = chain[-1].entry_hash if chain else \
                self.store.get_checkpoint_tail(event.source)
            event_hash = self.chain.hash_event(event)
            entry_hash = self.chain.derive_entry_hash(
                prev_hash, event_hash, event.sequence)
            self.store.append_entry(event.source, ChainEntry(
                event_id=event.event_id,
                sequence=event.sequence,
                event_hash=event_hash,
                entry_hash=entry_hash,
            ))

            # 修复重投判定：此前有未解决留档 → REPAIRED 并留痕
            prior = self.archive.get(event.source, event.event_id)
            if prior is not None and not prior.resolved:
                self.archive.mark_resolved(
                    event.source, event.event_id, now_ms)
                return IngestResult(
                    status=IngestStatus.REPAIRED,
                    event_id=event.event_id,
                    source=event.source,
                    sequence=event.sequence,
                    detail=(f"修复重投成功：此前因 "
                            f"{prior.reason.value} 被拒，本次通过全部校验"),
                )
            return IngestResult(
                status=IngestStatus.ACCEPTED,
                event_id=event.event_id,
                source=event.source,
                sequence=event.sequence,
            )

    # ---- 摄取 ----
    def ingest_batch(
        self, events: list[dict], declared_source: str | None = None
    ) -> list[IngestResult]:
        now_ms = _now_ms()
        return [self._ingest_one(raw, declared_source, now_ms)
                for raw in events]

    def resubmit(self, source: str, event_id: str, raw: dict,
                 declared_source: str | None = None) -> IngestResult:
        """修复重投通道：仅面向"曾被拒且从未成功落库"的事件标识。

        - 无留档且未接收过：拒绝进入本通道（RepairNotAllowed）；
        - 已成功接收过：回落到既有幂等/冲突语义（重放或冲突拒绝），
          修复通道不得绕过；
        - 其余情况与正常摄取同路径：通过则 REPAIRED，不通过则
          以本轮原因拒绝并更新留档。
        """
        if self.store.get_event(event_id) is None \
                and self.archive.get(source, event_id) is None:
            raise RepairNotAllowed(
                f"event_id={event_id} 既无被拒留档也未接收过，"
                "不属于修复重投对象，请走正常摄取")
        return self._ingest_one(raw, declared_source, _now_ms())

    # ---- 留档查询 ----
    def list_rejections(self, source: str | None = None,
                        reason: RejectReason | None = None,
                        event_id: str | None = None) -> list[RejectionRecord]:
        return self.archive.query(source=source, reason=reason,
                                  event_id=event_id)

    def get_rejection(self, source: str, event_id: str) -> RejectionRecord | None:
        return self.archive.get(source, event_id)

    # ---- 保留 ----
    def run_retention(self, now_ms: int | None = None) -> dict[str, int]:
        if self._policy is None:
            return {}
        now_ms = now_ms if now_ms is not None else _now_ms()
        evicted: dict[str, int] = {}
        for source in self.store.sources():
            lock = self._lock_for(source)
            with lock:
                events = self.store.get_events_by_source(source)
                victims = self._policy.choose_evictions(events, now_ms)
                if not victims:
                    continue
                chain = self.store.get_chain(source)
                cut = len(victims)
                new_tail = chain[cut - 1].entry_hash
                self.store.truncate_prefix(source, cut, new_tail)
                evicted[source] = cut
        return evicted

    # ---- 完整性校验 ----
    def verify_source(self, source: str) -> VerificationResult:
        chain = self.store.get_chain(source)
        events = [self.store.get_event(e.event_id) for e in chain]
        return self.chain.verify(
            source, events, chain,
            self.store.get_anchors(source),
            self.store.get_checkpoint_tail(source),
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
            events = [self.store.get_event(eid) for s in self.store.sources()
                      for e in self.store.get_chain(s)
                      if (eid := e.event_id)]
        results = []
        for event in events:
            rule_results = self.rules.evaluate_event(event)
            results.append(EventEvaluation(
                event_id=event.event_id,
                rule_results=tuple(rule_results),
            ))
        return EvaluationReport(
            evaluated_at=_now_ms(),
            rule_versions={d.rule_id: d.version
                           for d in self.rules.list_current()},
            results=tuple(results),
        )

    def upsert_rule(self, definition: RuleDefinition) -> RuleDefinition:
        return self.rules.upsert_rule(definition)

    def delete_rule(self, rule_id: str) -> None:
        self.rules.delete_rule(rule_id)

    def get_event(self, event_id: str):
        return self.store.get_event(event_id)
