"""被拒投递留档 + 修复后重投 的专项测试。"""
from __future__ import annotations

import logging
import threading

import pytest

from app.models import ChainStatus, IngestStatus, RejectReason
from app.retention import RetentionConfig
from app.service import AuditService

logger = logging.getLogger("test.repair")


def make_event(event_id: str, source: str = "s1", sequence: int = 1,
               timestamp: int = 1_000, event_type: str = "login",
               payload: dict | None = None) -> dict:
    return {
        "event_id": event_id,
        "source": source,
        "sequence": sequence,
        "timestamp": timestamp,
        "event_type": event_type,
        "payload": payload or {},
    }


@pytest.fixture()
def service() -> AuditService:
    return AuditService(RetentionConfig(max_events_per_source=100))


class TestRejectionArchive:
    def test_repeated_identical_rejection_deduped(self, service):
        """同一份被拒内容反复投递：留档不重复，只累计次数。"""
        bad = make_event("e-bad", timestamp="不是时间戳")
        for round_no in range(3):
            result = service.ingest_batch([bad])[0]
            logger.info("event_id=e-bad 第 %d 轮投递: status=%s reason=%s",
                        round_no + 1, result.status.value, result.reason.value)
            assert result.status == IngestStatus.REJECTED
            assert result.reason == RejectReason.MALFORMED_TIMESTAMP
        records = service.query_rejections(event_id="e-bad")
        logger.info("event_id=e-bad 留档数=%d attempts=%d",
                    len(records), records[0].attempts)
        assert len(records) == 1
        assert records[0].attempts == 3
        assert records[0].raw == bad  # 原始内容留档

    def test_query_filters(self, service):
        service.ingest_batch([make_event("e1", source="s1", timestamp="x")])
        service.ingest_batch([make_event("e2", source="s2", sequence="bad")])
        service.ingest_batch([make_event("e3", source="s1", sequence=1)],
                             declared_source="s9")
        by_source = service.query_rejections(source="s1")
        by_reason = service.query_rejections(
            reason=RejectReason.SOURCE_MISMATCH)
        by_id = service.query_rejections(event_id="e2")
        logger.info("按来源 s1: %s; 按原因 source_mismatch: %s; 按标识 e2: %s",
                    [r.event_id for r in by_source],
                    [(r.event_id, r.reason.value) for r in by_reason],
                    [(r.event_id, r.reason.value) for r in by_id])
        assert sorted(r.event_id for r in by_source) == ["e1", "e3"]
        assert [r.event_id for r in by_reason] == ["e3"]
        assert [r.event_id for r in by_id] == ["e2"]
        assert by_id[0].reason == RejectReason.MISSING_FIELD

    def test_archive_capacity_backpressure(self):
        """留档写满：可区分背压，不静默丢弃、不挤掉老留档。"""
        svc = AuditService(archive_capacity=2)
        for i in range(2):
            svc.ingest_batch([make_event(f"e{i}", timestamp="x")])
        overflow = svc.ingest_batch([make_event("e2", timestamp="x")])[0]
        logger.info("event_id=e2 留档写满时投递: status=%s reason=%s",
                    overflow.status.value, overflow.reason.value)
        assert overflow.status == IngestStatus.REJECTED
        assert overflow.reason == RejectReason.ARCHIVE_CAPACITY_EXCEEDED
        assert "malformed_timestamp" in (overflow.detail or "")  # 原始原因可溯
        # 老留档仍在，未被挤掉
        remaining = svc.query_rejections()
        assert sorted(r.event_id for r in remaining) == ["e0", "e1"]
        # 已留档内容重复投递仍只累计次数，不触发背压
        again = svc.ingest_batch([make_event("e0", timestamp="x")])[0]
        assert again.reason == RejectReason.MALFORMED_TIMESTAMP
        assert svc.query_rejections(event_id="e0")[0].attempts == 2


class TestRepairIngest:
    def test_repair_success(self, service):
        """修复后通过全部校验：按正常路径接收，参与完整性校验，留档留痕。"""
        bad = make_event("e-fix", sequence=1, timestamp="坏掉了")
        rejected = service.ingest_batch([bad])[0]
        logger.info("event_id=e-fix 首次: %s/%s",
                    rejected.status.value, rejected.reason.value)

        fixed = make_event("e-fix", sequence=1, timestamp=1_700_000_000_000)
        repaired = service.repair_ingest(fixed)
        logger.info("event_id=e-fix 修复重投: status=%s detail=%s",
                    repaired.status.value, repaired.detail)
        assert repaired.status == IngestStatus.REPAIRED

        # 与正常接收的事件一样参与完整性校验
        verify = service.verify_source("s1")
        logger.info("修复后完整性校验: %s (%s)", verify.status.value, verify.message)
        assert verify.status == ChainStatus.VALID
        assert verify.verified_count == 1
        assert service.get_event("e-fix") is not None

        # 当初被拒的留档与原因仍可查（合规留痕），并带 resolved 标记
        records = service.query_rejections(event_id="e-fix")
        assert len(records) == 1
        assert records[0].reason == RejectReason.MALFORMED_TIMESTAMP
        assert records[0].resolved is True
        logger.info("event_id=e-fix 留档留痕: reason=%s resolved=%s",
                    records[0].reason.value, records[0].resolved)

    def test_repair_still_failing(self, service):
        """修复后仍不通过：给出这一轮的原因，并写入修复通道留档。"""
        service.ingest_batch([make_event("e-x", sequence=1, timestamp="bad")])
        still_bad = make_event("e-x", sequence=1, timestamp=1_000,
                               event_type="")  # 仍缺合法 event_type
        result = service.repair_ingest(still_bad)
        logger.info("event_id=e-x 修复仍不通过: status=%s reason=%s detail=%s",
                    result.status.value, result.reason.value, result.detail)
        assert result.status == IngestStatus.REJECTED
        assert result.reason == RejectReason.MISSING_FIELD
        records = service.query_rejections(event_id="e-x")
        logger.info("event_id=e-x 留档: %s",
                    [(r.reason.value, r.channel) for r in records])
        assert {(r.reason, r.channel) for r in records} == {
            (RejectReason.MALFORMED_TIMESTAMP, "ingest"),
            (RejectReason.MISSING_FIELD, "repair"),
        }
        assert service.get_event("e-x") is None  # 未落库

    def test_accepted_id_conflict_not_bypassed(self, service):
        """已成功接收的标识再投不同内容：正常通道与修复通道都按冲突拒绝。"""
        original = make_event("e-done", sequence=1)
        accepted = service.ingest_batch([original])[0]
        assert accepted.status == IngestStatus.ACCEPTED

        tampered = make_event("e-done", sequence=2, payload={"改": "过"})
        via_ingest = service.ingest_batch([tampered])[0]
        via_repair = service.repair_ingest(tampered)
        logger.info("event_id=e-done 正常通道冲突: %s/%s; 修复通道冲突: %s/%s",
                    via_ingest.status.value, via_ingest.reason.value,
                    via_repair.status.value, via_repair.reason.value)
        assert via_ingest.reason == RejectReason.DUPLICATE_EVENT_ID
        assert via_repair.status == IngestStatus.REJECTED
        assert via_repair.reason == RejectReason.DUPLICATE_EVENT_ID
        # 同内容重放仍是幂等
        replay = service.repair_ingest(original)
        assert replay.status == IngestStatus.DUPLICATE
        # 已落库数据未被改写
        assert service.get_event("e-done").payload == {}
        assert service.verify_source("s1").status == ChainStatus.VALID

    def test_repair_requires_prior_rejection(self, service):
        fresh = make_event("e-new", sequence=1)
        result = service.repair_ingest(fresh)
        logger.info("event_id=e-new 无在先拒档走修复通道: %s/%s",
                    result.status.value, result.reason.value)
        assert result.status == IngestStatus.REJECTED
        assert result.reason == RejectReason.REPAIR_NOT_ALLOWED
        assert service.get_event("e-new") is None

    def test_repair_respects_sequence_constraints(self, service):
        """修复重投仍受来源内序号约束。"""
        service.ingest_batch([make_event("e1", sequence=10)])
        service.ingest_batch([make_event("e2", sequence=9, timestamp="bad")])
        # 修复 e2 时序号 9 < 已接收 10 -> 仍被拒
        result = service.repair_ingest(make_event("e2", sequence=9))
        logger.info("event_id=e2 修复重投序号倒退: %s/%s",
                    result.status.value, result.reason.value)
        assert result.status == IngestStatus.REJECTED
        assert result.reason == RejectReason.SEQUENCE_OUT_OF_ORDER
        # 修正序号后可通过
        ok = service.repair_ingest(make_event("e2", sequence=11))
        logger.info("event_id=e2 修正序号后: %s", ok.status.value)
        assert ok.status == IngestStatus.REPAIRED

    def test_repair_does_not_alter_chain_or_verdict(self, service):
        """留档与修复重投不改写入链数据、不改变校验结论。"""
        service.ingest_batch([make_event(f"e{i}", sequence=i)
                              for i in range(1, 3)])
        before = service.verify_source("s1")
        service.ingest_batch([make_event("e9", sequence=3, timestamp="bad")])
        service.repair_ingest(make_event("e9", sequence=3, timestamp="x"))
        mid = service.verify_source("s1")
        assert before.status == mid.status == ChainStatus.VALID
        assert before.head_entry_hash == mid.head_entry_hash  # 既有链未变
        repaired = service.repair_ingest(make_event("e9", sequence=3))
        assert repaired.status == IngestStatus.REPAIRED
        after = service.verify_source("s1")
        logger.info("修复前链头=%s 修复后链头=%s 校验=%s",
                    (before.head_entry_hash or "")[:12],
                    (after.head_entry_hash or "")[:12], after.status.value)
        assert after.status == ChainStatus.VALID
        assert after.verified_count == 3  # 只追加，不改写


class TestRepairConcurrency:
    def test_concurrent_repair_same_id_succeeds_once(self, service):
        """并发下同一标识的修复重投只成功一次，且来源内序号单调不重复。"""
        service.ingest_batch([make_event("e-race", sequence=1, timestamp="bad")])
        workers = 8
        results = []
        barrier = threading.Barrier(workers)

        def attempt(seq: int) -> None:
            barrier.wait()
            results.append(service.repair_ingest(
                make_event("e-race", sequence=seq)))

        threads = [threading.Thread(target=attempt, args=(i + 1,))
                   for i in range(workers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        for r in results:
            logger.info("event_id=e-race 并发修复结果: status=%s reason=%s seq=%s",
                        r.status.value,
                        r.reason.value if r.reason else "-", r.sequence)
        repaired = [r for r in results if r.status == IngestStatus.REPAIRED]
        assert len(repaired) == 1, "同一标识的修复重投必须只成功一次"
        winner_seq = repaired[0].sequence
        # 其余线程：同内容幂等重放，或标识冲突/序号约束被拒（均为可区分结果）
        for r in results:
            if r.status == IngestStatus.REPAIRED:
                continue
            assert r.status in (IngestStatus.DUPLICATE, IngestStatus.REJECTED)
            if r.status == IngestStatus.REJECTED:
                assert r.reason in (RejectReason.DUPLICATE_EVENT_ID,
                                    RejectReason.DUPLICATE_SEQUENCE,
                                    RejectReason.SEQUENCE_OUT_OF_ORDER)
        # 来源内序号单调不重复
        accepted_seqs = [e.sequence for e in service.store.get_events_by_source("s1")]
        logger.info("event_id=e-race 胜出的序号=%s，来源 s1 已接收序号=%s",
                    winner_seq, accepted_seqs)
        assert accepted_seqs == sorted(accepted_seqs)
        assert len(accepted_seqs) == len(set(accepted_seqs))
        assert service.verify_source("s1").status == ChainStatus.VALID
