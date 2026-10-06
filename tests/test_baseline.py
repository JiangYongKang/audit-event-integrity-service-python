"""基线能力回归测试：批量摄取、幂等、序号约束、容量背压、保留与完整性。"""
from __future__ import annotations

import logging

import pytest

from app.models import ChainStatus, IngestStatus, RejectReason
from app.retention import RetentionConfig
from app.service import AuditService

logger = logging.getLogger("test.baseline")


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
    return AuditService(RetentionConfig(max_events_per_source=3))


class TestIngest:
    def test_batch_accept(self, service):
        events = [make_event(f"e{i}", sequence=i) for i in range(1, 4)]
        results = service.ingest_batch(events)
        assert all(r.status == IngestStatus.ACCEPTED for r in results)
        logger.info("批量接收 3 条: %s", [r.event_id for r in results])

    def test_idempotent_replay(self, service):
        event = make_event("e1", sequence=1)
        first = service.ingest_batch([event])[0]
        replay = service.ingest_batch([event])[0]
        logger.info("event_id=e1 首次=%s 重放=%s", first.status, replay.status)
        assert first.status == IngestStatus.ACCEPTED
        assert replay.status == IngestStatus.DUPLICATE
        assert replay.existing_sequence == 1

    def test_conflict_same_id_different_content(self, service):
        service.ingest_batch([make_event("e1", sequence=1)])
        conflict = service.ingest_batch(
            [make_event("e1", sequence=2, payload={"tampered": True})])[0]
        logger.info("event_id=e1 冲突投递: status=%s reason=%s",
                    conflict.status, conflict.reason)
        assert conflict.status == IngestStatus.REJECTED
        assert conflict.reason == RejectReason.DUPLICATE_EVENT_ID

    def test_sequence_out_of_order_and_duplicate(self, service):
        service.ingest_batch([make_event("e1", sequence=5)])
        backwards = service.ingest_batch([make_event("e2", sequence=3)])[0]
        same = service.ingest_batch([make_event("e3", sequence=5)])[0]
        logger.info("序号倒退: %s/%s; 序号重复: %s/%s",
                    backwards.status, backwards.reason, same.status, same.reason)
        assert backwards.reason == RejectReason.SEQUENCE_OUT_OF_ORDER
        assert same.reason == RejectReason.DUPLICATE_SEQUENCE

    def test_missing_field_and_bad_timestamp(self, service):
        missing = service.ingest_batch([{"event_id": "e1"}])[0]
        bad_ts = service.ingest_batch(
            [make_event("e2", timestamp="昨天")])[0]
        out_of_range = service.ingest_batch(
            [make_event("e3", timestamp=-1)])[0]
        logger.info("缺字段: %s; 时间戳非法: %s; 时间戳越界: %s",
                    missing.reason, bad_ts.reason, out_of_range.reason)
        assert missing.reason == RejectReason.MISSING_FIELD
        assert bad_ts.reason == RejectReason.MALFORMED_TIMESTAMP
        assert out_of_range.reason == RejectReason.TIMESTAMP_OUT_OF_RANGE

    def test_source_mismatch(self, service):
        result = service.ingest_batch(
            [make_event("e1", source="s1")], declared_source="s2")[0]
        logger.info("来源不符: %s", result.reason)
        assert result.reason == RejectReason.SOURCE_MISMATCH

    def test_capacity_backpressure(self, service):
        for i in range(1, 4):
            service.ingest_batch([make_event(f"e{i}", sequence=i)])
        overflow = service.ingest_batch([make_event("e4", sequence=4)])[0]
        logger.info("容量背压: %s/%s", overflow.status, overflow.reason)
        assert overflow.status == IngestStatus.REJECTED
        assert overflow.reason == RejectReason.CAPACITY_EXCEEDED


class TestIntegrity:
    def test_verify_valid(self, service):
        service.ingest_batch([make_event(f"e{i}", sequence=i)
                              for i in range(1, 4)])
        result = service.verify_source("s1")
        logger.info("校验结论: %s (%s)", result.status, result.message)
        assert result.status == ChainStatus.VALID
        assert result.verified_count == 3

    def test_verify_detects_tamper(self, service):
        service.ingest_batch([make_event(f"e{i}", sequence=i)
                              for i in range(1, 3)])
        original = service.store.get_event("e1")
        service.store._events["e1"] = type(original)(
            **{**original.__dict__, "payload": {"forged": True}})
        result = service.verify_source("s1")
        logger.info("篡改后校验: %s (%s)", result.status, result.message)
        assert result.status == ChainStatus.BROKEN_HASH

    def test_retention_then_verify_still_valid(self):
        svc = AuditService(RetentionConfig(max_events_per_source=10,
                                           max_age_ms=50))
        svc.ingest_batch([
            make_event("e1", sequence=1, timestamp=1_001),
            make_event("e2", sequence=2, timestamp=1_002),
            make_event("e3", sequence=3, timestamp=1_100),
        ])
        evicted = svc.run_retention(now_ms=1_100)  #  cutoff=1050，淘汰 e1/e2
        logger.info("保留策略淘汰: %s", evicted)
        assert evicted == {"s1": 2}
        result = svc.verify_source("s1")
        logger.info("淘汰后校验: %s (%s)", result.status, result.message)
        assert result.status == ChainStatus.VALID
        assert result.verified_count == 1
