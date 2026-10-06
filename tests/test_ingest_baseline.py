"""既有摄取语义基线：接收、幂等、冲突、序号、容量背压、保留与完整性。"""
import logging

from app.models import ChainStatus, IngestStatus, RejectReason
from app.retention import RetentionConfig
from app.service import AuditService
from tests.conftest import make_event as ev

log = logging.getLogger("test.baseline")


def test_fresh_accept(service, event_factory):
    r = service.ingest_batch([event_factory("e-1", 0)])[0]
    log.info("event_id=e-1 -> status=%s", r.status)
    assert r.status == IngestStatus.ACCEPTED


def test_idempotent_replay(service, event_factory):
    service.ingest_batch([event_factory("e-2", 0)])
    r = service.ingest_batch([event_factory("e-2", 0)])[0]
    log.info("event_id=e-2 重放 -> status=%s existing_sequence=%s",
             r.status, r.existing_sequence)
    assert r.status == IngestStatus.DUPLICATE
    assert r.existing_sequence == 0
    assert service.store.count_for_source("s1") == 1


def test_conflict_same_id_different_content(service, event_factory):
    service.ingest_batch([event_factory("e-3", 0)])
    r = service.ingest_batch(
        [event_factory("e-3", 1, payload={"tampered": True})])[0]
    log.info("event_id=e-3 冲突 -> status=%s reason=%s", r.status, r.reason)
    assert r.status == IngestStatus.REJECTED
    assert r.reason == RejectReason.DUPLICATE_EVENT_ID


def test_sequence_must_be_strictly_monotonic(service, event_factory):
    service.ingest_batch([event_factory("e-4", 0)])
    skip = service.ingest_batch([event_factory("e-5", 5)])[0]
    log.info("event_id=e-5 跳号 -> reason=%s", skip.reason)
    assert skip.reason == RejectReason.SEQUENCE_OUT_OF_ORDER
    service.ingest_batch([event_factory("e-5", 1)])
    dup = service.ingest_batch([event_factory("e-6", 1)])[0]
    log.info("event_id=e-6 序号重复 -> reason=%s", dup.reason)
    assert dup.reason == RejectReason.DUPLICATE_SEQUENCE


def test_capacity_backpressure():
    svc = AuditService(retention=RetentionConfig(
        max_events_per_source=2, max_age_ms=1000))
    assert svc.ingest_batch([ev("c-1", 0)])[0].status == IngestStatus.ACCEPTED
    assert svc.ingest_batch([ev("c-2", 1)])[0].status == IngestStatus.ACCEPTED
    r = svc.ingest_batch([ev("c-3", 2)])[0]
    log.info("event_id=c-3 容量背压 -> status=%s reason=%s", r.status, r.reason)
    assert r.status == IngestStatus.REJECTED
    assert r.reason == RejectReason.CAPACITY_EXCEEDED
    # 执行保留策略（按年龄）释放空间后可重试
    evicted = svc.run_retention(now_ms=1700000000000 + 1001)
    log.info("保留策略淘汰: %s", evicted)
    assert evicted == {"s1": 2}
    # c-3 此前有 CAPACITY_EXCEEDED 留档，本次通过 → 修复重投语义
    r2 = svc.ingest_batch([ev("c-3", 2)])[0]
    log.info("event_id=c-3 重试 -> status=%s", r2.status)
    assert r2.status == IngestStatus.REPAIRED


def test_integrity_valid_and_tamper_detection(service, event_factory):
    service.ingest_batch([event_factory(f"t-{i}", i) for i in range(3)])
    v = service.verify_source("s1")
    log.info("完整性: %s", v.message)
    assert v.status == ChainStatus.VALID
    # 篡改已落库事件内容
    stored = service.store.get_event("t-1")
    object.__setattr__(stored, "payload", {"forged": True})
    v2 = service.verify_source("s1")
    log.info("篡改后完整性: %s", v2.message)
    assert v2.status == ChainStatus.BROKEN_HASH


def test_retention_keeps_chain_verifiable():
    svc = AuditService(retention=RetentionConfig(
        max_events_per_source=100, max_age_ms=1000))
    svc.ingest_batch([ev(f"r-{i}", i, timestamp=1700000000000 + i)
                      for i in range(5)])
    evicted = svc.run_retention(now_ms=1700000000000 + 1002)
    log.info("按年龄淘汰: %s", evicted)
    assert evicted == {"s1": 2}
    v = svc.verify_source("s1")
    log.info("淘汰后完整性: %s", v.message)
    assert v.status == ChainStatus.VALID
    assert v.verified_count == 3
