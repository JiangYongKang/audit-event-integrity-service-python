"""修复重投：成功、仍不通过、边界（已接收标识不得走修复通道）、留痕。"""
import logging

import pytest

from app.errors import RepairNotAllowed
from app.models import ChainStatus, IngestStatus, RejectReason
from tests.conftest import make_event as ev

log = logging.getLogger("test.repair")


def _reject_then(service, event_id, seq, **overrides):
    bad = ev(event_id, seq, **overrides)
    bad["timestamp"] = "bad-ts"
    r = service.ingest_batch([bad])[0]
    log.info("event_id=%s 首次投递 -> status=%s reason=%s",
             event_id, r.status, r.reason)
    assert r.status == IngestStatus.REJECTED
    assert r.reason == RejectReason.MALFORMED_TIMESTAMP
    return bad


def test_repair_resubmit_success(service):
    _reject_then(service, "rp-1", 0)
    fixed = ev("rp-1", 0, payload={"fixed": True})
    r = service.resubmit("s1", "rp-1", fixed)
    log.info("event_id=rp-1 修复重投 -> status=%s detail=%s", r.status, r.detail)
    assert r.status == IngestStatus.REPAIRED
    # 按正常路径落库，参与完整性校验
    assert service.get_event("rp-1") is not None
    v = service.verify_source("s1")
    log.info("修复后完整性: %s", v.message)
    assert v.status == ChainStatus.VALID
    assert v.verified_count == 1
    # 当初被拒的留档与原因仍可查（合规留痕）
    rec = service.get_rejection("s1", "rp-1")
    log.info("留档留痕: resolved=%s reason=%s attempts=%d",
             rec.resolved, rec.reason, rec.attempts)
    assert rec.resolved is True
    assert rec.reason == RejectReason.MALFORMED_TIMESTAMP
    assert rec.raw_payload["timestamp"] == "bad-ts"


def test_repair_resubmit_still_failing(service):
    _reject_then(service, "rp-2", 0)
    still_bad = ev("rp-2", 0)
    still_bad["timestamp"] = "still-bad"
    r = service.resubmit("s1", "rp-2", still_bad)
    log.info("event_id=rp-2 修复仍不通过 -> status=%s reason=%s",
             r.status, r.reason)
    assert r.status == IngestStatus.REJECTED
    assert r.reason == RejectReason.MALFORMED_TIMESTAMP
    rec = service.get_rejection("s1", "rp-2")
    log.info("留档更新: attempts=%d resolved=%s", rec.attempts, rec.resolved)
    assert rec.attempts == 2
    assert rec.resolved is False
    assert service.get_event("rp-2") is None
    # 再次修复，这次通过
    r2 = service.resubmit("s1", "rp-2", ev("rp-2", 0))
    log.info("event_id=rp-2 二次修复 -> status=%s", r2.status)
    assert r2.status == IngestStatus.REPAIRED


def test_accepted_id_cannot_use_repair_channel(service):
    service.ingest_batch([ev("rp-3", 0)])
    # 已接收标识 + 不同内容走修复通道 → 仍按冲突语义拒绝
    r = service.resubmit("s1", "rp-3", ev("rp-3", 1, payload={"x": 9}))
    log.info("event_id=rp-3 已接收+不同内容 -> status=%s reason=%s",
             r.status, r.reason)
    assert r.status == IngestStatus.REJECTED
    assert r.reason == RejectReason.DUPLICATE_EVENT_ID
    # 已接收标识 + 相同内容走修复通道 → 幂等重放，不是"修复"
    r2 = service.resubmit("s1", "rp-3", ev("rp-3", 0))
    log.info("event_id=rp-3 已接收+相同内容 -> status=%s", r2.status)
    assert r2.status == IngestStatus.DUPLICATE
    # 原记录未被改写
    assert service.get_event("rp-3").payload == {}
    assert service.store.count_for_source("s1") == 1


def test_resubmit_requires_prior_rejection(service):
    with pytest.raises(RepairNotAllowed) as exc:
        service.resubmit("s1", "rp-4", ev("rp-4", 0))
    log.info("event_id=rp-4 无留档走修复通道 -> %s", exc.value.detail)


def test_repair_respects_sequence_constraint(service):
    service.ingest_batch([ev("rp-5", 0)])
    # 内容合法但序号跳变 → 被拒并留档
    r0 = service.ingest_batch([ev("rp-6", 5)])[0]
    log.info("event_id=rp-6 首次投递 -> status=%s reason=%s",
             r0.status, r0.reason)
    assert r0.status == IngestStatus.REJECTED
    rec = service.get_rejection("s1", "rp-6")
    log.info("event_id=rp-6 留档原因=%s", rec.reason)
    assert rec.reason == RejectReason.SEQUENCE_OUT_OF_ORDER
    # 修正内容但序号仍不合规 → 仍被拒（本轮原因）
    r = service.resubmit("s1", "rp-6", ev("rp-6", 5))
    log.info("event_id=rp-6 序号仍错 -> reason=%s", r.reason)
    assert r.status == IngestStatus.REJECTED
    assert r.reason == RejectReason.SEQUENCE_OUT_OF_ORDER
    # 序号也修正 → 通过
    r2 = service.resubmit("s1", "rp-6", ev("rp-6", 1))
    log.info("event_id=rp-6 序号修正 -> status=%s", r2.status)
    assert r2.status == IngestStatus.REPAIRED
    seqs = [e.sequence for e in service.store.get_chain("s1")]
    log.info("链上序号: %s", seqs)
    assert seqs == [0, 1]


def test_archive_and_repair_do_not_alter_chain(service):
    service.ingest_batch([ev(f"rp-7-{i}", i) for i in range(3)])
    before = service.verify_source("s1")
    _reject_then(service, "rp-8", 3)
    mid = service.verify_source("s1")
    service.resubmit("s1", "rp-8", ev("rp-8", 3))
    after = service.verify_source("s1")
    log.info("链头: before=%s mid=%s after=%s",
             before.head_entry_hash, mid.head_entry_hash,
             after.head_entry_hash)
    # 留档不改写链：拒绝前后链头不变
    assert before.head_entry_hash == mid.head_entry_hash
    assert before.verified_count == mid.verified_count == 3
    # 修复只是正常追加：链变长且仍然有效
    assert after.status == ChainStatus.VALID
    assert after.verified_count == 4
