"""被拒投递留档：去重、筛选、容量背压、留痕可查。"""
import logging

from app.models import IngestStatus, RejectReason
from app.service import AuditService
from tests.conftest import make_event as ev

log = logging.getLogger("test.archive")


def _bad_timestamp(event_id, seq, source="s1"):
    e = ev(event_id, seq, source=source)
    e["timestamp"] = "not-a-timestamp"
    return e


def test_repeated_rejection_dedupes_into_one_record(service):
    for round_no in range(3):
        r = service.ingest_batch([_bad_timestamp("dup-1", 0)])[0]
        log.info("第%d轮 event_id=dup-1 -> status=%s reason=%s",
                 round_no + 1, r.status, r.reason)
        assert r.status == IngestStatus.REJECTED
        assert r.reason == RejectReason.MALFORMED_TIMESTAMP
    records = service.list_rejections(source="s1", event_id="dup-1")
    log.info("留档条数=%d attempts=%d", len(records), records[0].attempts)
    assert len(records) == 1            # 同一份内容反复投递 → 同一留档
    assert records[0].attempts == 3
    assert records[0].raw_payload["event_id"] == "dup-1"
    assert records[0].resolved is False


def test_query_filters_by_source_reason_event_id(service):
    service.ingest_batch([_bad_timestamp("f-1", 0)])
    service.ingest_batch([_bad_timestamp("f-2", 0, source="s2")])
    service.ingest_batch([ev("f-3", 0, source="s3"),
                          ev("f-3", 0, source="s3", payload={"x": 1})][:1])
    # s3 制造一个序号乱序拒绝
    service.ingest_batch([ev("f-4", 5, source="s3")])
    by_source = service.list_rejections(source="s2")
    log.info("按来源 s2 筛选: %s", [r.event_id for r in by_source])
    assert [r.event_id for r in by_source] == ["f-2"]
    by_reason = service.list_rejections(
        reason=RejectReason.MALFORMED_TIMESTAMP)
    log.info("按原因 malformed_timestamp 筛选: %s",
             [r.event_id for r in by_reason])
    assert {r.event_id for r in by_reason} == {"f-1", "f-2"}
    by_id = service.list_rejections(event_id="f-4")
    log.info("按事件标识 f-4 筛选: reason=%s", by_id[0].reason)
    assert len(by_id) == 1
    assert by_id[0].reason == RejectReason.SEQUENCE_OUT_OF_ORDER


def test_archive_capacity_backpressure():
    svc = AuditService(archive_capacity=2)
    r1 = svc.ingest_batch([_bad_timestamp("cap-1", 0)])[0]
    r2 = svc.ingest_batch([_bad_timestamp("cap-2", 0)])[0]
    assert r1.reason == RejectReason.MALFORMED_TIMESTAMP
    assert r2.reason == RejectReason.MALFORMED_TIMESTAMP
    assert len(svc.archive) == 2
    # 第三个新键：留档写满 → 可区分的背压拒绝，detail 保留原始原因
    r3 = svc.ingest_batch([_bad_timestamp("cap-3", 0)])[0]
    log.info("event_id=cap-3 留档背压 -> reason=%s detail=%s",
             r3.reason, r3.detail)
    assert r3.status == IngestStatus.REJECTED
    assert r3.reason == RejectReason.ARCHIVE_CAPACITY_EXCEEDED
    assert "malformed_timestamp" in r3.detail
    # 旧留档不被挤掉
    assert svc.get_rejection("s1", "cap-1") is not None
    assert svc.get_rejection("s1", "cap-2") is not None
    assert svc.get_rejection("s1", "cap-3") is None
    # 已有键的重复投递不受容量限制，仍合并到同一留档
    r4 = svc.ingest_batch([_bad_timestamp("cap-1", 0)])[0]
    log.info("event_id=cap-1 重复投递 -> reason=%s", r4.reason)
    assert r4.reason == RejectReason.MALFORMED_TIMESTAMP
    assert svc.get_rejection("s1", "cap-1").attempts == 2
    assert len(svc.archive) == 2
