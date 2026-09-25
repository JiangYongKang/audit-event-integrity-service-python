"""事件规范化、幂等重放与各类可区分拒绝原因。"""
from __future__ import annotations

from audit_service.models import RejectReason
from tests.factories import make_event


def test_batch_accept_and_normalize(service, helpers):
    events = [make_event("evt-1"), make_event("evt-2", event_type="refund")]
    results = service.ingest_batch(events)
    helpers.log_ingestion(results)

    assert [r.accepted for r in results] == [True, True]
    assert [r.sequence for r in results] == [1, 2]
    # 规范化后记录可按 event_id 取回，时间戳已解析
    stored = service.store.get_store("src-billing").get("evt-1")
    assert stored is not None and stored.sequence == 1
    assert stored.timestamp.isoformat() == "2026-09-25T10:00:00+00:00"


def test_idempotent_replay_creates_no_duplicate(service, helpers):
    service.ingest_batch([make_event("evt-1", payload={"amount": 100})])
    store = service.store.get_store("src-billing")
    assert store.count() == 1

    # 同一 event_id 重复投递（批量内含重复 + 独立重放）
    replay = service.ingest_batch([
        make_event("evt-1", payload={"amount": 100}),
        make_event("evt-1", payload={"amount": 100}),
    ])
    helpers.log_ingestion(replay)

    assert all(r.accepted and r.duplicate for r in replay)
    assert all(r.sequence == 1 and r.record_hash for r in replay)
    assert store.count() == 1  # 不得产生重复记录
    assert store.verify().ok


def test_same_id_different_content_is_distinct_rejection(service, helpers):
    service.ingest_batch([make_event("evt-1", payload={"amount": 100})])
    results = service.ingest_batch([make_event("evt-1", payload={"amount": 999})])
    helpers.log_ingestion(results)

    assert not results[0].accepted
    assert results[0].reason is RejectReason.DUPLICATE_EVENT
    assert "different content" in results[0].detail


def test_missing_field_is_rejected_with_reason(service, helpers):
    bad = make_event("")  # 空 event_id
    (result,) = service.ingest_batch([bad])
    helpers.log_ingestion([result])
    assert result.reason is RejectReason.MISSING_FIELD
    assert "event_id" in result.detail
    assert result.event_id == ""


def test_invalid_timestamps_are_distinguishable(service, helpers):
    # 空时间戳属于字段缺失（先于格式校验），与格式非法可区分
    empty, = service.ingest_batch([make_event("evt-empty-ts", timestamp="")])
    assert empty.reason is RejectReason.MISSING_FIELD
    helpers.log_ingestion([empty])

    for raw in ["not-a-time", "2026-13-45T99:00:00", "2026-02-30T12:00:00"]:
        result, = service.ingest_batch([make_event(f"evt-bad-{raw}", timestamp=raw)])
        assert result.reason is RejectReason.INVALID_TIMESTAMP, (raw, result)
        helpers.log_ingestion([result])


def test_source_mismatch_declared_vs_embedded(service, helpers):
    event = make_event("evt-3", source="src-billing", declared_source="src-iam")
    result, = service.ingest_batch([event])
    helpers.log_ingestion([result])
    assert result.reason is RejectReason.SOURCE_MISMATCH
    assert "src-iam" in result.detail and "src-billing" in result.detail


def test_unknown_source_rejected(service, helpers):
    event = make_event("evt-4", source="src-stranger", declared_source="src-stranger")
    result, = service.ingest_batch([event])
    helpers.log_ingestion([result])
    assert result.reason is RejectReason.UNKNOWN_SOURCE


def test_rejected_events_never_silent(service):
    """每类失败都必须显式出现在返回结果中，结果条数与请求条数一致。"""
    events = [
        make_event(""),
        make_event("evt-bad-ts", timestamp="nope"),
        make_event("evt-src", source="src-iam", declared_source="src-gateway"),
        make_event("evt-ok"),
    ]
    results = service.ingest_batch(events)
    assert len(results) == len(events)
    reasons = {r.reason for r in results if not r.accepted}
    assert reasons == {
        RejectReason.MISSING_FIELD,
        RejectReason.INVALID_TIMESTAMP,
        RejectReason.SOURCE_MISMATCH,
    }
