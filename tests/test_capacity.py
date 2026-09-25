"""容量上限背压：满员时新事件必须被可区分拒绝，重放仍幂等。"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

from audit_service.models import RejectReason
from tests.factories import make_event


def test_capacity_limit_rejects_with_distinct_reason(capacity_service, helpers):
    results = capacity_service.ingest_batch([
        make_event("cap-1", source="src-billing"),
        make_event("cap-2", source="src-billing"),
        make_event("cap-3", source="src-billing"),
    ])
    assert all(r.accepted for r in results)

    overflow = capacity_service.ingest_batch([
        make_event("cap-4", source="src-billing"),
    ])
    helpers.log_ingestion(overflow)
    assert overflow[0].reason is RejectReason.CAPACITY_EXCEEDED
    assert "backpressure" in overflow[0].detail
    # 被拒绝的事件不得落库
    assert capacity_service.store.get_store("src-billing").count() == 3


def test_replay_succeeds_at_capacity(capacity_service, helpers):
    capacity_service.ingest_batch([
        make_event("cap-1", source="src-billing"),
        make_event("cap-2", source="src-billing"),
        make_event("cap-3", source="src-billing"),
    ])
    replay = capacity_service.ingest_batch([make_event("cap-2", source="src-billing")])
    helpers.log_ingestion(replay)
    assert replay[0].accepted and replay[0].duplicate
    assert capacity_service.store.get_store("src-billing").count() == 3


def test_time_eviction_frees_capacity_then_new_event_accepted(helpers):
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=timezone.utc)
    from audit_service.models import RetentionPolicy
    from audit_service.service import AuditService

    svc = AuditService(
        retention=RetentionPolicy(max_age_seconds=60, max_count=3),
        known_sources={"src-billing"},
    )
    # 1) 三条均在保留窗口内 -> 容量满 -> 背压拒绝
    svc.ingest_batch([
        make_event("f-1", source="src-billing", timestamp=t0.isoformat()),
        make_event("f-2", source="src-billing", timestamp=(t0 + timedelta(seconds=10)).isoformat()),
        make_event("f-3", source="src-billing", timestamp=(t0 + timedelta(seconds=20)).isoformat()),
    ], now=t0 + timedelta(seconds=20))
    blocked = svc.ingest_batch(
        [make_event("f-blocked", source="src-billing", timestamp=(t0 + timedelta(seconds=25)).isoformat())],
        now=t0 + timedelta(seconds=25),
    )
    assert blocked[0].reason is RejectReason.CAPACITY_EXCEEDED

    # 2) 时钟推进到 75 秒后：f-1(75s)、f-2(65s) 到期淘汰，f-3(55s) 保留，空出 2 个槽位
    later = t0 + timedelta(seconds=75)
    accepted = svc.ingest_batch(
        [make_event("f-4", source="src-billing", timestamp=later.isoformat())], now=later
    )
    helpers.log_ingestion(accepted)
    assert accepted[0].accepted
    verify, = svc.verify_all()
    helpers.log_verify([verify])
    assert verify.ok and verify.evicted_count == 2
    assert svc.store.get_store("src-billing").count() == 2  # f-3 与 f-4，容量内仍有一个空槽


def test_concurrent_writes_never_exceed_capacity():
    """并发下容量是硬上限：接受数恰为容量，其余得到 CAPACITY_EXCEEDED。"""
    from audit_service.models import RetentionPolicy
    from audit_service.service import AuditService

    svc = AuditService(
        retention=RetentionPolicy(max_count=10),
        known_sources={"src-billing"},
    )
    accepted_count = 0
    rejected_count = 0
    lock = threading.Lock()
    barrier = threading.Barrier(8)

    def worker(prefix):
        nonlocal accepted_count, rejected_count
        barrier.wait()
        for i in range(10):
            r, = svc.ingest_batch([make_event(f"{prefix}-{i}", source="src-billing")])
            with lock:
                if r.accepted:
                    accepted_count += 1
                else:
                    assert r.reason is RejectReason.CAPACITY_EXCEEDED
                    rejected_count += 1

    threads = [threading.Thread(target=worker, args=(f"w{k}",)) for k in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert accepted_count == 10
    assert rejected_count == 70
    assert svc.store.get_store("src-billing").count() == 10
    assert svc.store.get_store("src-billing").verify().ok
