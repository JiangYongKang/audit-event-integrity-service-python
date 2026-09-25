"""顺序保证、哈希链篡改检测、保留淘汰后的确定性校验结论。"""
from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone

from audit_service.models import EventInput, RetentionPolicy, RejectReason
from audit_service.service import AuditService
from tests.factories import make_event


def test_client_sequence_must_be_monotonic_and_unique(service, helpers):
    ok1, = service.ingest_batch([make_event("s-1", sequence=1)])
    ok2, = service.ingest_batch([make_event("s-2", sequence=2)])
    assert ok1.accepted and ok2.accepted

    # 重复序号
    dup, = service.ingest_batch([make_event("s-dup", sequence=2)])
    # 回退/跳号
    back, = service.ingest_batch([make_event("s-back", sequence=1)])
    jump, = service.ingest_batch([make_event("s-jump", sequence=9)])
    helpers.log_ingestion([dup, back, jump])
    assert {r.reason for r in (dup, back, jump)} == {RejectReason.SEQUENCE_CONFLICT}

    # 正确的下一个序号仍可继续
    ok3, = service.ingest_batch([make_event("s-3", sequence=3)])
    assert ok3.accepted and ok3.sequence == 3
    helpers.log_verify(service.verify_all())
    assert all(v.ok for v in service.verify_all())


def test_server_assigns_sequences_under_concurrent_writes():
    """并发写入时同来源序号必须单调且不重复。"""
    import threading

    svc = AuditService(known_sources={"c1"})

    def worker(start: int):
        for i in range(start, start + 50):
            results = svc.ingest_batch([
                make_event(f"evt-{i}", source="c1", payload={"n": i})
            ])
            assert results[0].accepted, results[0]

    threads = [threading.Thread(target=worker, args=(i * 50,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    store = svc.store.get_store("c1")
    assert store.count() == 200
    events = store._chain.get_all()
    sequences = [e.sequence for e in events]
    assert sequences == list(range(1, 201))  # 单调、连续、不重复
    assert len({e.event_id for e in events}) == 200
    assert store.verify().ok


def test_concurrent_replay_of_same_event_id_is_idempotent():
    """同一事件并发重放只能落一条记录。"""
    import threading

    svc = AuditService(known_sources={"c2"})
    barrier = threading.Barrier(8)
    outcomes: list[bool] = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        r, = svc.ingest_batch([make_event("hot-event", source="c2", payload={"a": 1})])
        with lock:
            outcomes.append(r.accepted)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    store = svc.store.get_store("c2")
    assert store.count() == 1
    assert all(outcomes)  # 全部视为成功（幂等），但只有一条记录
    assert store.verify().ok


def test_payload_tampering_is_detected(service, helpers):
    service.ingest_batch([make_event("h-1"), make_event("h-2"), make_event("h-3")])
    assert all(v.ok for v in service.verify_all())

    # 直接在存储层篡改一条记录的 payload（模拟磁盘/外部篡改）
    store = service.store.get_store("src-billing")
    target = store._chain.get_all()[1]
    store._chain._events[1] = dataclasses.replace(target, payload={"amount": 666})

    results = service.verify_all()
    helpers.log_verify(results)
    (result,) = results
    assert not result.ok
    assert result.problem_event_id == "h-2"
    assert "tampered" in result.problem


def test_hash_linkage_break_is_detected(service, helpers):
    service.ingest_batch([make_event("l-1"), make_event("l-2")])
    store = service.store.get_store("src-billing")
    events = store._chain.get_all()
    # 交换两条记录顺序 -> 前驱链接断裂
    store._chain._events[0], store._chain._events[1] = events[1], events[0]

    result, = service.verify_all()
    helpers.log_verify([result])
    assert not result.ok
    assert "linkage broken" in result.problem


def test_undeclared_record_insertion_is_detected(service):
    """插入一条未通过正常摄取的伪造记录会破坏链。"""
    service.ingest_batch([make_event("i-1")])
    store = service.store.get_store("src-billing")
    forged = make_event("i-forged", sequence=2)
    r, = service.ingest_batch([forged])
    assert r.accepted  # 正常路径允许
    # 再手工插入一条 prev_hash 不指向链首的记录
    from audit_service.chain import compute_record_hash
    from audit_service.models import StoredEvent

    ts = datetime(2026, 9, 25, 11, 0, tzinfo=timezone.utc)
    fake_hash = compute_record_hash(
        "i-injected", "src-billing", "payment", ts.isoformat(), 3, {}, "f" * 64
    )
    injected = StoredEvent(
        "i-injected", "src-billing", "payment", ts, 3, {}, "f" * 64, fake_hash
    )
    store._chain._events.append(injected)
    result, = service.verify_all()
    assert not result.ok and result.problem_event_id == "i-injected"


def test_verification_after_eviction_is_determinate_and_explained(helpers):
    now = datetime(2026, 9, 25, 12, 0, 0, tzinfo=timezone.utc)
    svc = AuditService(
        retention=RetentionPolicy(max_age_seconds=60),
        known_sources={"src-billing"},
    )

    def at(offset_seconds):
        return (now - timedelta(seconds=offset_seconds)).isoformat()

    svc.ingest_batch([
        make_event("old-1", timestamp=at(300)),
        make_event("old-2", timestamp=at(200)),
        make_event("old-3", timestamp=at(120)),
    ], now=now)
    # 摄取一条新事件，触发 3 条旧事件淘汰；淘汰后新链仍可校验
    results = svc.ingest_batch([make_event("new-1", timestamp=now.isoformat())], now=now)
    assert results[0].accepted
    store = svc.store.get_store("src-billing")
    assert store.count() == 1
    verify, = svc.verify_all()
    helpers.log_verify([verify])
    assert verify.ok
    assert verify.checked == 1 and verify.evicted_count == 3
    assert verify.last_sequence == 4  # 序号跨淘汰继续单调，不复用
    assert "anchored" in verify.problem

    # 篡改唯一剩余记录仍必须被检出
    import dataclasses as dc
    only = store._chain.get_all()[0]
    store._chain._events[0] = dc.replace(only, event_type="elevated")
    verify2, = svc.verify_all()
    assert not verify2.ok and verify2.problem_event_id == "new-1"
