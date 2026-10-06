"""并发：同一标识的修复重投只成功一次；来源内序号单调不重复。"""
import logging
import threading
import time
from collections import Counter

from app.models import ChainStatus, IngestStatus, RejectReason
from tests.conftest import make_event as ev

log = logging.getLogger("test.concurrent")


def _run_concurrently(fn, n):
    barrier = threading.Barrier(n)
    results = [None] * n

    def worker(i):
        barrier.wait()
        results[i] = fn(i)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


def test_concurrent_repair_same_id_succeeds_once(service):
    bad = ev("cc-1", 0)
    bad["timestamp"] = "bad"
    r0 = service.ingest_batch([bad])[0]
    log.info("event_id=cc-1 首次 -> %s/%s", r0.status, r0.reason)

    n = 16
    results = _run_concurrently(
        lambda i: service.resubmit("s1", "cc-1", ev("cc-1", 0)), n)
    statuses = Counter(r.status for r in results)
    log.info("并发修复结果分布: %s", {s.value: c for s, c in statuses.items()})
    # 同一标识的修复只成功一次，其余为幂等重放
    assert statuses[IngestStatus.REPAIRED] == 1
    assert statuses[IngestStatus.DUPLICATE] == n - 1
    assert service.store.count_for_source("s1") == 1
    rec = service.get_rejection("s1", "cc-1")
    log.info("留档: resolved=%s attempts=%d", rec.resolved, rec.attempts)
    assert rec.resolved is True
    assert rec.attempts == 1   # 修复成功后不再有新拒绝写入


def test_concurrent_repair_with_different_fixes(service):
    bad = ev("cc-2", 0)
    bad["timestamp"] = "bad"
    service.ingest_batch([bad])

    n = 8
    results = _run_concurrently(
        lambda i: service.resubmit(
            "s1", "cc-2", ev("cc-2", 0, payload={"attempt": i})), n)
    statuses = Counter(r.status for r in results)
    log.info("不同修正内容并发: %s", {s.value: c for s, c in statuses.items()})
    # 只有一个修正版本被接收，其余按冲突拒绝（不得绕过冲突语义）
    assert statuses[IngestStatus.REPAIRED] == 1
    assert statuses[IngestStatus.REJECTED] == n - 1
    for r in results:
        if r.status == IngestStatus.REJECTED:
            assert r.reason == RejectReason.DUPLICATE_EVENT_ID
    assert service.store.count_for_source("s1") == 1


def test_concurrent_ingest_keeps_sequences_monotonic(service):
    """并发下乱序到达会被拒绝，客户端重试后链上序号仍单调连续不重复。"""
    n = 32

    def submit_with_retry(i):
        event = ev(f"cc-3-{i}", i)
        for _ in range(n * 50):
            r = service.ingest_batch([event])[0]
            if r.status in (IngestStatus.ACCEPTED, IngestStatus.REPAIRED):
                return r
            assert r.reason == RejectReason.SEQUENCE_OUT_OF_ORDER
            time.sleep(0.001)   # 让出调度，等待前序事件落库
        raise AssertionError(f"event_id=cc-3-{i} 重试耗尽")

    results = _run_concurrently(submit_with_retry, n)
    log.info("并发摄取: 全部 %d 条最终接收", len(results))
    seqs = [e.sequence for e in service.store.get_chain("s1")]
    log.info("链上序号: %s", seqs)
    assert seqs == list(range(n))   # 单调、连续、不重复
    v = service.verify_source("s1")
    log.info("完整性: %s", v.message)
    assert v.status == ChainStatus.VALID
