"""本地可运行的端到端演示：摄取/幂等/拒绝原因/篡改检测/规则异常隔离。

运行：uv run python examples/demo.py
"""
from __future__ import annotations

import dataclasses
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from audit_service.models import EventInput, RetentionPolicy
from audit_service.service import AuditService

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("demo")


def show(title: str) -> None:
    log.info("=" * 20 + " %s " + "=" * 20, title)


def main() -> None:
    svc = AuditService(
        retention=RetentionPolicy(max_age_seconds=300, max_count=5),
        known_sources={"src-billing", "src-iam"},
    )

    show("1. 批量摄取（含重复投递、缺字段、坏时间戳、来源不符）")
    events = [
        EventInput("evt-001", "src-billing", "src-billing", "payment",
                   "2026-09-25T10:00:00+00:00", {"amount": 5000}),
        EventInput("evt-001", "src-billing", "src-billing", "payment",
                   "2026-09-25T10:00:00+00:00", {"amount": 5000}),  # 幂等重放
        EventInput("", "src-billing", "src-billing", "payment",
                   "2026-09-25T10:01:00+00:00", {}),  # 缺标识
        EventInput("evt-003", "src-billing", "src-billing", "payment",
                   "not-a-timestamp", {}),  # 时间戳非法
        EventInput("evt-004", "src-iam", "src-billing", "payment",
                   "2026-09-25T10:02:00+00:00", {}),  # 来源与声明不符
        EventInput("evt-005", "src-ghost", "src-ghost", "payment",
                   "2026-09-25T10:03:00+00:00", {}),  # 未知来源
    ]
    for r in svc.ingest_batch(events):
        if r.accepted:
            log.info("event_id=%-10s ACCEPTED seq=%s duplicate=%s",
                     r.event_id, r.sequence, r.duplicate)
        else:
            log.info("event_id=%-10s REJECTED reason=%s detail=%s",
                     r.event_id or "<empty>", r.reason.value, r.detail)

    show("2. 完整性校验（正常）")
    for v in svc.verify_all():
        log.info("source=%s ok=%s %s", v.source, v.ok, v.problem)

    show("3. 篡改检测：直接改写一条已落库记录")
    store = svc.store.get_store("src-billing")
    target = store._chain.get_all()[0]
    store._chain._events[0] = dataclasses.replace(target, payload={"amount": 999999})
    for v in svc.verify_all():
        log.info("source=%s ok=%s problem_event=%s problem=%s",
                 v.source, v.ok, v.problem_event_id, v.problem)
    # 恢复后重新校验
    store._chain._events[0] = target
    assert all(v.ok for v in svc.verify_all())

    show("4. 保留策略：时间淘汰后链仍可校验（锚点推进）")
    now = datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc)
    old = [
        EventInput("old-1", "src-iam", "src-iam", "login",
                   (now - timedelta(seconds=600)).isoformat(), {}),
        EventInput("old-2", "src-iam", "src-iam", "login",
                   (now - timedelta(seconds=500)).isoformat(), {}),
    ]
    svc.ingest_batch(old, now=now)
    svc.ingest_batch([EventInput("new-1", "src-iam", "src-iam", "login",
                                 now.isoformat(), {})], now=now)
    for v in svc.verify_all():
        log.info("source=%s ok=%s checked=%s evicted=%s %s",
                 v.source, v.ok, v.checked, v.evicted_count, v.problem)

    show("5. 规则评估：命中解释 + 单条异常隔离")
    svc.upsert_rule("large-pay", "payload_threshold", {"path": "amount", "min": 1000})
    svc.upsert_rule("broken", "payload_threshold", {"path": "missing.path", "min": 1})
    evaluated = svc.evaluate([events[0]])
    for r in evaluated[0]:
        if r.status == "OK":
            log.info("event_id=%s rule=%s v%s matched=%s rationale=%s",
                     r.event_id, r.rule_id, r.rule_version, r.matched, r.rationale)
        else:
            log.info("event_id=%s rule=%s v%s STATUS=%s error=%s",
                     r.event_id, r.rule_id, r.rule_version, r.status, r.error)

    show("6. 容量背压")
    tiny = AuditService(retention=RetentionPolicy(max_count=1), known_sources={"src-billing"})
    tiny.ingest_batch([EventInput("only", "src-billing", "src-billing", "t",
                                  "2026-09-25T10:00:00+00:00", {})])
    r, = tiny.ingest_batch([EventInput("over", "src-billing", "src-billing", "t",
                                       "2026-09-25T10:00:01+00:00", {})])
    log.info("event_id=over accepted=%s reason=%s", r.accepted,
             r.reason.value if r.reason else None)
    log.info("演示完成")


if __name__ == "__main__":
    main()
