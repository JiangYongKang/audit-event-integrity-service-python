"""按时间与容量双重上限的保留策略。

语义（与文档一致）：
- 时间上限 max_age_seconds：在摄取/巡检时主动淘汰已过期的最旧记录（链首的一个连续前缀），
  淘汰后哈希链推进锚点，校验结论仍确定且可解释。
- 容量上限 max_count：硬上限（背压语义）。时间淘汰后若记录数已达上限，新事件以
  RejectReason.CAPACITY_EXCEEDED 被明确拒绝，绝不静默丢弃。
因此本模块只计算"时间到期"导致的淘汰前缀；容量判定发生在摄取路径。
"""
from __future__ import annotations

from datetime import datetime, timedelta

from .models import RetentionPolicy, StoredEvent


def _as_datetime(now) -> datetime:
    if now is None:
        return datetime.now().astimezone()
    if isinstance(now, datetime):
        return now
    raise TypeError("now must be a datetime or None")


def select_evicted(
    events: list[StoredEvent], policy: RetentionPolicy, now
) -> list[StoredEvent]:
    """返回应当被时间策略淘汰的事件：自链首起、连续的已过期前缀。

    遇到第一条未过期记录即停止，保证淘汰集合永远是链首连续前缀，
    与哈希链锚点推进方式一致。
    """
    if policy.max_age_seconds is None or not events:
        return []
    current = _as_datetime(now)
    cutoff = current - timedelta(seconds=float(policy.max_age_seconds))
    evicted: list[StoredEvent] = []
    for event in events:
        if event.timestamp < cutoff:
            evicted.append(event)
        else:
            break
    return evicted
