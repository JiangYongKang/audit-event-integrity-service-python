"""保留策略：按时间与容量双重上限淘汰旧数据。

语义：
- max_age_ms：超过年龄上限的最旧事件（链首前缀）被淘汰；
- max_events_per_source：在窗事件数超过容量时，继续从最旧端淘汰至容量；
- 摄取路径不做静默淘汰：窗口满时新事件以 CAPACITY_EXCEEDED 被拒绝（背压），
  由调用方显式执行保留策略释放空间后重试。
淘汰是确定性的前缀裁剪，配合 IntegrityChain 的检查点，校验结论仍然可复现。
"""
from __future__ import annotations

from dataclasses import dataclass

from app.models import AuditEvent


@dataclass(frozen=True)
class RetentionConfig:
    max_events_per_source: int
    max_age_ms: int | None = None

    def __post_init__(self) -> None:
        if self.max_events_per_source <= 0:
            raise ValueError("max_events_per_source 必须为正整数")
        if self.max_age_ms is not None and self.max_age_ms <= 0:
            raise ValueError("max_age_ms 必须为正数或 None")


class RetentionPolicy:
    def __init__(self, config: RetentionConfig) -> None:
        self.config = config

    def choose_evictions(self, events: list[AuditEvent],
                         now_ms: int) -> list[str]:
        """返回需要从链首淘汰的事件 event_id 前缀列表。"""
        n = len(events)
        cut = 0
        # 1) 时间上限：链首连续的过期事件
        if self.config.max_age_ms is not None:
            cutoff = now_ms - self.config.max_age_ms
            while cut < n and events[cut].timestamp < cutoff:
                cut += 1
        # 2) 容量上限：仍超出容量则继续淘汰最旧事件
        overflow = (n - cut) - self.config.max_events_per_source
        if overflow > 0:
            cut += overflow
        return [e.event_id for e in events[:cut]]
