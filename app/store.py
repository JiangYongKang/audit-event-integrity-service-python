"""事件与完整性链的内存存储层。

并发约定：AuditService 对每个来源持有一把锁，写入路径在锁内调用本类，
因此本类不再自行加锁；读取方法为无锁只读，供校验与评估使用。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field

from app.models import AuditEvent, ChainEntry


@dataclass
class _SourceState:
    # chain 按写入顺序（即 sequence 单调顺序）保存
    chain: list[ChainEntry] = field(default_factory=list)
    # 每追加一个 entry，就把其 entry_hash 追加到 anchors（只增不改）
    anchors: list[str] = field(default_factory=list)
    # 链尾锚点：淘汰旧数据后指向"仍可校验窗口"的前驱；初始为 None（创世）
    checkpoint_tail: str | None = None
    # 已淘汰的事件数量（用于容量统计与可解释性）
    evicted_count: int = 0
    # 已接收的最大序号（淘汰不清零）：来源内序号严格递增的判定基准
    last_sequence: int | None = None


class EventStore:
    """事件主存储 + 完整性链状态。"""

    def __init__(self) -> None:
        self._events: dict[str, AuditEvent] = {}
        self._sources: dict[str, _SourceState] = {}
        # event_id -> source，便于拒绝重复与定位
        self._event_source: dict[str, str] = {}
        # 监控/调试用
        self.lock = threading.Lock()

    def _state(self, source: str) -> _SourceState:
        return self._sources.setdefault(source, _SourceState())

    # ---- 写入 ----
    def put_event(self, event: AuditEvent) -> None:
        self._events[event.event_id] = event
        self._event_source[event.event_id] = event.source

    def append_entry(self, source: str, entry: ChainEntry) -> None:
        state = self._state(source)
        state.chain.append(entry)
        state.anchors.append(entry.entry_hash)
        state.last_sequence = entry.sequence

    def truncate_prefix(self, source: str, count: int,
                        new_tail: str | None) -> None:
        """淘汰链首 count 个链接，并记录新的链尾锚点。"""
        state = self._state(source)
        removed = state.chain[:count]
        del state.chain[:count]
        state.checkpoint_tail = new_tail
        state.evicted_count += len(removed)

    # ---- 读取 ----
    def get_event(self, event_id: str) -> AuditEvent | None:
        return self._events.get(event_id)

    def event_source(self, event_id: str) -> str | None:
        return self._event_source.get(event_id)

    def sources(self) -> list[str]:
        return sorted(self._sources.keys())

    def get_events_by_source(self, source: str) -> list[AuditEvent]:
        state = self._state(source)
        return [self._events[e.event_id] for e in state.chain]

    def get_chain(self, source: str) -> list[ChainEntry]:
        return list(self._state(source).chain)

    def get_anchors(self, source: str) -> list[str]:
        return list(self._state(source).anchors)

    def get_checkpoint_tail(self, source: str) -> str | None:
        return self._state(source).checkpoint_tail

    def get_evicted_count(self, source: str) -> int:
        return self._state(source).evicted_count

    def get_last_sequence(self, source: str) -> int | None:
        return self._state(source).last_sequence

    def count_for_source(self, source: str) -> int:
        return len(self._state(source).chain)
