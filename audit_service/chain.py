"""按来源维护的完整性链（哈希链）。

每条记录的 record_hash = SHA256(规范化(字段 + prev_hash))；
链首的 prev_hash 为 GENESIS_HASH。旧记录被保留策略淘汰后，
链会记住被淘汰最后一条记录的摘要作为锚点，使剩余链仍可确定性校验。
"""
from __future__ import annotations

from .hashing import canonical_json, digest_hex
from .models import StoredEvent, VerifyResult

GENESIS_HASH = "0" * 64


def compute_record_hash(
    event_id: str,
    source: str,
    event_type: str,
    timestamp_iso: str,
    sequence: int,
    payload: dict,
    prev_hash: str,
) -> str:
    """根据记录字段与前驱摘要计算当前记录摘要。"""
    document = {
        "event_id": event_id,
        "source": source,
        "event_type": event_type,
        "timestamp": timestamp_iso,
        "sequence": sequence,
        "payload": payload,
        "prev_hash": prev_hash,
    }
    return digest_hex(document)


class IntegrityChain:
    """单来源的哈希链。append / verify / truncate_prefix，线程安全由上层 SourceStore 保证。"""

    def __init__(self, source: str) -> None:
        self.source = source
        self._events: list[StoredEvent] = []
        # 已淘汰前缀的锚点：最后一条被淘汰记录的摘要与序号
        self._anchor_hash: str = GENESIS_HASH
        self._anchor_seq: int | None = None
        self.evicted_total: int = 0

    def head_hash(self) -> str:
        return self._events[-1].record_hash if self._events else self._anchor_hash

    def head_sequence(self) -> int | None:
        return self._events[-1].sequence if self._events else self._anchor_seq

    def append(self, event: StoredEvent) -> None:
        if event.source != self.source:
            raise ValueError(f"chain source mismatch: {event.source} != {self.source}")
        expected_prev = self.head_hash()
        if event.prev_hash != expected_prev:
            raise ValueError("prev_hash does not match chain head")
        expected_hash = compute_record_hash(
            event.event_id,
            event.source,
            event.event_type,
            event.timestamp.isoformat(),
            event.sequence,
            event.payload,
            event.prev_hash,
        )
        if event.record_hash != expected_hash:
            raise ValueError("record_hash mismatch on append")
        self._events.append(event)

    def get_all(self) -> list[StoredEvent]:
        return list(self._events)

    def truncate_prefix(self, count: int) -> list[StoredEvent]:
        """淘汰链首 count 条记录，并推进锚点。"""
        if count <= 0:
            return []
        if count > len(self._events):
            raise ValueError("truncate count exceeds chain length")
        removed = self._events[:count]
        self._anchor_hash = removed[-1].record_hash
        self._anchor_seq = removed[-1].sequence
        self._events = self._events[count:]
        self.evicted_total += count
        return removed

    def verify(self) -> VerifyResult:
        """重算全部记录摘要并检查前驱链接与序号单调性。"""
        prev_hash = self._anchor_hash
        prev_seq = self._anchor_seq
        for event in self._events:
            # 1) 序号必须严格单调且不重复
            if prev_seq is not None and event.sequence <= prev_seq:
                return self._fail(
                    event,
                    f"sequence not strictly increasing: {event.sequence} after {prev_seq}",
                )
            # 2) 前驱链接必须指向锚点/上一条记录
            if event.prev_hash != prev_hash:
                return self._fail(event, "prev_hash linkage broken")
            # 3) 记录摘要必须可由字段重算得到 —— 任何字段篡改都会在此暴露
            recomputed = compute_record_hash(
                event.event_id,
                event.source,
                event.event_type,
                event.timestamp.isoformat(),
                event.sequence,
                event.payload,
                event.prev_hash,
            )
            if recomputed != event.record_hash:
                return self._fail(event, "record_hash mismatch: data tampered or corrupted")
            prev_hash = event.record_hash
            prev_seq = event.sequence

        first_seq = self._events[0].sequence if self._events else (self._anchor_seq or 0) + 1 if self._anchor_seq is not None else None
        last_seq = self._events[-1].sequence if self._events else self._anchor_seq
        return VerifyResult(
            source=self.source,
            ok=True,
            checked=len(self._events),
            first_sequence=first_seq,
            last_sequence=last_seq,
            evicted_count=self.evicted_total,
            problem=(
                f"OK; {self.evicted_total} historical record(s) evicted before sequence "
                f"{(self._anchor_seq + 1) if self._anchor_seq is not None else 1}, "
                f"anchored at {self._anchor_hash[:12]}"
                if self.evicted_total
                else "OK"
            ),
        )

    def _fail(self, event: StoredEvent, problem: str) -> VerifyResult:
        return VerifyResult(
            source=self.source,
            ok=False,
            checked=len(self._events),
            first_sequence=self._events[0].sequence if self._events else None,
            last_sequence=self._events[-1].sequence if self._events else None,
            problem=problem,
            problem_event_id=event.event_id,
            evicted_count=self.evicted_total,
        )
