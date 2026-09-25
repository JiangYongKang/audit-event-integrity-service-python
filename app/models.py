"""领域模型：审计事件、摄取结果、完整性校验、规则判定。

本模块只包含类型定义，不包含业务逻辑。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class RejectReason(str, Enum):
    """摄取被拒绝的可区分原因。"""

    MISSING_FIELD = "missing_field"                # 必填字段缺失
    MALFORMED_TIMESTAMP = "malformed_timestamp"    # 时间戳非法/不可解析
    TIMESTAMP_OUT_OF_RANGE = "timestamp_out_of_range"
    SOURCE_MISMATCH = "source_mismatch"            # 来源与声明不符
    DUPLICATE_EVENT_ID = "duplicate_event_id"      # 事件标识重复（幂等重放）
    DUPLICATE_SEQUENCE = "duplicate_sequence"      # 同来源序号重复
    SEQUENCE_OUT_OF_ORDER = "sequence_out_of_order"  # 同来源序号倒退
    CAPACITY_EXCEEDED = "capacity_exceeded"        # 容量上限（背压，拒绝新事件）


class IngestStatus(str, Enum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    DUPLICATE = "duplicate"   # 幂等重放：原已接收，非重复记录


class ChainStatus(str, Enum):
    """完整性链校验结论。"""

    VALID = "valid"
    BROKEN_HASH = "broken_hash"                 # 事件内容被篡改或删除
    BROKEN_LINK = "broken_link"                 # 链式指针断裂（插入/删除）
    BROKEN_ANCHOR = "broken_anchor"             # 锚点记录被篡改/重写
    EMPTY = "empty"


class RuleOutcome(str, Enum):
    MATCH = "match"
    NO_MATCH = "no_match"
    ERROR = "error"   # 规则异常，被隔离，不影响其它规则与整批评估


@dataclass(frozen=True)
class AuditEvent:
    """规范化后的审计事件（不可变）。"""

    event_id: str
    source: str
    sequence: int
    timestamp: int          # 统一为 Unix epoch 毫秒
    event_type: str
    payload: dict[str, Any] = field(default_factory=dict)

    def to_canonical_json(self) -> bytes:
        """确定性序列化：字段固定 + 键排序，作为哈希输入。"""
        import json
        return json.dumps(
            {
                "event_id": self.event_id,
                "source": self.source,
                "sequence": self.sequence,
                "timestamp": self.timestamp,
                "event_type": self.event_type,
                "payload": self.payload,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")


@dataclass(frozen=True)
class IngestResult:
    status: IngestStatus
    event_id: str
    source: str | None = None
    sequence: int | None = None
    reason: RejectReason | None = None
    detail: str | None = None
    existing_sequence: int | None = None  # 幂等重放时指向原记录


@dataclass(frozen=True)
class ChainEntry:
    """完整性链上的一个链接（每个已接收事件一条）。"""

    event_id: str
    sequence: int
    event_hash: str   # H(canonical event)
    entry_hash: str   # H(prev_entry_hash | event_hash | sequence)


@dataclass(frozen=True)
class VerificationResult:
    source: str
    status: ChainStatus
    message: str
    verified_count: int = 0
    head_entry_hash: str | None = None
    anchor_count: int = 0
    chain_tip: str | None = None  # 链头（最新）entry_hash，独立锚点记录用


@dataclass(frozen=True)
class RuleResult:
    rule_id: str
    rule_version: int
    event_id: str
    outcome: RuleOutcome
    rationale: str                         # 可解释判定依据
    matched: bool = False
    error: str | None = None               # outcome == ERROR 时的异常信息


@dataclass(frozen=True)
class EventEvaluation:
    event_id: str
    rule_results: tuple[RuleResult, ...]


@dataclass(frozen=True)
class EvaluationReport:
    evaluated_at: int
    rule_versions: dict[str, int]
    results: tuple[EventEvaluation, ...]
