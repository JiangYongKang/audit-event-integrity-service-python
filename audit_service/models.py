"""审计事件、摄取结果与规则的数据模型（类型定义层）。"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class RejectReason(str, Enum):
    """事件被拒绝的可区分原因。"""

    MISSING_FIELD = "MISSING_FIELD"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    SOURCE_MISMATCH = "SOURCE_MISMATCH"
    UNKNOWN_SOURCE = "UNKNOWN_SOURCE"
    DUPLICATE_EVENT = "DUPLICATE_EVENT"
    SEQUENCE_CONFLICT = "SEQUENCE_CONFLICT"
    CAPACITY_EXCEEDED = "CAPACITY_EXCEEDED"


@dataclass(frozen=True)
class EventInput:
    """入站事件的原始描述。

    declared_source 为投递通道声明的来源（信封），source 为事件体内嵌入的来源；
    二者不一致将以 SOURCE_MISMATCH 拒绝。
    """

    event_id: str
    declared_source: str
    source: str
    event_type: str
    timestamp: str
    payload: dict[str, Any]
    sequence: int | None = None


@dataclass(frozen=True)
class StoredEvent:
    """规范化后落库的事件记录。"""

    event_id: str
    source: str
    event_type: str
    timestamp: datetime
    sequence: int
    payload: dict[str, Any]
    prev_hash: str
    record_hash: str


@dataclass(frozen=True)
class IngestionResult:
    """单条事件摄取的处理结果。"""

    accepted: bool
    event_id: str
    source: str
    sequence: int | None = None
    reason: RejectReason | None = None
    detail: str = ""
    duplicate: bool = False
    record_hash: str = ""


@dataclass
class VerifyResult:
    """完整性链校验结论。"""

    source: str
    ok: bool
    checked: int = 0
    first_sequence: int | None = None
    last_sequence: int | None = None
    problem: str = ""
    problem_event_id: str = ""
    evicted_count: int = 0


@dataclass(frozen=True)
class RuleSpec:
    """规则定义；version 单调递增，历史版本保留以供结论复现。"""

    rule_id: str
    rule_type: str
    params: dict[str, Any]
    version: int
    enabled: bool = True


@dataclass(frozen=True)
class RuleResult:
    """单条规则对单条事件的判定结论。"""

    rule_id: str
    rule_version: int
    event_id: str
    matched: bool
    status: str  # OK / RULE_ERROR
    rationale: str = ""
    error: str = ""


@dataclass
class RetentionPolicy:
    """保留策略：时间与容量双重上限。"""

    max_age_seconds: float | None = None
    max_count: int | None = None
