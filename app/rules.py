"""可配置合规规则：求值、版本管理与异常隔离（骨架）。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.models import AuditEvent, RuleOutcome, RuleResult


@dataclass(frozen=True)
class RuleDefinition:
    """声明式规则定义；condition 为受支持算子组成的表达式树。"""

    rule_id: str
    name: str
    condition: dict[str, Any]
    version: int = 1
    enabled: bool = True
    rationale_template: str = ""


@dataclass
class RuleRevision:
    """规则的一次历史版本（保证历史结论可复现/可重算）。"""

    definition: RuleDefinition
    created_at_ms: int


class RuleError(Exception):
    """规则定义或求值错误。"""


class ComplianceRule:
    def __init__(self, definition: RuleDefinition) -> None:
        self.definition = definition

    def evaluate(self, event: AuditEvent) -> tuple[bool, str]:
        raise NotImplementedError


class RuleEngine:
    def __init__(self) -> None:
        raise NotImplementedError

    def upsert_rule(self, definition: RuleDefinition) -> RuleDefinition:
        raise NotImplementedError

    def delete_rule(self, rule_id: str) -> None:
        raise NotImplementedError

    def get_definition(self, rule_id: str, version: int | None = None) -> RuleDefinition:
        raise NotImplementedError

    def list_current(self) -> list[RuleDefinition]:
        raise NotImplementedError

    def evaluate_event(self, event: AuditEvent,
                       rule_versions: dict[str, int] | None = None) -> list[RuleResult]:
        raise NotImplementedError
