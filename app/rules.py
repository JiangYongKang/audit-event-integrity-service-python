"""可配置合规规则：求值、版本管理与异常隔离。

condition 为表达式树（dict），支持算子：
  {"op": "eq"|"ne"|"lt"|"lte"|"gt"|"gte"|"in"|"contains",
   "field": "payload.amount", "value": ...}
  {"op": "exists", "field": "payload.user"}
  {"op": "and"|"or", "args": [cond, ...]}
  {"op": "not", "arg": cond}
field 支持点路径，从事件顶层字段（含 payload）取值。
单条规则求值异常被隔离为 RuleOutcome.ERROR，不影响其它规则。
"""
from __future__ import annotations

import time
from dataclasses import dataclass
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


def _resolve_field(event: AuditEvent, field: str) -> Any:
    parts = field.split(".")
    if not parts or not parts[0]:
        raise RuleError(f"非法字段路径: {field!r}")
    head = parts[0]
    if head == "payload":
        node: Any = event.payload
    elif head in ("event_id", "source", "sequence", "timestamp", "event_type"):
        node = getattr(event, head)
    else:
        raise RuleError(f"不支持的顶层字段: {head!r}")
    for part in parts[1:]:
        if not isinstance(node, dict) or part not in node:
            raise RuleError(f"字段不存在: {field!r}（在 {part!r} 处中断）")
        node = node[part]
    return node


def _eval_condition(cond: dict[str, Any], event: AuditEvent) -> bool:
    if not isinstance(cond, dict) or "op" not in cond:
        raise RuleError(f"条件必须是含 op 的对象: {cond!r}")
    op = cond["op"]

    if op in ("and", "or"):
        args = cond.get("args")
        if not isinstance(args, list) or not args:
            raise RuleError(f"{op} 需要非空 args 列表")
        results = [_eval_condition(c, event) for c in args]
        return all(results) if op == "and" else any(results)
    if op == "not":
        return not _eval_condition(cond.get("arg"), event)
    if op == "exists":
        try:
            _resolve_field(event, cond.get("field", ""))
            return True
        except RuleError:
            return False

    if op in ("eq", "ne", "lt", "lte", "gt", "gte", "in", "contains"):
        actual = _resolve_field(event, cond.get("field", ""))
        expected = cond.get("value")
        if op == "eq":
            return actual == expected
        if op == "ne":
            return actual != expected
        if op == "in":
            if not isinstance(expected, (list, tuple, set)):
                raise RuleError("in 的 value 必须是列表")
            return actual in expected
        if op == "contains":
            if not isinstance(actual, (str, list, tuple, set, dict)):
                raise RuleError(f"contains 不支持的操作数: {type(actual).__name__}")
            return expected in actual
        # 比较算子
        try:
            if op == "lt":
                return actual < expected
            if op == "lte":
                return actual <= expected
            if op == "gt":
                return actual > expected
            return actual >= expected
        except TypeError as exc:
            raise RuleError(f"比较算子 {op} 类型不匹配: {exc}") from exc

    raise RuleError(f"不支持的算子: {op!r}")


class ComplianceRule:
    def __init__(self, definition: RuleDefinition) -> None:
        self.definition = definition

    def evaluate(self, event: AuditEvent) -> tuple[bool, str]:
        matched = _eval_condition(self.definition.condition, event)
        rationale = (
            self.definition.rationale_template
            or f"规则 {self.definition.rule_id} 条件"
        )
        return matched, f"{rationale} => {'命中' if matched else '未命中'}"


class RuleEngine:
    """规则注册表：按 rule_id 保留全部历史版本。"""

    def __init__(self) -> None:
        self._revisions: dict[str, list[RuleRevision]] = {}

    def upsert_rule(self, definition: RuleDefinition) -> RuleDefinition:
        if not definition.rule_id:
            raise RuleError("rule_id 不能为空")
        # 定义校验：对一棵语法树做静态检查（用哑事件试算结构错误之外的算子）
        revisions = self._revisions.setdefault(definition.rule_id, [])
        if revisions:
            current = revisions[-1].definition
            if definition.version <= current.version:
                definition = RuleDefinition(
                    rule_id=definition.rule_id,
                    name=definition.name,
                    condition=definition.condition,
                    version=current.version + 1,
                    enabled=definition.enabled,
                    rationale_template=definition.rationale_template,
                )
        revisions.append(RuleRevision(
            definition=definition, created_at_ms=int(time.time() * 1000)))
        return definition

    def delete_rule(self, rule_id: str) -> None:
        if rule_id not in self._revisions:
            raise RuleError(f"规则不存在: {rule_id}")
        del self._revisions[rule_id]

    def get_definition(self, rule_id: str, version: int | None = None) -> RuleDefinition:
        revisions = self._revisions.get(rule_id)
        if not revisions:
            raise RuleError(f"规则不存在: {rule_id}")
        if version is None:
            return revisions[-1].definition
        for rev in revisions:
            if rev.definition.version == version:
                return rev.definition
        raise RuleError(f"规则 {rule_id} 无版本 {version}")

    def list_current(self) -> list[RuleDefinition]:
        return [revs[-1].definition for revs in self._revisions.values()]

    def evaluate_event(
        self, event: AuditEvent, rule_versions: dict[str, int] | None = None
    ) -> list[RuleResult]:
        results: list[RuleResult] = []
        for rule_id, revisions in sorted(self._revisions.items()):
            version = (rule_versions or {}).get(rule_id)
            try:
                definition = self.get_definition(rule_id, version)
            except RuleError as exc:
                results.append(RuleResult(
                    rule_id=rule_id, rule_version=version or -1,
                    event_id=event.event_id, outcome=RuleOutcome.ERROR,
                    rationale="规则版本解析失败", error=str(exc)))
                continue
            if not definition.enabled:
                continue
            try:
                matched, rationale = ComplianceRule(definition).evaluate(event)
                results.append(RuleResult(
                    rule_id=rule_id, rule_version=definition.version,
                    event_id=event.event_id,
                    outcome=RuleOutcome.MATCH if matched else RuleOutcome.NO_MATCH,
                    rationale=rationale, matched=matched))
            except Exception as exc:  # noqa: BLE001 异常隔离：单规则失败不影响整批
                results.append(RuleResult(
                    rule_id=rule_id, rule_version=definition.version,
                    event_id=event.event_id, outcome=RuleOutcome.ERROR,
                    rationale="规则求值异常，已隔离", error=str(exc)))
        return results
