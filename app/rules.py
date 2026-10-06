"""可配置合规规则：求值、版本管理与异常隔离。

- condition 为声明式表达式树，支持算子：
  eq/ne/gt/gte/lt/lte/in/contains/exists（字段级），and/or/not（组合级）；
  字段路径支持点号下钻，如 "payload.amount"、"event_type"。
- 每次 upsert 生成新版本，历史版本保留（RuleRevision），
  可按版本取回定义以复现历史结论。
- 单条规则求值异常被隔离为 RuleOutcome.ERROR，不影响其它规则与整批评估。
"""
from __future__ import annotations

import threading
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


_FIELD_OPS = {"eq", "ne", "gt", "gte", "lt", "lte", "in", "contains", "exists"}
_COMBINATORS = {"and", "or", "not"}


def _resolve(event: AuditEvent, path: str) -> Any:
    """按点号路径从事件上取值；路径不存在抛 RuleError。"""
    node: Any = event
    for part in path.split("."):
        if isinstance(node, dict):
            if part not in node:
                raise RuleError(f"字段路径不存在: {path}")
            node = node[part]
        elif hasattr(node, part):
            node = getattr(node, part)
        else:
            raise RuleError(f"字段路径不存在: {path}")
    return node


def _eval_condition(cond: dict[str, Any], event: AuditEvent) -> bool:
    if not isinstance(cond, dict) or len(cond) != 1:
        raise RuleError(f"条件节点必须是单键对象: {cond!r}")
    (op, body), = cond.items()

    if op in ("and", "or"):
        if not isinstance(body, list) or not body:
            raise RuleError(f"{op} 需要非空条件列表")
        results = [_eval_condition(c, event) for c in body]
        return all(results) if op == "and" else any(results)
    if op == "not":
        return not _eval_condition(body, event)

    if op not in _FIELD_OPS:
        raise RuleError(f"不支持的算子: {op}")
    if not isinstance(body, dict) or "field" not in body:
        raise RuleError(f"算子 {op} 需要 {{'field': ..., 'value': ...}}")
    field = body["field"]
    if op == "exists":
        try:
            _resolve(event, field)
            return True
        except RuleError:
            return False
    expected = body.get("value")
    actual = _resolve(event, field)
    try:
        if op == "eq":
            return actual == expected
        if op == "ne":
            return actual != expected
        if op == "gt":
            return actual > expected
        if op == "gte":
            return actual >= expected
        if op == "lt":
            return actual < expected
        if op == "lte":
            return actual <= expected
        if op == "in":
            return actual in expected
        if op == "contains":
            return expected in actual
    except TypeError as exc:
        raise RuleError(f"算子 {op} 比较类型不匹配: {exc}") from exc
    raise RuleError(f"不支持的算子: {op}")


class ComplianceRule:
    def __init__(self, definition: RuleDefinition) -> None:
        self.definition = definition

    def evaluate(self, event: AuditEvent) -> tuple[bool, str]:
        matched = _eval_condition(self.definition.condition, event)
        rationale = (
            self.definition.rationale_template
            or f"规则 {self.definition.rule_id} "
               f"{'命中' if matched else '未命中'}"
        )
        return matched, rationale


class RuleEngine:
    def __init__(self) -> None:
        self._current: dict[str, RuleDefinition] = {}
        self._history: dict[str, list[RuleRevision]] = {}
        self._lock = threading.Lock()

    def upsert_rule(self, definition: RuleDefinition) -> RuleDefinition:
        with self._lock:
            history = self._history.setdefault(definition.rule_id, [])
            version = (history[-1].definition.version + 1) if history else 1
            stored = RuleDefinition(
                rule_id=definition.rule_id,
                name=definition.name,
                condition=definition.condition,
                version=version,
                enabled=definition.enabled,
                rationale_template=definition.rationale_template,
            )
            history.append(RuleRevision(
                definition=stored, created_at_ms=int(time.time() * 1000)))
            self._current[definition.rule_id] = stored
            return stored

    def delete_rule(self, rule_id: str) -> None:
        with self._lock:
            self._current.pop(rule_id, None)
            # 历史版本保留，保证历史结论可复现

    def get_definition(self, rule_id: str,
                       version: int | None = None) -> RuleDefinition:
        if version is None:
            try:
                return self._current[rule_id]
            except KeyError:
                raise RuleError(f"规则不存在: {rule_id}") from None
        for rev in self._history.get(rule_id, []):
            if rev.definition.version == version:
                return rev.definition
        raise RuleError(f"规则 {rule_id} 无版本 {version}")

    def list_current(self) -> list[RuleDefinition]:
        return [self._current[k] for k in sorted(self._current)]

    def evaluate_event(
        self, event: AuditEvent,
        rule_versions: dict[str, int] | None = None,
    ) -> list[RuleResult]:
        results: list[RuleResult] = []
        for definition in self.list_current():
            if not definition.enabled:
                continue
            if rule_versions and definition.rule_id in rule_versions:
                try:
                    definition = self.get_definition(
                        definition.rule_id, rule_versions[definition.rule_id])
                except RuleError as exc:
                    results.append(RuleResult(
                        rule_id=definition.rule_id,
                        rule_version=definition.version,
                        event_id=event.event_id,
                        outcome=RuleOutcome.ERROR,
                        rationale="指定版本不存在，规则被隔离",
                        error=str(exc),
                    ))
                    continue
            try:
                matched, rationale = ComplianceRule(definition).evaluate(event)
                results.append(RuleResult(
                    rule_id=definition.rule_id,
                    rule_version=definition.version,
                    event_id=event.event_id,
                    outcome=RuleOutcome.MATCH if matched else RuleOutcome.NO_MATCH,
                    rationale=rationale,
                    matched=matched,
                ))
            except Exception as exc:  # noqa: BLE001 异常隔离：不影响其它规则
                results.append(RuleResult(
                    rule_id=definition.rule_id,
                    rule_version=definition.version,
                    event_id=event.event_id,
                    outcome=RuleOutcome.ERROR,
                    rationale="规则求值异常，已隔离",
                    error=str(exc),
                ))
        return results
