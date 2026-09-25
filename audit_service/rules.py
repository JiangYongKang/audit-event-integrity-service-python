"""可配置合规规则：评估器、单条异常隔离与版本化注册表。

规则类型（rule_type）：
- event_type_equals   params: {"event_type": str}
- source_equals       params: {"source": str}
- payload_equals      params: {"path": str, "value": Any}
- payload_threshold   params: {"path": str, "min": number}  （命中：数值 >= min）

单条规则评估抛出的任何异常都会被隔离为 status=RULE_ERROR 的 RuleResult，
不影响同批其他规则/事件。规则每次 upsert 产生新版本，历史版本永久保留，
判定结果中带 rule_version，可按版本取规则重算，保证历史结论可复现。
"""
from __future__ import annotations

from typing import Any, Callable

from .errors import RuleNotFoundError
from .models import EventInput, RuleResult, RuleSpec

STATUS_OK = "OK"
STATUS_ERROR = "RULE_ERROR"


def _payload_path(payload: dict[str, Any], path: str) -> Any:
    """以点号路径读取嵌套 payload，例如 'user.role'。"""
    current: Any = payload
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            raise KeyError(f"payload path not found: {path}")
        current = current[part]
    return current


def _eval_event_type_equals(spec: RuleSpec, event: EventInput) -> tuple[bool, str]:
    expected = spec.params["event_type"]
    actual = event.event_type
    matched = actual == expected
    return matched, f"event_type == {expected!r}: actual={actual!r} -> {'MATCH' if matched else 'NO_MATCH'}"


def _eval_source_equals(spec: RuleSpec, event: EventInput) -> tuple[bool, str]:
    expected = spec.params["source"]
    actual = event.source
    matched = actual == expected
    return matched, f"source == {expected!r}: actual={actual!r} -> {'MATCH' if matched else 'NO_MATCH'}"


def _eval_payload_equals(spec: RuleSpec, event: EventInput) -> tuple[bool, str]:
    path = spec.params["path"]
    expected = spec.params["value"]
    actual = _payload_path(event.payload, path)
    matched = actual == expected
    return matched, f"payload.{path} == {expected!r}: actual={actual!r} -> {'MATCH' if matched else 'NO_MATCH'}"


def _eval_payload_threshold(spec: RuleSpec, event: EventInput) -> tuple[bool, str]:
    path = spec.params["path"]
    minimum = spec.params["min"]
    actual = _payload_path(event.payload, path)
    if not isinstance(actual, (int, float)) or isinstance(actual, bool):
        raise TypeError(f"payload.{path} is not numeric: {actual!r}")
    matched = actual >= minimum
    return matched, f"payload.{path}={actual!r} >= min={minimum!r} -> {'MATCH' if matched else 'NO_MATCH'}"


_EVALUATORS: dict[str, Callable[[RuleSpec, EventInput], tuple[bool, str]]] = {
    "event_type_equals": _eval_event_type_equals,
    "source_equals": _eval_source_equals,
    "payload_equals": _eval_payload_equals,
    "payload_threshold": _eval_payload_threshold,
}


class RuleEvaluator:
    """按 rule_type 分发评估；任何异常被隔离为 RULE_ERROR，不向上抛出。"""

    def evaluate(self, spec: RuleSpec, event: EventInput) -> RuleResult:
        evaluator = _EVALUATORS.get(spec.rule_type)
        if evaluator is None:
            return RuleResult(
                rule_id=spec.rule_id,
                rule_version=spec.version,
                event_id=event.event_id,
                matched=False,
                status=STATUS_ERROR,
                error=f"unknown rule_type: {spec.rule_type!r}",
            )
        try:
            matched, rationale = evaluator(spec, event)
        except Exception as exc:  # 隔离单条规则的异常
            return RuleResult(
                rule_id=spec.rule_id,
                rule_version=spec.version,
                event_id=event.event_id,
                matched=False,
                status=STATUS_ERROR,
                rationale=f"rule failed on event {event.event_id}",
                error=f"{type(exc).__name__}: {exc}",
            )
        return RuleResult(
            rule_id=spec.rule_id,
            rule_version=spec.version,
            event_id=event.event_id,
            matched=matched,
            status=STATUS_OK,
            rationale=rationale,
        )


class RuleRegistry:
    """规则增删改：每次 upsert 产生单调递增的新版本，历史版本保留。"""

    def __init__(self) -> None:
        self._history: dict[str, list[RuleSpec]] = {}

    def upsert(
        self, rule_id: str, rule_type: str, params: dict, enabled: bool = True
    ) -> RuleSpec:
        versions = self._history.setdefault(rule_id, [])
        if versions and not versions[-1].enabled:
            raise RuleNotFoundError(f"rule {rule_id!r} was deleted; create under a new id")
        spec = RuleSpec(
            rule_id=rule_id,
            rule_type=rule_type,
            params=dict(params),
            version=len(versions) + 1,
            enabled=enabled,
        )
        versions.append(spec)
        return spec

    def delete(self, rule_id: str) -> None:
        """逻辑删除：追加 enabled=False 的墓碑版本；历史版本仍可取回重算。"""
        versions = self._history.get(rule_id)
        if not versions or not versions[-1].enabled:
            raise RuleNotFoundError(f"rule not found or already deleted: {rule_id!r}")
        last = versions[-1]
        versions.append(
            RuleSpec(
                rule_id=rule_id,
                rule_type=last.rule_type,
                params=dict(last.params),
                version=len(versions) + 1,
                enabled=False,
            )
        )

    def get(self, rule_id: str, version: int | None = None) -> RuleSpec:
        versions = self._history.get(rule_id)
        if not versions:
            raise RuleNotFoundError(f"unknown rule: {rule_id!r}")
        if version is None:
            return versions[-1]
        if version < 1 or version > len(versions):
            raise RuleNotFoundError(f"rule {rule_id!r} version {version} not found")
        return versions[version - 1]

    def active_rules(self) -> list[RuleSpec]:
        """返回每个规则的当前启用版本。"""
        result = []
        for versions in self._history.values():
            latest = versions[-1]
            if latest.enabled:
                result.append(latest)
        return result
