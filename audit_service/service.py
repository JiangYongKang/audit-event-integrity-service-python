"""审计事件摄取与合规评估服务（门面）。

组合 EventStore（摄取/完整性/保留）、RuleRegistry（版本化规则）与
RuleEvaluator（异常隔离）。evaluate 对每条事件评估所有当前启用规则；
可通过 rule_versions 传入 {rule_id: version} 复算历史结论。
"""
from __future__ import annotations

from .errors import RuleNotFoundError
from .models import EventInput, IngestionResult, RuleResult, RuleSpec, VerifyResult
from .rules import RuleEvaluator, RuleRegistry
from .store import EventStore


class AuditService:
    def __init__(self, retention=None, known_sources: set[str] | None = None) -> None:
        self.store = EventStore(retention=retention, known_sources=known_sources)
        self.registry = RuleRegistry()
        self._evaluator = RuleEvaluator()

    # ---- 摄取 ----
    def ingest_batch(self, events: list[EventInput], now=None) -> list[IngestionResult]:
        return self.store.ingest_batch(events, now=now)

    def apply_retention(self, now=None) -> dict[str, int]:
        return self.store.apply_retention(now)

    # ---- 完整性 ----
    def verify_all(self) -> list[VerifyResult]:
        return self.store.verify_all()

    # ---- 规则管理 ----
    def upsert_rule(
        self, rule_id: str, rule_type: str, params: dict, enabled: bool = True
    ) -> RuleSpec:
        return self.registry.upsert(rule_id, rule_type, params, enabled)

    def delete_rule(self, rule_id: str) -> None:
        self.registry.delete(rule_id)

    def get_rule(self, rule_id: str, version: int | None = None) -> RuleSpec:
        return self.registry.get(rule_id, version)

    # ---- 合规评估 ----
    def evaluate(
        self,
        events: list[EventInput],
        rule_versions: dict[str, int] | None = None,
    ) -> list[list[RuleResult]]:
        """对每条事件评估一组规则。

        rule_versions 为 None 时评估全部当前启用规则；
        提供 {rule_id: version} 时按指定历史版本取规则，用于结论复算。
        单条规则异常已在 RuleEvaluator 内隔离为 RULE_ERROR，不影响整批。
        """
        if rule_versions is None:
            specs = self.registry.active_rules()
        else:
            specs = [
                self.registry.get(rule_id, version)
                for rule_id, version in sorted(rule_versions.items())
            ]
        batch_results: list[list[RuleResult]] = []
        for event in events:
            per_event: list[RuleResult] = []
            for spec in specs:
                if not spec.enabled:
                    continue
                try:
                    per_event.append(self._evaluator.evaluate(spec, event))
                except RuleNotFoundError:
                    raise
                except Exception as exc:  # 双保险：评估器外的任何意外也隔离
                    per_event.append(
                        RuleResult(
                            rule_id=spec.rule_id,
                            rule_version=spec.version,
                            event_id=event.event_id,
                            matched=False,
                            status="RULE_ERROR",
                            error=f"{type(exc).__name__}: {exc}",
                        )
                    )
            batch_results.append(per_event)
        return batch_results
