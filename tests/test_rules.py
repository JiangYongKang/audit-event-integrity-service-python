"""合规规则评估：命中解释、单条异常隔离、规则增删改与历史结论可复现。"""
from __future__ import annotations

import pytest

from audit_service.errors import RuleNotFoundError
from tests.factories import make_event


@pytest.fixture
def rule_service(service):
    service.upsert_rule("large-payment", "payload_threshold", {"path": "amount", "min": 10000})
    service.upsert_rule("is-login", "event_type_equals", {"event_type": "login"})
    service.upsert_rule("from-billing", "source_equals", {"source": "src-billing"})
    service.upsert_rule("vip-user", "payload_equals", {"path": "user.tier", "value": "vip"})
    # 两条必然出错的规则：缺失路径 + 未知类型
    service.upsert_rule("broken-path", "payload_threshold", {"path": "does.not.exist", "min": 1})
    service.upsert_rule("unknown-type", "totally_unknown", {"x": 1})
    return service


def _events():
    return [
        make_event("pay-1", payload={"amount": 50000, "user": {"tier": "vip"}}),
        make_event("pay-2", event_type="login", payload={"amount": 50}),
        make_event("pay-3", source="src-iam", declared_source="src-iam",
                   event_type="login", payload={"amount": 0}),
    ]


def test_rule_matches_have_explainable_rationale(rule_service, helpers):
    batch = rule_service.evaluate(_events())
    helpers.log_rule_results(batch)

    pay1 = {r.rule_id: r for r in batch[0]}
    assert pay1["large-payment"].matched and pay1["large-payment"].status == "OK"
    assert "50000" in pay1["large-payment"].rationale and "MATCH" in pay1["large-payment"].rationale
    assert pay1["vip-user"].matched
    assert pay1["from-billing"].matched

    pay2 = {r.rule_id: r for r in batch[1]}
    assert pay2["is-login"].matched
    assert not pay2["large-payment"].matched and "NO_MATCH" in pay2["large-payment"].rationale


def test_single_rule_error_is_isolated(rule_service, helpers):
    """一条规则出错不中断整批：每条事件仍返回全部 6 条规则的结果。"""
    batch = rule_service.evaluate(_events())
    helpers.log_rule_results(batch)

    assert all(len(per_event) == 6 for per_event in batch)
    for per_event in batch:
        statuses = {r.rule_id: r.status for r in per_event}
        assert statuses["broken-path"] == "RULE_ERROR"
        assert statuses["unknown-type"] == "RULE_ERROR"
        # 出错规则的邻接规则照常得到确定结论
        assert statuses["large-payment"] == "OK"
        assert statuses["is-login"] == "OK"

    errors = {r.rule_id: r for r in batch[0] if r.status == "RULE_ERROR"}
    assert "KeyError" in errors["broken-path"].error
    assert "unknown rule_type" in errors["unknown-type"].error
    # 失败结果必须携带事件标识
    assert all(r.event_id == "pay-1" for r in errors.values())


def test_type_error_rule_does_not_abort_batch(service):
    service.upsert_rule("num", "payload_threshold", {"path": "label", "min": 1})
    service.upsert_rule("login", "event_type_equals", {"event_type": "login"})
    events = [
        make_event("e-1", payload={"label": "not-a-number"}),
        make_event("e-2", event_type="login", payload={"label": 0}),
    ]
    batch = service.evaluate(events)
    r1, r2 = {r.rule_id: r for r in batch[0]}, {r.rule_id: r for r in batch[1]}
    assert r1["num"].status == "RULE_ERROR" and "TypeError" in r1["num"].error
    assert r2["num"].status == "OK" and not r2["num"].matched
    assert r2["login"].matched


def test_rule_update_keeps_history_and_results_reproducible(service, helpers):
    v1 = service.upsert_rule("threshold", "payload_threshold", {"path": "amount", "min": 100})
    v2 = service.upsert_rule("threshold", "payload_threshold", {"path": "amount", "min": 1000})
    assert v1.version == 1 and v2.version == 2

    event = [make_event("pay-x", payload={"amount": 500})]

    # 用当前版本（v2）：500 不命中
    current = {r.rule_id: r for r in service.evaluate(event)[0]}
    helpers.log_rule_results(service.evaluate(event))
    assert current["threshold"].rule_version == 2 and not current["threshold"].matched

    # 指定历史版本（v1）重算：500 命中 —— 历史结论可复现
    historical = {r.rule_id: r for r in service.evaluate(event, rule_versions={"threshold": 1})[0]}
    assert historical["threshold"].rule_version == 1
    assert historical["threshold"].matched
    assert "500" in historical["threshold"].rationale


def test_rule_delete_is_tombstone_and_recompute_still_works(service):
    service.upsert_rule("temp-rule", "event_type_equals", {"event_type": "login"})
    service.delete_rule("temp-rule")

    assert service.get_rule("temp-rule").enabled is False
    assert service.registry.active_rules() == []
    # 历史版本仍可显式取回重算
    old = service.get_rule("temp-rule", version=1)
    assert old.enabled and old.rule_type == "event_type_equals"
    batch = service.evaluate(
        [make_event("d-1", event_type="login")],
        rule_versions={"temp-rule": 1},
    )
    assert batch[0][0].matched

    # 重复删除与不存在规则给出明确错误
    with pytest.raises(RuleNotFoundError):
        service.delete_rule("temp-rule")
    with pytest.raises(RuleNotFoundError):
        service.get_rule("missing", version=9)


def test_disabled_rule_is_not_evaluated(service):
    service.upsert_rule("off-rule", "event_type_equals", {"event_type": "login"}, enabled=False)
    # upsert 返回的是 enabled=False 的墓碑式版本；不参与当前评估
    batch = service.evaluate([make_event("z-1")])
    assert batch[0] == []
