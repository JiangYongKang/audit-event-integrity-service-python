"""HTTP API 端到端：摄取、拒绝原因、完整性、规则增删改与评估。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from audit_service.api import create_app
from audit_service.models import RetentionPolicy
from audit_service.service import AuditService


@pytest.fixture
def client():
    svc = AuditService(
        retention=RetentionPolicy(max_age_seconds=3600, max_count=10),
        known_sources={"src-billing", "src-iam"},
    )
    return TestClient(create_app(svc))


def _event(eid, **overrides):
    body = {
        "event_id": eid,
        "source": "src-billing",
        "event_type": "payment",
        "timestamp": "2026-09-25T10:00:00+00:00",
        "payload": {"amount": 100},
    }
    body.update(overrides)
    return body


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_ingest_batch_with_idempotent_replay(client):
    payload = {"source": "src-billing", "events": [_event("a-1"), _event("a-1")]}
    resp = client.post("/ingest", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["received"] == 2 and data["accepted"] == 2
    assert data["results"][0]["duplicate"] is False  # 首次接收
    assert data["results"][1]["duplicate"] is True  # 同批重放，幂等
    # 两种可区分拒绝
    payload2 = {
        "source": "src-billing",
        "events": [
            _event("a-bad-ts", timestamp="nonsense"),
            _event("a-src", source="src-iam"),
        ],
    }
    data2 = client.post("/ingest", json=payload2).json()
    reasons = {item["event_id"]: item["reason"] for item in data2["results"]}
    assert reasons == {"a-bad-ts": "INVALID_TIMESTAMP", "a-src": "SOURCE_MISMATCH"}


def test_verify_endpoint_reports_chain(client):
    client.post("/ingest", json={"source": "src-billing", "events": [_event("v-1"), _event("v-2")]})
    data = client.get("/verify").json()
    assert data["all_ok"] is True
    (result,) = data["results"]
    assert result["source"] == "src-billing" and result["checked"] == 2
    assert result["first_sequence"] == 1 and result["last_sequence"] == 2


def test_capacity_backpressure_over_http(client):
    events = [_event(f"c-{i}") for i in range(11)]
    data = client.post("/ingest", json={"source": "src-billing", "events": events}).json()
    assert data["accepted"] == 10 and data["rejected"] == 1
    rejected = [r for r in data["results"] if not r["accepted"]]
    assert rejected[0]["reason"] == "CAPACITY_EXCEEDED"


def test_rule_crud_and_evaluate_over_http(client):
    # 创建
    resp = client.put("/rules/large-pay", json={
        "rule_type": "payload_threshold",
        "params": {"path": "amount", "min": 1000},
    })
    assert resp.status_code == 200 and resp.json()["version"] == 1
    # 更新 -> 新版本
    resp2 = client.put("/rules/large-pay", json={
        "rule_type": "payload_threshold",
        "params": {"path": "amount", "min": 5000},
    })
    assert resp2.json()["version"] == 2

    # 评估：命中 + 规则异常隔离（未知路径）
    client.put("/rules/broken", json={"rule_type": "payload_threshold",
                                      "params": {"path": "missing.path", "min": 1}})
    eval_resp = client.post("/evaluate", json={"events": [
        _event("p-1", payload={"amount": 9000}),
    ]})
    assert eval_resp.status_code == 200
    by_rule = {r["rule_id"]: r for r in eval_resp.json()["results"][0]}
    assert by_rule["large-pay"]["matched"] is True
    assert "9000" in by_rule["large-pay"]["rationale"]
    assert by_rule["broken"]["status"] == "RULE_ERROR"

    # 列表与删除
    rules = client.get("/rules").json()["rules"]
    assert {r["rule_id"] for r in rules} == {"large-pay", "broken"}
    deleted = client.delete("/rules/broken")
    assert deleted.status_code == 200
    assert client.delete("/rules/broken").status_code == 404
    assert client.delete("/rules/ghost").status_code == 404


def test_evaluate_with_historical_version(client):
    client.put("/rules/th", json={"rule_type": "payload_threshold",
                                  "params": {"path": "amount", "min": 100}})
    client.put("/rules/th", json={"rule_type": "payload_threshold",
                                  "params": {"path": "amount", "min": 1000}})
    resp = client.post("/evaluate", json={
        "events": [_event("h-1", payload={"amount": 500})],
        "rule_versions": {"th": 1},
    })
    result = resp.json()["results"][0][0]
    assert result["rule_version"] == 1 and result["matched"] is True
