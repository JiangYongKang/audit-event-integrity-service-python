"""HTTP 层冒烟：摄取、留档查询、修复重投、完整性校验。"""
import logging

from fastapi.testclient import TestClient

from app.api import create_app
from app.service import AuditService
from tests.conftest import make_event as ev

log = logging.getLogger("test.api")


def test_ingest_archive_resubmit_verify_over_http():
    client = TestClient(create_app(AuditService()))

    bad = ev("api-1", 0)
    bad["timestamp"] = "bad"
    r = client.post("/events:ingest", json=[bad]).json()[0]
    log.info("HTTP 摄取 event_id=api-1 -> %s/%s", r["status"], r["reason"])
    assert r["status"] == "rejected"
    assert r["reason"] == "malformed_timestamp"

    records = client.get("/rejections", params={"source": "s1"}).json()
    log.info("HTTP 留档查询: %d 条, reason=%s",
             len(records), records[0]["reason"])
    assert len(records) == 1
    assert records[0]["event_id"] == "api-1"

    r2 = client.post("/sources/s1/events/api-1:resubmit", json=ev("api-1", 0))
    body = r2.json()
    log.info("HTTP 修复重投 -> %s", body["status"])
    assert body["status"] == "repaired"

    # 无留档的标识走修复通道 → 409
    r3 = client.post("/sources/s1/events/api-9:resubmit", json=ev("api-9", 1))
    log.info("HTTP 无留档修复 -> %s", r3.status_code)
    assert r3.status_code == 409

    v = client.get("/verify/s1").json()
    log.info("HTTP 完整性: %s", v["message"])
    assert v["status"] == "valid"
