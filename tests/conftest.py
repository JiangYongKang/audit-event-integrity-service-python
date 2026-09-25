"""测试公共夹具与日志配置：所有摄取/判定日志均带事件标识与依据。"""
from __future__ import annotations

import logging
import sys

import pytest

from audit_service.models import RetentionPolicy
from audit_service.service import AuditService


@pytest.fixture
def service() -> AuditService:
    return AuditService(
        retention=RetentionPolicy(max_age_seconds=3600, max_count=100),
        known_sources={"src-billing", "src-iam", "src-gateway"},
    )


@pytest.fixture
def capacity_service() -> AuditService:
    return AuditService(
        retention=RetentionPolicy(max_count=3),
        known_sources={"src-billing"},
    )


def log_ingestion(results) -> None:
    """打印每条摄取结果：事件标识 + 结论 + 可区分原因。"""
    for r in results:
        if r.accepted:
            logging.info(
                "INGEST event_id=%s source=%s ACCEPTED seq=%s duplicate=%s hash=%s detail=%s",
                r.event_id,
                r.source,
                r.sequence,
                r.duplicate,
                r.record_hash[:12],
                r.detail or "-",
            )
        else:
            logging.info(
                "INGEST event_id=%s source=%s REJECTED reason=%s detail=%s",
                r.event_id,
                r.source,
                r.reason.value if r.reason else "UNKNOWN",
                r.detail,
            )


def log_rule_results(batch) -> None:
    """打印每条规则判定：事件标识 + 规则/版本 + 结论与依据。"""
    for per_event in batch:
        for r in per_event:
            if r.status == "OK":
                logging.info(
                    "RULE event_id=%s rule=%s v%s matched=%s rationale=%s",
                    r.event_id,
                    r.rule_id,
                    r.rule_version,
                    r.matched,
                    r.rationale,
                )
            else:
                logging.info(
                    "RULE event_id=%s rule=%s v%s STATUS=RULE_ERROR error=%s",
                    r.event_id,

                    r.rule_id,
                    r.rule_version,
                    r.error,
                )


def log_verify(results) -> None:
    for r in results:
        logging.info(
            "VERIFY source=%s ok=%s checked=%s seq=[%s..%s] evicted=%s problem=%s event=%s",
            r.source,
            r.ok,
            r.checked,
            r.first_sequence,
            r.last_sequence,
            r.evicted_count,
            r.problem,
            r.problem_event_id or "-",
        )


@pytest.fixture
def helpers():
    return type(
        "H",
        (),
        {
            "log_ingestion": staticmethod(log_ingestion),
            "log_rule_results": staticmethod(log_rule_results),
            "log_verify": staticmethod(log_verify),
        },
    )


def pytest_configure(config):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)
