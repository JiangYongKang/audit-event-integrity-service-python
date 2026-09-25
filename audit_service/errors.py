"""可区分的业务异常定义。"""
from __future__ import annotations


class AuditServiceError(Exception):
    """审计服务错误基类。"""


class EventValidationError(AuditServiceError):
    """事件规范化阶段的校验错误。"""


class RuleEvaluationError(AuditServiceError):
    """单条规则评估时抛出的错误（需被隔离，不中断整批评估）。"""


class RuleNotFoundError(AuditServiceError):
    """规则不存在。"""
