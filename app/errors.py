"""可区分的错误类型：摄取拒绝、留档容量、修复通道误用等。"""
from __future__ import annotations

from app.models import RejectReason


class IngestRejection(Exception):
    """单条事件被拒绝（带可区分原因与可解释细节）。"""

    def __init__(self, reason: RejectReason, detail: str) -> None:
        super().__init__(f"{reason.value}: {detail}")
        self.reason = reason
        self.detail = detail


class ArchiveCapacityExceeded(Exception):
    """被拒投递留档已达容量上限：拒绝写入新留档（背压），不静默丢弃、不挤占旧留档。"""

    def __init__(self, source: str, event_id: str, capacity: int) -> None:
        super().__init__(
            f"留档容量已满（{capacity}），无法记录 source={source} event_id={event_id}"
        )
        self.source = source
        self.event_id = event_id
        self.capacity = capacity


class RepairNotAllowed(Exception):
    """修复重投通道被误用（无留档 / 事件已成功接收）。"""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail
