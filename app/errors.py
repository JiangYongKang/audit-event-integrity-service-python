"""错误原因枚举与可区分异常。"""


class ArchiveFullError(Exception):
    """留档容量已满：拒绝写入新留档（背压），不静默丢弃、不挤掉老留档。"""
