"""规范化哈希工具：以键排序的 JSON 作为规范化序列化，SHA-256 做摘要。"""
from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_json(value: Any) -> str:
    """把任意 JSON 兼容值序列化为确定形式：键排序、无空白、非 ASCII 原样保留。"""
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )


def digest_hex(value: Any) -> str:
    """对规范化后的字节计算 SHA-256，返回 64 位十六进制摘要。"""
    blob = canonical_json(value).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()
