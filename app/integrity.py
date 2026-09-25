"""基于 SHA-256 哈希链的完整性校验。

链结构（按来源独立维护）：
    event_hash  = SHA256(canonical_json(event))
    entry_hash₀ = SHA256("GENESIS|" + event_hash₀ + "|" + seq₀)
    entry_hashᵢ = SHA256(entry_hashᵢ₋₁ + "|" + event_hashᵢ + "|" + seqᵢ)

每条 entry_hash 同时追加到只增不改的锚点日志 anchors。
旧数据被保留策略淘汰后，checkpoint_tail 记录窗口首条记录的前驱哈希，
校验从该锚点继续，结论依然确定（VALID 或具体断裂类型）。
"""
from __future__ import annotations

import hashlib

from app.models import AuditEvent, ChainEntry, ChainStatus, VerificationResult

GENESIS = "GENESIS"


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class IntegrityChain:
    @staticmethod
    def hash_event(event: AuditEvent) -> str:
        return _sha256_hex(event.to_canonical_json())

    @staticmethod
    def derive_entry_hash(prev_entry_hash: str | None, event_hash: str,
                          sequence: int) -> str:
        prev = prev_entry_hash if prev_entry_hash is not None else GENESIS
        body = f"{prev}|{event_hash}|{sequence}".encode("utf-8")
        return _sha256_hex(body)

    def verify(self, source: str, events: list[AuditEvent | None],
               chain: list[ChainEntry], anchors: list[str],
               checkpoint_tail: str | None) -> VerificationResult:
        """对单个来源做完整性校验。

        返回状态可区分：
          VALID / EMPTY：通过（或无数据）；
          BROKEN_HASH：事件内容被篡改（event_hash 不符）；
          BROKEN_LINK：链式指针断裂（插入/删除/重排/序号篡改/检查点不符）；
          BROKEN_ANCHOR：独立锚点日志被篡改或截断。
        """
        n = len(chain)
        if n == 0:
            return VerificationResult(
                source=source,
                status=ChainStatus.EMPTY,
                message=f"来源 {source} 当前无在窗事件（锚点 {len(anchors)} 条）",
                verified_count=0,
                head_entry_hash=None,
                anchor_count=len(anchors),
                chain_tip=anchors[-1] if anchors else None,
            )

        if len(events) != n:
            return VerificationResult(
                source=source,
                status=ChainStatus.BROKEN_LINK,
                message=(f"事件数 {len(events)} 与链记录数 {n} 不一致，"
                         "存在删除或插入"),
                anchor_count=len(anchors),
            )

        # 1) 逐条重算 event_hash 与 entry_hash
        prev = checkpoint_tail
        recomputed_tip: str | None = None
        for i, (event, entry) in enumerate(zip(events, chain)):
            if event is None:
                return VerificationResult(
                    source=source,
                    status=ChainStatus.BROKEN_LINK,
                    message=(f"链上事件缺失（疑似删除）：event_id={entry.event_id}, "
                             f"sequence={entry.sequence}, 窗口位置={i}"),
                    verified_count=i,
                    anchor_count=len(anchors),
                )
            event_hash = self.hash_event(event)
            if event_hash != entry.event_hash:
                return VerificationResult(
                    source=source,
                    status=ChainStatus.BROKEN_HASH,
                    message=(f"事件内容哈希不一致（疑似篡改）：event_id={event.event_id}, "
                             f"sequence={event.sequence}, 窗口位置={i}, "
                             f"期望={entry.event_hash[:12]}, 实际={event_hash[:12]}"),
                    verified_count=i,
                    anchor_count=len(anchors),
                )
            entry_hash = self.derive_entry_hash(prev, event_hash, entry.sequence)
            if entry_hash != entry.entry_hash:
                return VerificationResult(
                    source=source,
                    status=ChainStatus.BROKEN_LINK,
                    message=(f"链式指针断裂：event_id={event.event_id}, "
                             f"sequence={event.sequence}, 窗口位置={i}，"
                             "存在插入/删除/重排/序号篡改或检查点不匹配"),
                    verified_count=i,
                    anchor_count=len(anchors),
                )
            prev = entry_hash
            recomputed_tip = entry_hash

        # 2) 与独立锚点日志比对（覆盖锚点篡改/重写/截断）
        if len(anchors) < n:
            return VerificationResult(
                source=source,
                status=ChainStatus.BROKEN_ANCHOR,
                message=(f"锚点记录数 {len(anchors)} 少于在窗链接数 {n}，"
                         "锚点日志疑似被删除"),
                verified_count=n,
                head_entry_hash=recomputed_tip,
                anchor_count=len(anchors),
            )
        window_anchors = anchors[-n:]
        expected = [e.entry_hash for e in chain]
        if window_anchors != expected:
            first_diff = next(
                (i for i, (a, b) in enumerate(zip(window_anchors, expected))
                 if a != b), n,
            )
            return VerificationResult(
                source=source,
                status=ChainStatus.BROKEN_ANCHOR,
                message=(f"锚点日志与链重算结果不一致（疑似锚点篡改/重写），"
                         f"首个分歧窗口位置={first_diff}"),
                verified_count=n,
                head_entry_hash=recomputed_tip,
                anchor_count=len(anchors),
            )

        return VerificationResult(
            source=source,
            status=ChainStatus.VALID,
            message=(f"来源 {source} 完整性校验通过：在窗 {n} 条，"
                     f"锚点共 {len(anchors)} 条，检查点="
                     f"{(checkpoint_tail or GENESIS)[:12]}"),
            verified_count=n,
            head_entry_hash=recomputed_tip,
            anchor_count=len(anchors),
            chain_tip=recomputed_tip,
        )
