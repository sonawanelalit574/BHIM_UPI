"""Discord-style Snowflake identifiers for UPI UTRs.

Layout (64-bit):
  [42 bits timestamp ms since epoch] [10 bits shard] [12 bits sequence]
Chronological sort order matches Discord snowflakes so ledger scans by UTR
are time-ordered without a secondary index.
"""

from __future__ import annotations

import threading
import time

EPOCH_MS = 1_704_067_200_000  # 2024-01-01 UTC
SHARD_BITS = 10
SEQ_BITS = 12
MAX_SHARD = (1 << SHARD_BITS) - 1
MAX_SEQ = (1 << SEQ_BITS) - 1


class SnowflakeFactory:
    def __init__(self, shard_id: int = 1) -> None:
        if not 0 <= shard_id <= MAX_SHARD:
            raise ValueError("shard_id out of range")
        self.shard_id = shard_id
        self._seq = 0
        self._last_ms = 0
        self._lock = threading.Lock()

    def next_id(self) -> int:
        with self._lock:
            now = int(time.time() * 1000)
            if now == self._last_ms:
                self._seq = (self._seq + 1) & MAX_SEQ
                if self._seq == 0:
                    while now <= self._last_ms:
                        now = int(time.time() * 1000)
            else:
                self._seq = 0
                self._last_ms = now
            ts = now - EPOCH_MS
            return (ts << (SHARD_BITS + SEQ_BITS)) | (self.shard_id << SEQ_BITS) | self._seq

    def next_utr(self) -> str:
        return f"UTR{self.next_id():016d}"


def decode_snowflake(utr: str) -> dict:
    raw = int(utr.replace("UTR", ""))
    seq = raw & MAX_SEQ
    shard = (raw >> SEQ_BITS) & MAX_SHARD
    ts_ms = (raw >> (SHARD_BITS + SEQ_BITS)) + EPOCH_MS
    return {"id": raw, "shard": shard, "sequence": seq, "created_ms": ts_ms}
