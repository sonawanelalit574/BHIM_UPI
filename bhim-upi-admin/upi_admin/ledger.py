"""Hot/cold partitioned ledger modeled on Discord message storage.

Partition key: (time_bucket, shard) where shard = hash(payer_vpa) % N.
Reads coalesce like Discord data services: concurrent lookups of the same UTR
share one in-flight fetch so a viral payee VPA cannot hotspot a shard.
"""

from __future__ import annotations

import hashlib
import threading
from collections import defaultdict
from concurrent.futures import Future
from datetime import datetime, timedelta, timezone

from .models import Transaction

SHARD_COUNT = 16
HOT_WINDOW_DAYS = 2


def time_bucket(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d")


def shard_for(vpa: str) -> int:
    digest = hashlib.sha256(vpa.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % SHARD_COUNT


class Coalescer:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._inflight: dict[str, Future] = {}
        self.hits = 0
        self.coalesced = 0

    def get(self, key: str, loader) -> object:
        with self._lock:
            self.hits += 1
            existing = self._inflight.get(key)
            if existing is not None:
                self.coalesced += 1
                fut = existing
                wait = True
            else:
                fut = Future()
                self._inflight[key] = fut
                wait = False
        if wait:
            return fut.result()
        try:
            value = loader()
            fut.set_result(value)
            return value
        except Exception as exc:  # pragma: no cover - defensive
            fut.set_exception(exc)
            raise
        finally:
            with self._lock:
                self._inflight.pop(key, None)


class PartitionedLedger:
    def __init__(self, shard_count: int = SHARD_COUNT) -> None:
        self.shard_count = shard_count
        self._hot: dict[tuple[str, int], dict[str, Transaction]] = defaultdict(dict)
        self._cold: dict[tuple[str, int], dict[str, Transaction]] = defaultdict(dict)
        self._by_utr: dict[str, tuple[str, int, str]] = {}
        self._lock = threading.RLock()
        self.coalescer = Coalescer()

    def insert(self, txn: Transaction, *, hot: bool = True) -> None:
        key = (txn.bucket, txn.shard)
        with self._lock:
            store = self._hot if hot else self._cold
            store[key][txn.utr] = txn
            self._by_utr[txn.utr] = (txn.bucket, txn.shard, "hot" if hot else "cold")

    def get(self, utr: str) -> Transaction | None:
        def _load() -> Transaction | None:
            with self._lock:
                loc = self._by_utr.get(utr)
                if not loc:
                    return None
                bucket, shard, tier = loc
                store = self._hot if tier == "hot" else self._cold
                return store[(bucket, shard)].get(utr)

        return self.coalescer.get(utr, _load)  # type: ignore[return-value]

    def update(self, utr: str, **fields) -> Transaction | None:
        with self._lock:
            txn = self.get(utr)
            if txn is None:
                return None
            for name, value in fields.items():
                setattr(txn, name, value)
            return txn

    def search(
        self,
        *,
        vpa: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[Transaction]:
        with self._lock:
            rows: list[Transaction] = []
            for store in (self._hot, self._cold):
                for partition in store.values():
                    rows.extend(partition.values())
            rows.sort(key=lambda t: t.utr, reverse=True)
            out = []
            for txn in rows:
                if vpa and vpa not in (txn.payer_vpa, txn.payee_vpa):
                    continue
                if status and txn.status != status:
                    continue
                out.append(txn)
                if len(out) >= limit:
                    break
            return out

    def unsettled(self, psp_id: str) -> list[Transaction]:
        with self._lock:
            found = []
            for partition in self._hot.values():
                for txn in partition.values():
                    if txn.status == "success" and not txn.settled:
                        if psp_id in (txn.payer_psp, txn.payee_psp):
                            found.append(txn)
            return found

    def archive_cold_buckets(self, as_of: datetime | None = None) -> int:
        """Move partitions older than HOT_WINDOW_DAYS into cold storage."""
        cutoff = (as_of or datetime.now(timezone.utc)) - timedelta(days=HOT_WINDOW_DAYS)
        cutoff_bucket = time_bucket(cutoff)
        moved = 0
        with self._lock:
            keys = [k for k in list(self._hot) if k[0] < cutoff_bucket]
            for key in keys:
                partition = self._hot.pop(key)
                self._cold[key].update(partition)
                for utr in partition:
                    bucket, shard, _ = self._by_utr[utr]
                    self._by_utr[utr] = (bucket, shard, "cold")
                    moved += 1
        return moved

    def shard_stats(self) -> list[dict]:
        with self._lock:
            stats = []
            for shard in range(self.shard_count):
                hot_n = sum(len(p) for (b, s), p in self._hot.items() if s == shard)
                cold_n = sum(len(p) for (b, s), p in self._cold.items() if s == shard)
                stats.append(
                    {
                        "shard": shard,
                        "hot_txns": hot_n,
                        "cold_txns": cold_n,
                        "hot": hot_n > 40,
                    }
                )
            return stats

    def hot_partitions(self, threshold: int = 8) -> list[dict]:
        with self._lock:
            rows = []
            for (bucket, shard), partition in self._hot.items():
                if len(partition) >= threshold:
                    rows.append(
                        {
                            "bucket": bucket,
                            "shard": shard,
                            "txns": len(partition),
                            "sample_vpa": next(iter(partition.values())).payer_vpa,
                        }
                    )
            rows.sort(key=lambda r: r["txns"], reverse=True)
            return rows
