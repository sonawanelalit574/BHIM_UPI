from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

CircuitState = Literal["closed", "open", "half"]
TxnStatus = Literal["initiated", "pending_psp", "success", "failed", "reversed", "disputed"]
PspRole = Literal["payer", "payee"]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


@dataclass
class Bank:
    id: str
    name: str
    ifsc_prefix: str
    settlement_account: str


@dataclass
class Psp:
    id: str
    name: str
    handle: str
    bank_id: str
    circuit: CircuitState = "closed"
    draining: bool = False
    rps_limit: int = 800
    inflight: int = 0
    success_24h: int = 0
    failed_24h: int = 0


@dataclass
class Vpa:
    address: str
    owner_name: str
    psp_id: str
    bank_id: str
    account_ref: str
    frozen: bool = False
    kyc_tier: str = "full"


@dataclass
class Transaction:
    utr: str
    bucket: str
    shard: int
    payer_vpa: str
    payee_vpa: str
    payer_psp: str
    payee_psp: str
    amount_paise: int
    note: str
    status: TxnStatus
    created_at: datetime
    settled: bool = False
    failure_reason: str | None = None

    def to_public(self) -> dict[str, Any]:
        data = asdict(self)
        data["created_at"] = iso(self.created_at)
        data["amount_inr"] = round(self.amount_paise / 100, 2)
        return data


@dataclass
class Dispute:
    id: str
    utr: str
    reason: str
    status: Literal["open", "accepted", "rejected"] = "open"
    opened_at: datetime = field(default_factory=utcnow)


@dataclass
class SettlementBatch:
    id: str
    psp_id: str
    txn_count: int
    net_paise: int
    status: Literal["draft", "posted"] = "draft"
    created_at: datetime = field(default_factory=utcnow)


@dataclass
class AuditEvent:
    id: str
    actor: str
    action: str
    target: str
    detail: str
    at: datetime = field(default_factory=utcnow)

    def to_public(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "actor": self.actor,
            "action": self.action,
            "target": self.target,
            "detail": self.detail,
            "at": iso(self.at),
        }
