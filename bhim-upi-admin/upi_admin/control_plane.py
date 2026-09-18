"""System administrative control plane for a simulated BHIM UPI switch.

Operators: freeze VPAs, trip PSP circuits, reverse in the hot window,
run net settlement, archive cold ledger buckets, inspect hot partitions.
Every mutation writes an audit event.
"""

from __future__ import annotations

import itertools
from datetime import datetime

from .ledger import PartitionedLedger
from .models import AuditEvent, Dispute, Psp, SettlementBatch, Vpa
from .switch import SwitchError, UpiSwitch

_ids = itertools.count(1)


def _new_id(prefix: str) -> str:
    return f"{prefix}{next(_ids):06d}"


class ControlPlane:
    def __init__(self, switch: UpiSwitch, ledger: PartitionedLedger) -> None:
        self.switch = switch
        self.ledger = ledger
        self.audit: list[AuditEvent] = []
        self.disputes: dict[str, Dispute] = {}
        self.settlements: list[SettlementBatch] = []

    def record(self, actor: str, action: str, target: str, detail: str) -> AuditEvent:
        event = AuditEvent(
            id=_new_id("AUD"),
            actor=actor,
            action=action,
            target=target,
            detail=detail,
        )
        self.audit.insert(0, event)
        return event

    def dashboard(self) -> dict:
        txns = self.ledger.search(limit=10_000)
        success = sum(1 for t in txns if t.status == "success")
        failed = sum(1 for t in txns if t.status == "failed")
        disputed = sum(1 for t in txns if t.status == "disputed")
        volume = sum(t.amount_paise for t in txns if t.status == "success")
        return {
            "psps": len(self.switch.psps),
            "vpas": len(self.switch.vpas),
            "transactions": len(txns),
            "success": success,
            "failed": failed,
            "disputed": disputed,
            "volume_inr": round(volume / 100, 2),
            "open_circuits": [p.id for p in self.switch.psps.values() if p.circuit != "closed"],
            "frozen_vpas": [v.address for v in self.switch.vpas.values() if v.frozen],
            "hot_partitions": self.ledger.hot_partitions(),
            "shards": self.ledger.shard_stats(),
            "coalesce_hits": self.ledger.coalescer.hits,
            "coalesce_saved": self.ledger.coalescer.coalesced,
        }

    def list_psps(self) -> list[dict]:
        return [
            {
                "id": p.id,
                "name": p.name,
                "handle": p.handle,
                "bank_id": p.bank_id,
                "circuit": p.circuit,
                "draining": p.draining,
                "rps_limit": p.rps_limit,
                "success_24h": p.success_24h,
                "failed_24h": p.failed_24h,
            }
            for p in self.switch.psps.values()
        ]

    def set_circuit(self, actor: str, psp_id: str, state: str) -> Psp:
        if state not in ("closed", "open", "half"):
            raise SwitchError("BAD_CIRCUIT", "state must be closed|open|half")
        psp = self._psp(psp_id)
        psp.circuit = state  # type: ignore[assignment]
        self.record(actor, "circuit.set", psp_id, state)
        return psp

    def drain_psp(self, actor: str, psp_id: str, draining: bool) -> Psp:
        psp = self._psp(psp_id)
        psp.draining = draining
        self.record(actor, "psp.drain" if draining else "psp.undrain", psp_id, str(draining))
        return psp

    def add_psp(
        self,
        actor: str,
        *,
        psp_id: str,
        name: str,
        handle: str,
        bank_id: str,
        rps_limit: int = 800,
    ) -> Psp:
        psp_id = psp_id.strip().lower()
        handle = handle.strip()
        if not psp_id or not name.strip() or not handle:
            raise SwitchError("BAD_PSP", "id, name, and handle are required")
        if psp_id in self.switch.psps:
            raise SwitchError("PSP_EXISTS", psp_id)
        psp = Psp(
            id=psp_id,
            name=name.strip(),
            handle=handle if handle.startswith("@") else f"@{handle}",
            bank_id=bank_id.strip() or "sbi",
            rps_limit=int(rps_limit or 800),
        )
        self.switch.register_psp(psp)
        self.record(actor, "psp.add", psp.id, psp.handle)
        return psp

    def add_vpa(
        self,
        actor: str,
        *,
        address: str,
        owner_name: str,
        psp_id: str,
        bank_id: str = "",
        account_ref: str = "",
    ) -> Vpa:
        address = address.strip().lower()
        if "@" not in address:
            raise SwitchError("BAD_VPA", "address must look like name@handle")
        if address in self.switch.vpas:
            raise SwitchError("VPA_EXISTS", address)
        psp = self._psp(psp_id.strip())
        vpa = Vpa(
            address=address,
            owner_name=owner_name.strip() or address.split("@", 1)[0],
            psp_id=psp.id,
            bank_id=bank_id.strip() or psp.bank_id,
            account_ref=account_ref.strip() or "****0000",
        )
        self.switch.register_vpa(vpa)
        self.record(actor, "vpa.add", address, f"{vpa.owner_name} via {psp.id}")
        return vpa

    def list_vpas(self) -> list[dict]:
        return [
            {
                "address": v.address,
                "owner_name": v.owner_name,
                "psp_id": v.psp_id,
                "bank_id": v.bank_id,
                "account_ref": v.account_ref,
                "frozen": v.frozen,
                "kyc_tier": v.kyc_tier,
            }
            for v in self.switch.vpas.values()
        ]

    def freeze_vpa(self, actor: str, address: str, frozen: bool, reason: str) -> None:
        vpa = self.switch.vpas.get(address)
        if not vpa:
            raise SwitchError("VPA_NOT_FOUND", address)
        vpa.frozen = frozen
        action = "vpa.freeze" if frozen else "vpa.unfreeze"
        self.record(actor, action, address, reason)

    def reverse(self, actor: str, utr: str, reason: str) -> dict:
        txn = self.ledger.get(utr)
        if txn is None:
            raise SwitchError("TXN_NOT_FOUND", utr)
        if txn.status != "success":
            raise SwitchError("NOT_REVERSIBLE", f"status is {txn.status}")
        loc = self.ledger._by_utr.get(utr)
        if loc and loc[2] == "cold":
            raise SwitchError("COLD_WINDOW", "reverse only in hot ledger; open a dispute")
        self.ledger.update(utr, status="reversed", settled=False, failure_reason=reason)
        self.record(actor, "txn.reverse", utr, reason)
        return txn.to_public()

    def open_dispute(self, actor: str, utr: str, reason: str) -> Dispute:
        txn = self.ledger.get(utr)
        if txn is None:
            raise SwitchError("TXN_NOT_FOUND", utr)
        dispute = Dispute(id=_new_id("DSP"), utr=utr, reason=reason)
        self.disputes[dispute.id] = dispute
        self.ledger.update(utr, status="disputed")
        self.record(actor, "dispute.open", utr, reason)
        return dispute

    def settle_psp(self, actor: str, psp_id: str) -> SettlementBatch:
        self._psp(psp_id)
        pending = self.ledger.unsettled(psp_id)
        net = 0
        for txn in pending:
            if txn.payee_psp == psp_id:
                net += txn.amount_paise
            if txn.payer_psp == psp_id:
                net -= txn.amount_paise
            txn.settled = True
        batch = SettlementBatch(
            id=_new_id("STL"),
            psp_id=psp_id,
            txn_count=len(pending),
            net_paise=net,
            status="posted",
        )
        self.settlements.insert(0, batch)
        self.record(actor, "settlement.post", psp_id, f"{len(pending)} txns net {net}")
        return batch

    def archive(self, actor: str, as_of: datetime | None = None) -> int:
        moved = self.ledger.archive_cold_buckets(as_of=as_of)
        self.record(actor, "ledger.archive", "hot", f"moved {moved}")
        return moved

    def lookup(self, utr: str) -> dict:
        txn = self.ledger.get(utr)
        if txn is None:
            raise SwitchError("TXN_NOT_FOUND", utr)
        return txn.to_public()

    def _psp(self, psp_id: str) -> Psp:
        psp = self.switch.psps.get(psp_id)
        if not psp:
            raise SwitchError("PSP_NOT_FOUND", psp_id)
        return psp
