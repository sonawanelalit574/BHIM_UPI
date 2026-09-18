"""UPI switch: Slack-like request path + isolated PSP adapters.

Chat analog:
  Slack HTTP POST /chat  ->  BHIM collect/pay API
  Slack channel server   ->  payer/payee PSP adapter (fan-out)
  Slack RTM presence     ->  VPA registry + circuit state
Money never rides a websocket; the gateway stays request/response, then
asynchronously notifies both PSPs the way Slack fans a message out.
"""

from __future__ import annotations

from .ids import SnowflakeFactory
from .ledger import PartitionedLedger, shard_for, time_bucket
from .models import Psp, Transaction, Vpa, utcnow


class SwitchError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class UpiSwitch:
    def __init__(self, ledger: PartitionedLedger, snowflake: SnowflakeFactory) -> None:
        self.ledger = ledger
        self.snowflake = snowflake
        self.banks: dict[str, object] = {}
        self.psps: dict[str, Psp] = {}
        self.vpas: dict[str, Vpa] = {}

    def register_psp(self, psp: Psp) -> None:
        self.psps[psp.id] = psp

    def register_vpa(self, vpa: Vpa) -> None:
        self.vpas[vpa.address] = vpa

    def collect_pay(
        self,
        *,
        payer_vpa: str,
        payee_vpa: str,
        amount_paise: int,
        note: str = "",
    ) -> Transaction:
        if amount_paise <= 0:
            raise SwitchError("BAD_AMOUNT", "amount must be positive")
        payer = self._require_vpa(payer_vpa)
        payee = self._require_vpa(payee_vpa)
        if payer.frozen:
            raise SwitchError("PAYER_FROZEN", f"{payer_vpa} is frozen")
        if payee.frozen:
            raise SwitchError("PAYEE_FROZEN", f"{payee_vpa} is frozen")
        payer_psp = self._require_psp(payer.psp_id)
        payee_psp = self._require_psp(payee.psp_id)
        self._assert_circuit(payer_psp)
        self._assert_circuit(payee_psp)

        now = utcnow()
        txn = Transaction(
            utr=self.snowflake.next_utr(),
            bucket=time_bucket(now),
            shard=shard_for(payer.address),
            payer_vpa=payer.address,
            payee_vpa=payee.address,
            payer_psp=payer_psp.id,
            payee_psp=payee_psp.id,
            amount_paise=amount_paise,
            note=note,
            status="initiated",
            created_at=now,
        )
        self.ledger.insert(txn, hot=True)
        self._fanout(txn, payer_psp, payee_psp)
        return txn

    def _fanout(self, txn: Transaction, payer_psp: Psp, payee_psp: Psp) -> None:
        """Debit payer PSP, credit payee PSP. Isolated failures map to FAILED."""
        try:
            self._debit(payer_psp, txn)
            self._credit(payee_psp, txn)
        except SwitchError as exc:
            txn.status = "failed"
            txn.failure_reason = exc.message
            payer_psp.failed_24h += 1
            return
        txn.status = "success"
        payer_psp.success_24h += 1
        payee_psp.success_24h += 1

    def _debit(self, psp: Psp, txn: Transaction) -> None:
        if psp.draining:
            raise SwitchError("PSP_DRAINING", f"{psp.id} is draining")
        psp.inflight += 1
        psp.inflight -= 1

    def _credit(self, psp: Psp, txn: Transaction) -> None:
        if psp.draining:
            raise SwitchError("PSP_DRAINING", f"{psp.id} is draining")

    def _require_vpa(self, address: str) -> Vpa:
        vpa = self.vpas.get(address)
        if not vpa:
            raise SwitchError("VPA_NOT_FOUND", f"unknown VPA {address}")
        return vpa

    def _require_psp(self, psp_id: str) -> Psp:
        psp = self.psps.get(psp_id)
        if not psp:
            raise SwitchError("PSP_NOT_FOUND", f"unknown PSP {psp_id}")
        return psp

    def _assert_circuit(self, psp: Psp) -> None:
        if psp.circuit == "open":
            raise SwitchError("CIRCUIT_OPEN", f"{psp.id} circuit is open")
        if psp.circuit == "half" and psp.inflight > 0:
            raise SwitchError("CIRCUIT_HALF", f"{psp.id} half-open, probe in flight")
