from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .ids import SnowflakeFactory
from .ledger import PartitionedLedger, shard_for, time_bucket
from .models import Bank, Psp, Transaction, Vpa
from .switch import UpiSwitch


def bootstrap(switch: UpiSwitch, ledger: PartitionedLedger, snowflake: SnowflakeFactory) -> None:
    banks = [
        Bank("sbi", "State Bank of India", "SBIN", "SETTLE-SBI-001"),
        Bank("hdfc", "HDFC Bank", "HDFC", "SETTLE-HDFC-001"),
        Bank("icici", "ICICI Bank", "ICIC", "SETTLE-ICICI-001"),
    ]
    for bank in banks:
        switch.banks[bank.id] = bank

    psps = [
        Psp("bhim", "BHIM UPI", "@upi", "sbi", rps_limit=1200),
        Psp("phonepe", "PhonePe", "@ybl", "icici", rps_limit=2000),
        Psp("gpay", "Google Pay", "@okhdfcbank", "hdfc", rps_limit=1800),
        Psp("paytm", "Paytm", "@paytm", "hdfc", rps_limit=900, circuit="open"),
    ]
    for psp in psps:
        switch.register_psp(psp)

    vpas = [
        Vpa("lalit@upi", "Lalit", "bhim", "sbi", "****4521"),
        Vpa("ops@upi", "BHIM Ops", "bhim", "sbi", "****8800"),
        Vpa("merchant@ybl", "Kirana Mart", "phonepe", "icici", "****1190"),
        Vpa("payroll@okhdfcbank", "Acme Payroll", "gpay", "hdfc", "****3344"),
        Vpa("risk@paytm", "Held Wallet", "paytm", "hdfc", "****0002", frozen=True),
    ]
    for vpa in vpas:
        switch.register_vpa(vpa)

    now = datetime.now(timezone.utc)
    samples = [
        ("lalit@upi", "merchant@ybl", 25_000, "chai + samosa", "success", 0),
        ("payroll@okhdfcbank", "lalit@upi", 85_000_00, "salary", "success", 0),
        ("lalit@upi", "ops@upi", 1_00, "test ping", "success", 0),
        ("lalit@upi", "merchant@ybl", 4_99_00, "weekly grocery", "success", 1),
        ("ops@upi", "merchant@ybl", 12_00, "failed timeout", "failed", 0),
    ]
    for payer, payee, amount, note, status, days_ago in samples:
        created = now - timedelta(days=days_ago)
        payer_v = switch.vpas[payer]
        payee_v = switch.vpas[payee]
        txn = Transaction(
            utr=snowflake.next_utr(),
            bucket=time_bucket(created),
            shard=shard_for(payer),
            payer_vpa=payer,
            payee_vpa=payee,
            payer_psp=payer_v.psp_id,
            payee_psp=payee_v.psp_id,
            amount_paise=amount,
            note=note,
            status=status,  # type: ignore[arg-type]
            created_at=created,
            failure_reason="psp timeout" if status == "failed" else None,
        )
        ledger.insert(txn, hot=days_ago < 2)

    hot_payer = "lalit@upi"
    for i in range(18):
        created = now
        txn = Transaction(
            utr=snowflake.next_utr(),
            bucket=time_bucket(created),
            shard=shard_for(hot_payer),
            payer_vpa=hot_payer,
            payee_vpa="merchant@ybl",
            payer_psp="bhim",
            payee_psp="phonepe",
            amount_paise=100 + i,
            note=f"burst {i}",
            status="success",
            created_at=created,
        )
        ledger.insert(txn, hot=True)
        switch.psps["bhim"].success_24h += 1
        switch.psps["phonepe"].success_24h += 1
