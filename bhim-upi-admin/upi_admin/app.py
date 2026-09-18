"""BHIM UPI system administrative console.

Simulation only — no NPCI, bank, or production credentials.
Run: python run_upi_admin.py
"""

from __future__ import annotations

import os
from functools import wraps

from flask import Flask, jsonify, render_template, request

from .control_plane import ControlPlane
from .ids import SnowflakeFactory, decode_snowflake
from .ledger import PartitionedLedger
from .seed import bootstrap
from .switch import SwitchError, UpiSwitch

HERE = os.path.dirname(os.path.abspath(__file__))


def create_admin_app() -> Flask:
    app = Flask(
        __name__,
        template_folder=os.path.join(HERE, "templates"),
        static_folder=os.path.join(HERE, "static"),
    )
    app.config["SECRET_KEY"] = os.environ.get("UPI_ADMIN_SECRET", "dev-upi-admin")
    app.config["ADMIN_TOKEN"] = os.environ.get("UPI_ADMIN_TOKEN", "dev-admin")

    ledger = PartitionedLedger()
    snowflake = SnowflakeFactory(shard_id=3)
    switch = UpiSwitch(ledger, snowflake)
    bootstrap(switch, ledger, snowflake)
    plane = ControlPlane(switch, ledger)
    app.extensions["upi"] = {"plane": plane, "switch": switch, "ledger": ledger}

    def require_admin(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            token = request.headers.get("X-Admin-Token") or request.args.get("token")
            if token != app.config["ADMIN_TOKEN"]:
                return jsonify({"error": "unauthorized", "hint": "send X-Admin-Token"}), 401
            return fn(*args, **kwargs)

        return wrapper

    def actor() -> str:
        return request.headers.get("X-Admin-Actor", "ops@upi")

    @app.errorhandler(SwitchError)
    def on_switch_error(err: SwitchError):
        return jsonify({"error": err.code, "message": err.message}), 400

    @app.get("/")
    def console():
        return render_template("console.html")

    @app.get("/admin/api/dashboard")
    @require_admin
    def dashboard():
        return jsonify(plane.dashboard())

    @app.get("/admin/api/psps")
    @require_admin
    def psps():
        return jsonify(plane.list_psps())

    @app.post("/admin/api/psps")
    @require_admin
    def add_psp():
        body = request.get_json(force=True, silent=True) or {}
        psp = plane.add_psp(
            actor(),
            psp_id=str(body.get("id") or body.get("psp_id") or ""),
            name=str(body.get("name") or ""),
            handle=str(body.get("handle") or ""),
            bank_id=str(body.get("bank_id") or "sbi"),
            rps_limit=int(body.get("rps_limit") or 800),
        )
        return jsonify({"ok": True, "id": psp.id, "handle": psp.handle})

    @app.post("/admin/api/psps/<psp_id>/circuit")
    @require_admin
    def circuit(psp_id: str):
        body = request.get_json(force=True, silent=True) or {}
        plane.set_circuit(actor(), psp_id, body.get("state", "closed"))
        return jsonify({"ok": True, "psps": plane.list_psps()})

    @app.post("/admin/api/psps/<psp_id>/drain")
    @require_admin
    def drain(psp_id: str):
        body = request.get_json(force=True, silent=True) or {}
        plane.drain_psp(actor(), psp_id, bool(body.get("draining", True)))
        return jsonify({"ok": True})

    @app.post("/admin/api/psps/<psp_id>/settle")
    @require_admin
    def settle(psp_id: str):
        batch = plane.settle_psp(actor(), psp_id)
        return jsonify(
            {
                "id": batch.id,
                "psp_id": batch.psp_id,
                "txn_count": batch.txn_count,
                "net_inr": round(batch.net_paise / 100, 2),
                "status": batch.status,
            }
        )

    @app.get("/admin/api/vpas")
    @require_admin
    def vpas():
        return jsonify(plane.list_vpas())

    @app.post("/admin/api/vpas")
    @require_admin
    def add_vpa():
        body = request.get_json(force=True, silent=True) or {}
        vpa = plane.add_vpa(
            actor(),
            address=str(body.get("address") or ""),
            owner_name=str(body.get("owner_name") or ""),
            psp_id=str(body.get("psp_id") or ""),
            bank_id=str(body.get("bank_id") or ""),
            account_ref=str(body.get("account_ref") or ""),
        )
        return jsonify({"ok": True, "address": vpa.address})

    @app.post("/admin/api/vpas/state")
    @require_admin
    def vpa_state():
        body = request.get_json(force=True, silent=True) or {}
        plane.freeze_vpa(
            actor(),
            str(body.get("address") or ""),
            bool(body.get("frozen")),
            str(body.get("reason") or "ops"),
        )
        return jsonify({"ok": True})

    @app.get("/admin/api/transactions")
    @require_admin
    def transactions():
        utr = request.args.get("utr")
        if utr:
            payload = plane.lookup(utr)
            payload["snowflake"] = decode_snowflake(utr)
            return jsonify([payload])
        rows = plane.ledger.search(
            vpa=request.args.get("vpa"),
            status=request.args.get("status"),
            limit=int(request.args.get("limit", 50)),
        )
        return jsonify([t.to_public() for t in rows])

    @app.post("/admin/api/transactions/<utr>/reverse")
    @require_admin
    def reverse(utr: str):
        body = request.get_json(force=True, silent=True) or {}
        return jsonify(plane.reverse(actor(), utr, body.get("reason", "ops reverse")))

    @app.post("/admin/api/transactions/<utr>/dispute")
    @require_admin
    def dispute(utr: str):
        body = request.get_json(force=True, silent=True) or {}
        d = plane.open_dispute(actor(), utr, body.get("reason", "customer claim"))
        return jsonify({"id": d.id, "utr" : d.utr, "status": d.status, "reason": d.reason})

    @app.post("/admin/api/pay")
    @require_admin
    def pay():
        body = request.get_json(force=True, silent=True) or {}
        amount_paise = body.get("amount_paise")
        if amount_paise in (None, "", 0) and body.get("amount_inr") not in (None, ""):
            amount_paise = int(round(float(body["amount_inr"]) * 100))
        else:
            amount_paise = int(amount_paise or 0)
        txn = switch.collect_pay(
            payer_vpa=str(body.get("payer_vpa") or ""),
            payee_vpa=str(body.get("payee_vpa") or ""),
            amount_paise=amount_paise,
            note=str(body.get("note") or "admin simulate"),
        )
        plane.record(actor(), "txn.simulate", txn.utr, txn.note)
        return jsonify(txn.to_public())

    @app.post("/admin/api/ledger/archive")
    @require_admin
    def archive():
        moved = plane.archive(actor())
        return jsonify({"moved": moved, "shards": ledger.shard_stats()})

    @app.get("/admin/api/shards")
    @require_admin
    def shards():
        return jsonify(
            {
                "shards": ledger.shard_stats(),
                "hot_partitions": ledger.hot_partitions(),
                "coalesce": {
                    "hits": ledger.coalescer.hits,
                    "saved": ledger.coalescer.coalesced,
                },
            }
        )

    @app.get("/admin/api/audit")
    @require_admin
    def audit():
        return jsonify([e.to_public() for e in plane.audit[:100]])

    return app
