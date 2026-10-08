"""Exact original cash-order reads and one guarded cancellation."""

from decimal import Decimal
from hashlib import sha256
import json

from algo_trader_broker_sdk import SubmissionGate
from algo_trader_broker_sdk.cash_control import CashOrderQuery, CashOrderState, CashOrderEvidence
from algo_trader_broker_sdk.options import check

from .options_codec import _decimal, _uuid
from .options_reads import now_wire
from .raw_stream import decode_native

STATUS = {"new": "WORKING", "accepted": "WORKING", "pending_new": "WORKING", "partially_filled": "WORKING",
          "pending_cancel": "CANCEL_PENDING", "canceled": "CANCELLED", "expired": "EXPIRED",
          "rejected": "REJECTED", "filled": "FILLED"}


async def read(adapter, request):
    check(type(request) is CashOrderQuery, "Cash control requires an exact scoped query")
    bound, _, _ = await adapter._option_bound(request)
    if request.broker_order_id is None:
        path, params = "/v2/orders:by_client_order_id", {"client_order_id": request.client_order_id}
    else:
        path, params = "/v2/orders/" + _uuid(request.broker_order_id), {"nested": "true"}
    raw = await adapter._backend.get_option_resource(path, params=params)
    item = decode_native(raw)
    check(type(item) is dict and item.get("asset_class") == "us_equity"
          and item.get("order_class") in {None, "", "simple"} and not item.get("legs"),
          "Cash control cannot act on option or bracket orders")
    native_id = _uuid(item["id"])
    check(request.broker_order_id in {None, native_id} and item["client_order_id"] == request.client_order_id
          and "ALPACA:" + _uuid(item["asset_id"]) == request.instrument_id
          and item["symbol"] == request.symbol and item["side"].upper() == request.side
          and item["type"] == "limit" and item["time_in_force"].upper() == request.tif
          and Decimal(_decimal(item["qty"])) == Decimal(request.quantity)
          and Decimal(_decimal(item["limit_price"])) == Decimal(request.limit_price),
          "Native order differs from the admitted instrument, quantity or limit")
    # The account endpoint and order endpoint use the same fixed credentials.
    # Preserve a native account field when present and reject any mismatch.
    check(item.get("account_id", bound.native_account_ref) == bound.native_account_ref, "Native order account differs")
    filled = None if item.get("filled_qty") is None else _decimal(item["filled_qty"])
    adapter._option_still_bound(bound)
    state = CashOrderState(request, native_id, STATUS.get(item.get("status"), "UNKNOWN"), filled,
        sha256(raw).hexdigest(), now_wire())
    return CashOrderEvidence(state, raw)


async def cancel(adapter, request, gate):
    check(type(gate) is SubmissionGate and gate.retain_native is not None, "Cash cancellation requires the durable Runner send gate")
    evidence = await read(adapter, request)
    if evidence.state.status in {"CANCELLED", "EXPIRED", "REJECTED", "FILLED"}:
        return "ALREADY_TERMINAL"
    check(evidence.state.status == "WORKING", "Native cash order must be observed working before cancellation")
    prepared = json.dumps(dict(source="ALPACA_CASH_CANCEL", method="DELETE",
        order_id=evidence.state.broker_order_id, client_order_id=request.client_order_id,
        observation_hash=evidence.state.raw_hash), sort_keys=True, separators=(",", ":")).encode()
    return await adapter._backend.cancel_order_raw(evidence.state.broker_order_id,
        submission_gate=gate, native_preparation=prepared)
