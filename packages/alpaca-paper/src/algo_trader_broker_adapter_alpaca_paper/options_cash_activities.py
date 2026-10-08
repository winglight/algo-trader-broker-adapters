"""Booked cash fills used to prove an admitted order's funds are reflected."""

from decimal import InvalidOperation
from hashlib import sha256

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import check
from algo_trader_broker_sdk.options_account import NativeCashExecution

from .options_codec import _decimal, _time, _uuid
from .options_lifecycle import records
from .raw_stream import decode_native


async def read(adapter, scope, *, retain_evidence):
    bound, _, _ = await adapter._option_bound(scope)
    executions, cursor, seen, orders = [], None, set(), {}

    async def retain(raw):
        reference = await retain_evidence(raw)
        check(reference == sha256(raw).hexdigest(), "Cash activity archive changed native evidence")
        adapter._option_still_bound(bound)
        return reference

    for _ in range(100):
        params = dict(activity_types="FILL", direction="desc", page_size=100)
        if cursor is not None:
            params["page_token"] = cursor
        raw = await adapter._backend.get_option_resource("/v2/account/activities", params=params)
        await retain(raw)
        rows = records(raw)
        check(all(item["activity_type"] == "FILL" and item["id"] not in seen for item, _ in rows),
              "Cash activity pagination changed filter or repeated an identity")
        for item, original in rows:
            seen.add(item["id"])
            activity_ref = await retain(original)
            try:
                order_id = _uuid(item["order_id"])
            except (BrokerContractError, KeyError, ValueError, TypeError):
                continue
            if order_id not in orders:
                order_raw = await adapter._backend.get_option_resource("/v2/orders/" + order_id, params={"nested": "true"})
                order_ref = await retain(order_raw)
                orders[order_id] = decode_native(order_raw), order_ref
            order, order_ref = orders[order_id]
            try:
                check(type(order) is dict and _uuid(order["id"]) == order_id
                      and order.get("account_id", bound.native_account_ref) == bound.native_account_ref,
                      "Cash activity order changed account or native identity")
                # Option activities continue through the existing option journal.
                if order.get("asset_class") != "us_equity":
                    continue
                check(order.get("order_class") in {None, "", "simple"} and not order.get("legs")
                      and item.get("type") in {"fill", "partial_fill"}
                      and item.get("side") in {"buy", "sell"}
                      and item.get("symbol") == order.get("symbol") and item["side"] == order.get("side")
                      and item.get("currency", "USD") == "USD", "Cash activity economics are unresolved")
                prefix, separator, execution = item["id"].rpartition("::")
                check(bool(prefix) and separator == "::", "Retain the original FILL activity identity")
                executions.append(NativeCashExecution("ALPACA_ACCOUNT_FILL", item["id"], _uuid(execution),
                    order_id, "ALPACA:" + _uuid(order["asset_id"]), item["symbol"], item["side"].upper(),
                    _decimal(item["qty"]), _decimal(item["price"]), "USD", _time(item["transaction_time"]),
                    activity_ref, order_ref))
            except (BrokerContractError, KeyError, ValueError, TypeError, InvalidOperation):
                # Missing/uninterpretable entries cannot satisfy Risk's exact
                # per-fill match. Raw evidence remains available for reconciliation.
                continue
        if len(rows) < 100:
            break
        cursor = rows[-1][0]["id"]
    adapter._option_still_bound(bound)
    # A bounded observation is not a claim of full native execution coverage.
    return tuple(executions)
