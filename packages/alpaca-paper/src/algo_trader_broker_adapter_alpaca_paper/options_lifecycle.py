"""Exact option NTAs and their independently reported underlying delivery."""

from datetime import date
from decimal import Decimal, InvalidOperation, localcontext
from hashlib import sha256
import json
from urllib.parse import quote

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import ActivityQuery, OptionActivityPage, OptionLifecycleEvent, check

from .options_reads import contract_from_native, now_wire, signed_decimal, whole
from .raw_stream import decode_native, _constant, _object

KINDS = {"OPEXC": "EXERCISE", "OPXRC": "EXERCISE", "OPASN": "ASSIGNMENT", "OPEXP": "EXPIRATION"}


def records(raw):
    data = decode_native(raw)
    check(type(data) is list and len(data) <= 100 and all(type(row) is dict for row in data), "Invalid lifecycle page")
    text = raw.decode("utf-8")
    decoder = json.JSONDecoder(parse_float=Decimal, parse_constant=_constant, object_pairs_hook=_object)
    pos, result = text.index("[") + 1, []
    for expected in data:
        while text[pos] in " \t\r\n,": pos += 1
        item, end = decoder.raw_decode(text, pos)
        check(item == expected, "Lifecycle raw span changed")
        result.append((item, text[pos:end].encode("utf-8")))
        pos = end
    check(len({(row["activity_type"], row["id"]) for row, _ in result}) == len(result), "Repeated lifecycle record")
    return result


def event(parent, delivery, contract, *, observed, raw_ref):
    kind = KINDS[parent["activity_type"]]
    day = parent["date"]
    check(date.fromisoformat(day).isoformat() == day and parent["status"] in {"executed", "correct", "canceled"},
          "Lifecycle date or status is unresolved")
    qty = Decimal(signed_decimal(parent["qty"]))
    delta = whole(qty.copy_abs()) * (-1 if qty < 0 else 1)
    check(delta != 0 and signed_decimal(parent["net_amount"]) == "0", "Unrecognized option lifecycle quantity or cash")
    check((kind != "EXERCISE" or delta < 0) and (kind != "ASSIGNMENT" or delta > 0), "Lifecycle direction changed")
    shares, cash = 0, "0"
    if kind != "EXPIRATION":
        check(delivery is not None and delivery["activity_type"] == "OPTRD"
              and (delivery["id"], delivery["date"], delivery["symbol"], delivery["status"]) ==
              (parent["id"], day, contract.key.underlying, parent["status"]), "Matching native delivery is required")
        quantity = Decimal(signed_decimal(delivery["qty"]))
        shares = whole(quantity.copy_abs()) * (-1 if quantity < 0 else 1)
        check(shares == -delta * contract.key.multiplier * (1 if contract.key.right == "C" else -1), "Delivery quantity differs from contract")
        cash = signed_decimal(delivery["net_amount"])
        with localcontext() as context:
            context.prec = 100
            check(Decimal(cash) == -Decimal(shares) * Decimal(contract.key.strike), "Delivery cash differs from strike economics")
    return OptionLifecycleEvent(kind + ":" + parent["id"], kind, contract.canonical_id, delta,
        contract.key.underlying, shares, cash, contract.key.currency, None, observed,
        "ALPACA_OPTION_NTA", 1, None, raw_ref, effective_date=day)


async def read(adapter, request, *, retain_evidence, lifecycle_state=None):
    check(type(request) is ActivityQuery and retain_evidence is not None, "Lifecycle reads require a scoped query and durable evidence archive")
    bound, _, _ = await adapter._option_bound(request)
    async def retain(raw):
        # Persist before interpretation, including unsupported native records.
        reference = await retain_evidence(raw)
        check(reference == sha256(raw).hexdigest(), "Lifecycle archive changed raw evidence")
        adapter._option_still_bound(bound)
        return reference
    async def page(types, cursor, *, since=None, limit=100):
        params = dict(activity_types=types, direction="desc", page_size=limit)
        if cursor is not None: params["page_token"] = cursor
        if since is not None: params["after"] = since
        raw = await adapter._backend.get_option_resource("/v2/account/activities", params=params)
        await retain(raw)
        rows = records(raw)
        check(all(row["activity_type"] in types.split(",") and row["id"] != cursor for row, _ in rows), "Lifecycle page changed filter or cursor")
        return rows

    # OPTRD is paged separately: option and delivery records can share the
    # entire activity ID, so a mixed page boundary could skip the second row.
    limit = min(request.limit, 100)
    parents = await page(",".join(KINDS), request.cursor, since=request.since, limit=limit)
    next_cursor = parents[-1][0]["id"] if len(parents) == limit else None
    wanted = {row["id"] for row, _ in parents if row["activity_type"] != "OPEXP"}
    deliveries, ambiguous, cursor, seen = {}, set(), None, set()
    if wanted:
        for _ in range(100):
            rows = await page("OPTRD", cursor)
            for row, raw in rows:
                if row["id"] in wanted:
                    if row["id"] in deliveries: ambiguous.add(row["id"])
                    deliveries[row["id"]] = (row, raw)
            if len(rows) < 100 or wanted <= deliveries.keys(): break
            cursor = rows[-1][0]["id"]
            check(cursor not in seen, "Delivery pagination did not advance")
            seen.add(cursor)

    events, unresolved = [], []
    for parent, original in parents:
        identity = parent["activity_type"] + ":" + parent["id"]
        try:
            check(parent["id"] not in ambiguous, "Native delivery identity is ambiguous")
            symbol = parent["symbol"]
            check(type(symbol) is str and 0 < len(symbol) <= 64, "Missing lifecycle contract symbol")
            raw = await adapter._backend.get_option_resource("/v2/options/contracts/" + quote(symbol, safe=""))
            await retain(raw)
            native = decode_native(raw)
            check(native["symbol"] == symbol, "Lifecycle contract lookup changed native symbol")
            contract = contract_from_native(native)
            observed = now_wire()
            adapter._remember_contract(bound.scope, native, contract, raw, observed)
            delivery, delivery_raw = deliveries.get(parent["id"], (None, None))
            proof = b"[" + original + (b"," + delivery_raw if delivery_raw is not None else b"") + b"]"
            reference = await retain(proof)
            item = event(parent, delivery, contract, observed=observed, raw_ref=reference)
            if lifecycle_state is not None:
                item = await lifecycle_state.record(item, parent["status"])
            else:
                check(parent["status"] == "executed", "Native corrections require durable lifecycle state")
            events.append(item)
        except (BrokerContractError, KeyError, ValueError, TypeError, InvalidOperation):
            unresolved.append(identity)
    adapter._option_still_bound(bound)
    adapter._option_lifecycle_observed = (bound.scope, now_wire())
    return OptionActivityPage(tuple(events), next_cursor, next_cursor is None and not unresolved, now_wire(), tuple(unresolved))
