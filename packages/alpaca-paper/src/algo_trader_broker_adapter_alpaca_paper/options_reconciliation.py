"""Read current option orders/positions without creating financial executions."""

from decimal import Decimal
import json

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import OptionPosition, OptionReconciliation, ReconcileOptionsRequest, check, timestamp
from algo_trader_broker_sdk.options_events import OptionRawEvent

from .options_codec import _uuid, _time
from .options_orders import order_state
from .options_reads import now_wire
from .raw_stream import decode_native, _constant, _object


def order_records(raw):
    """Yield exact native object bytes; reserializing a list loses raw proof."""
    data = decode_native(raw)
    check(type(data) is list and len(data) <= 500 and all(type(row) is dict for row in data), "Invalid native order page")
    text = raw.decode("utf-8")
    decoder = json.JSONDecoder(parse_float=Decimal, parse_constant=_constant, object_pairs_hook=_object)
    pos = text.index("[") + 1
    byte_pos, char_pos = 0, 0
    records = []
    for expected in data:
        while text[pos] in " \t\r\n,": pos += 1
        item, end = decoder.raw_decode(text, pos)
        check(item == expected, "Native order span differs from page")
        start_byte = byte_pos + len(text[char_pos:pos].encode("utf-8"))
        end_byte = start_byte + len(text[pos:end].encode("utf-8"))
        records.append((item, raw[start_byte:end_byte]))
        byte_pos, char_pos, pos = end_byte, end, end
    check(len({_uuid(item["id"]) for item, _ in records}) == len(records), "Repeated order in native page")
    return records


def option_order(item):
    return item.get("asset_class") == "us_option" or item.get("order_class") == "mleg" or bool(item.get("legs"))


async def reconcile(adapter, request, *, resolve_order):
    check(type(request) is ReconcileOptionsRequest, "Expected an exact scoped reconciliation query")
    state, bound, open_raw = await adapter._option_account_snapshot(request)
    params = dict(status="all", asset_class="us_option", direction="desc", limit=500, nested="true")
    if request.cursor is not None:
        params["before_order_id"] = _uuid(request.cursor)
    elif request.since is not None:
        params["after"] = request.since
    raw = await adapter._backend.get_option_resource("/v2/orders", params=params)
    adapter._remember_option_read(raw)
    page = order_records(raw)
    check(all(option_order(item) and item["id"] != request.cursor for item, _ in page), "Native option page changed its filter or cursor")
    # Order-ID pagination avoids dropping rows sharing a submission timestamp.
    # The provider forbids combining ID cursors with after/until, so subsequent
    # pages enforce the original lower bound locally.
    end = len(page) < 500
    history = []
    for item, original in page:
        if request.since is not None:
            submitted = _time(item.get("submitted_at") or item["created_at"])
            if timestamp(submitted) <= timestamp(request.since):
                end = True
                continue
        history.append((item, original))
    next_cursor = None if end else _uuid(page[-1][0]["id"])
    # Open orders are always included, even if submitted before the history
    # window. The later all-orders read wins for duplicate native references.
    records = {_uuid(item["id"]): (item, original) for item, original in order_records(open_raw) if option_order(item)}
    records.update({_uuid(item["id"]): (item, original) for item, original in history})
    orders, unresolved = [], ["position:" + item.broker_position_ref for item in state.unresolved_positions]
    order_unresolved = []
    for native_id, (item, original) in records.items():
        if not option_order(item):
            continue
        mapped = None
        if resolve_order is not None and type(item.get("client_order_id")) is str:
            mapped = await resolve_order(item["client_order_id"])
        if mapped is None:
            order_unresolved.append("order:" + native_id)
            continue
        check((mapped.execution_target, mapped.account, mapped.environment) ==
            (bound.scope.execution_target, bound.scope.account, bound.scope.environment), "Reconciliation mapping changed account")
        try:
            parsed = order_state(mapped, original)
        except (BrokerContractError, KeyError, ValueError, TypeError):
            order_unresolved.append("order:" + native_id)
            continue
        adapter._option_still_bound(bound)
        sink = adapter._option_evidence_handler
        check(sink is not None and adapter._option_event_binding == bound, "Order reconciliation requires the existing durable evidence sink")
        await sink(OptionRawEvent(bound.scope, "ALPACA_ORDER_DETAIL", original, native_id))
        orders.append(parsed)
    adapter._option_still_bound(bound)
    unresolved += order_unresolved
    if not state.orders_complete: unresolved.append("OPEN_ORDER_PAGE_INCOMPLETE")
    positions = tuple(OptionPosition(item.binding, item.signed_contracts,
        item.raw_cost_value if item.raw_cost_unit == "CASH_TOTAL" else None,
        "CASH_TOTAL" if item.raw_cost_unit == "CASH_TOTAL" else "UNKNOWN", item.broker_as_of) for item in state.positions)
    # Cumulative native quantities describe status, never individual fills.
    # The independent execution/fee/lifecycle journal still owns completeness.
    return OptionReconciliation(tuple(orders), positions, (), (), end and state.orders_complete and not order_unresolved,
        state.positions_complete and not state.unresolved_positions, False, False,
        tuple(sorted(set(unresolved))), next_cursor, now_wire())
