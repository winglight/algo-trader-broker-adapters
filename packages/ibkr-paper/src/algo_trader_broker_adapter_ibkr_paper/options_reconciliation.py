"""Current IB order/position reads and overlapping native execution recovery."""

from collections import Counter
from decimal import Decimal
import re

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import (
    OptionLegOrderState, OptionOrderState, OptionPosition, OptionReconciliation, ReconcileOptionsRequest, check,
)
from algo_trader_broker_sdk.options_events import OptionNativeOrderQuery, OptionRawEvent

from .options_account_native import completed_orders, executions, open_orders, raw_bytes
from .options_events import SOURCE, STATUS
from .options_reads import native_decimal, now_wire


def validate_order(original, row, account):
    order, contract = row["order"], row["contract"]
    combo = len(original.legs) > 1
    check(order["account"] == account and order["orderRef"] == original.client_order_id
          and not order["whatIf"] and order["modelCode"] == ""
          and type(order["permId"]) is int and order["permId"] > 0,
          "IB native order differs from its retained account/reference")
    price = Decimal(original.signed_limit)
    check(order["action"] == ("BUY" if combo else original.legs[0].side)
          and Decimal(str(order["totalQuantity"])) == original.groups
          and Decimal(str(order["lmtPrice"])) == (price if combo else price.copy_abs())
          and order["orderType"] == "LMT" and order["tif"] == "DAY" and not order["outsideRth"]
          and order["openClose"] == ("O" if original.legs[0].position_effect == "OPEN" else "C"),
          "IB native order changed its retained quantity, limit or policy")
    check(contract["currency"] == "USD", "IB native order changed its currency")
    if combo:
        check(contract["secType"] == "BAG" and sorted((str(leg["conId"]), leg["ratio"], leg["action"], leg["openClose"])
            for leg in contract["comboLegs"]) == sorted((leg.binding.broker_contract_id, leg.ratio, leg.side, 0)
            for leg in original.legs), "IB native order changed its exact combo vector")
    else:
        check(contract["secType"] == "OPT" and str(contract["conId"]) == original.legs[0].binding.broker_contract_id
              and contract["localSymbol"] == original.legs[0].binding.local_symbol,
              "IB native order changed its exact option contract")


def order_state(original, row, fills, account):
    validate_order(original, row, account)
    order = row["order"]
    parent = str(order["permId"])
    status = STATUS.get(row["state"]["status"])
    check(status is not None, "IB native order status is unresolved")
    counts, seen = Counter(), {}
    intents = {leg.binding.broker_contract_id: leg for leg in original.legs}
    for record in fills:
        execution, contract = record["execution"], record["contract"]
        if execution["permId"] != order["permId"] or contract["secType"] == "BAG":
            continue
        native_id = str(contract["conId"])
        leg = intents.get(native_id)
        check(leg is not None and contract["secType"] == "OPT" and execution["acctNumber"] == account
              and contract["localSymbol"] == leg.binding.local_symbol and execution["orderRef"] == original.client_order_id
              and execution["side"] == ("BOT" if leg.side == "BUY" else "SLD") and execution["modelCode"] == ""
              and re.fullmatch(r"[^\s.]+(?:\.[^\s.]+)+\.01", execution["execId"]) is not None
              and not execution["pendingPriceRevision"], "IB recovered execution needs attribution or revision reconciliation")
        identity = (execution["execId"], native_id)
        quantity = Decimal(native_decimal(execution["shares"]))
        check(quantity > 0 and quantity == quantity.to_integral_value(), "IB recovered execution has non-contract quantity")
        signature = (quantity, native_decimal(execution["price"]), execution["time"])
        if identity in seen:
            check(seen[identity] == signature, "IB execution identity has conflicting observations")
            continue
        seen[identity] = signature
        counts[native_id] += int(quantity)
    legs = []
    for leg in original.legs:
        filled, target = counts[leg.binding.broker_contract_id], original.groups * leg.ratio
        check(filled <= target, "IB recovered quantity exceeds the original order")
        reference = parent if len(original.legs) == 1 else parent + ":" + leg.binding.broker_contract_id
        legs.append(OptionLegOrderState(leg.leg_id, leg.binding.canonical_id, reference, filled, target - filled))
    matched = min(leg.filled_contracts // intent.ratio for leg, intent in zip(legs, original.legs))
    residual = any(leg.filled_contracts != matched * intent.ratio for leg, intent in zip(legs, original.legs))
    check(status != "FILLED" or (matched == original.groups and not residual),
          "IB filled parent lacks its actual recovered leg executions")
    if status == "ACKNOWLEDGED" and any(counts.values()):
        status = "PARTIALLY_FILLED"
    return OptionOrderState(original.command_id, original.client_order_id, parent, status, original.groups, matched,
        original.groups - matched, tuple(legs), residual, row["received_at"])


async def native_snapshot(adapter, request, *, with_executions=True):
    async def read(ib, bound):
        stream = getattr(adapter, "_option_events", None)
        check(stream is not None and stream.ib is ib and stream.bound == bound and not stream.closed and stream.failure is None,
              "IB reconciliation requires the current durable callback sink")
        async with adapter._option_account_read_lock:
            working = await open_orders(ib, bound.native_account_ref, timeout=adapter._qualification_timeout)
            completed = await completed_orders(ib, bound.native_account_ref, timeout=adapter._qualification_timeout)
            history = (await executions(ib, bound.native_account_ref, timeout=adapter._qualification_timeout)
                       if with_executions else None)
        await stream.flush()
        return bound, working, completed, history
    return await adapter._option_read(request, read)


def native_rows(working, completed):
    records = {}
    # The completed-order request follows the working-order snapshot. Its
    # terminal observation wins if the order finished between the two reads.
    for page in (working, completed):
        for row in page["orders"]:
            if row["contract"]["secType"] not in {"OPT", "BAG"}:
                continue
            order = row["order"]
            key = (order["permId"], order["clientId"], row["order_id"]) if order["permId"] <= 0 else order["permId"]
            records[key] = row
    return tuple(records.values())


async def read_order(adapter, request):
    check(type(request) is OptionNativeOrderQuery and re.fullmatch(r"[1-9][0-9]*", request.order_id) is not None,
          "IB order lookup requires an exact permanent ID")
    bound, working, completed, _ = await native_snapshot(adapter, request, with_executions=False)
    matches = [row for row in native_rows(working, completed) if str(row["order"]["permId"]) == request.order_id]
    check(len(matches) == 1, "IB order was not uniquely observed; absence never permits resubmission")
    row = matches[0]
    # completedOrder does not supply clientId/orderId; preserve native zeros.
    kind = "openOrder" if row in working["orders"] else "completedOrder"
    return OptionRawEvent(bound.scope, SOURCE, raw_bytes(dict(kind=kind, native_account_ref=bound.native_account_ref,
        observed_at=row["received_at"], order=row["order"], contract=row["contract"], state=row["state"])), request.order_id)


async def reconcile(adapter, request, *, resolve_order):
    check(type(request) is ReconcileOptionsRequest and request.cursor is None, "IB finite reconciliation has no page cursor")
    account = await adapter.option_account_state(request)
    bound, working, completed, history = await native_snapshot(adapter, request)
    rows = native_rows(working, completed)
    references = Counter(row["order"]["orderRef"] for row in rows)
    orders, unresolved = [], ["IB_ORDER_HISTORY_WINDOW_LIMITED", "IB_EXECUTION_HISTORY_WINDOW_LIMITED",
                             "IB_MANUAL_ORDER_VISIBILITY_UNVERIFIED", "IB_LIFECYCLE_RECONCILIATION_REQUIRED"]
    for row in rows:
        order = row["order"]
        original = await resolve_order(order["orderRef"]) if resolve_order is not None and order["orderRef"] else None
        try:
            check(original is not None and references[order["orderRef"]] == 1
                  and (original.execution_target, original.account, original.environment) ==
                  (bound.scope.execution_target, bound.scope.account, bound.scope.environment),
                  "IB order has no unique retained command in this account")
            orders.append(order_state(original, row, history["executions"], bound.native_account_ref))
        except BrokerContractError:
            unresolved.append("order:" + str(order["permId"]))
    unresolved += ["position:" + item.broker_position_ref for item in account.unresolved_positions]
    positions = tuple(OptionPosition(item.binding, item.signed_contracts, None, "UNKNOWN", item.broker_as_of)
                      for item in account.positions)
    # The actual individual callbacks already reached the durable event sink.
    # A download-end marker is not full historical or fee/lifecycle coverage.
    return OptionReconciliation(tuple(orders), positions, (), (), False,
        account.positions_complete and not account.unresolved_positions, False, False,
        tuple(sorted(set(unresolved))), None, now_wire())
