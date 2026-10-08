"""Read original STK orders and cancel only the current API client's order."""

import asyncio
from decimal import Decimal
from hashlib import sha256

from algo_trader_broker_sdk import SubmissionGate, BrokerContractError
from algo_trader_broker_sdk.cash_control import CashOrderQuery, CashOrderState, CashOrderEvidence
from algo_trader_broker_sdk.options import check

from .options_account_native import open_orders, completed_orders, positions, raw_bytes
from .options_orders import transport_ready
from .options_reads import native_decimal, now_wire

STATUS = {"Submitted": "WORKING", "PreSubmitted": "WORKING", "PendingSubmit": "WORKING",
          "PendingCancel": "CANCEL_PENDING", "Cancelled": "CANCELLED", "ApiCancelled": "CANCELLED",
          "Filled": "FILLED"}


async def validate_close(ib, contract, order):
    """A stock CLOSE reduces the current native position without crossing zero."""
    check(bool(order.account) and not order.modelCode and contract.currency == "USD",
          "Cash close requires an explicit native USD account")
    details = await asyncio.wait_for(ib.reqContractDetailsAsync(contract), 15)
    check(len(details) == 1, "Cash close contract did not qualify uniquely")
    exact = details[0].contract
    check(exact.secType == "STK" and exact.symbol == contract.symbol and exact.currency == contract.currency
          and exact.conId > 0 and (not contract.conId or exact.conId == contract.conId),
          "Cash close changed its qualified asset")
    snapshot = await positions(ib, order.account, timeout=15)
    matches = [row for row in snapshot["positions"] if row["contract"]["conId"] == exact.conId]
    quantity = Decimal(str(order.totalQuantity))
    native = Decimal(matches[0]['quantity']) if len(matches) == 1 else Decimal(0)
    check(len(matches) == 1 and matches[0]["contract"]["secType"] == "STK"
          and matches[0]["contract"]["symbol"] == exact.symbol and matches[0]["contract"]["currency"] == "USD"
          and quantity.is_finite() and order.action in {'BUY', 'SELL'}
          and ((order.action == 'SELL' and native > 0) or (order.action == 'BUY' and native < 0))
          and 0 < quantity <= abs(native), "Cash close exceeds the native position in its reducing direction")
    contract.conId = exact.conId


async def native_read(adapter, ib, bound, request):
    check(type(request) is CashOrderQuery, "Cash control requires an exact scoped query")
    async with adapter._option_account_read_lock:
        working = await open_orders(ib, bound.native_account_ref, timeout=adapter._qualification_timeout)
        completed = await completed_orders(ib, bound.native_account_ref, timeout=adapter._qualification_timeout)
    matches = {}
    for page, done in ((working, False), (completed, True)):
        for item in page["orders"]:
            order = item["order"]
            if order["orderRef"] == request.client_order_id and order["account"] == bound.native_account_ref:
                check(type(order["permId"]) is int and order["permId"] > 0, "Native cash order lacks its permanent identity")
                matches[order["permId"]] = (item, done)
    check(len(matches) == 1, "The original cash order was not uniquely observed")
    item, done = next(iter(matches.values()))
    order, contract = item["order"], item["contract"]
    check(contract["secType"] == "STK" and contract["symbol"] == request.symbol and contract["currency"] == "USD"
          and "IBKR:" + str(contract["conId"]) == request.instrument_id
          and order["action"] == request.side and order["orderType"] == "LMT"
          and Decimal(str(order["totalQuantity"])) == Decimal(request.quantity)
          and Decimal(str(order["lmtPrice"])) == Decimal(request.limit_price) and order["tif"] == request.tif
          and not order["whatIf"] and not order["modelCode"], "Native cash order changed its admitted economics")
    if not done and request.broker_order_id is not None:
        check(str(order["orderId"]) == request.broker_order_id, "Native cash order ID differs")
    filled = None
    # completedOrder carries cumulative filledQuantity. An empty execution
    # download or a working-order end marker is never evidence of zero fills.
    if done:
        try:
            filled = native_decimal(order["filledQuantity"])
        except (BrokerContractError, KeyError):
            pass
    raw = raw_bytes(dict(source="IB_CASH_ORDER", completed=done, account=bound.native_account_ref, observation=item))
    # completedOrder omits the API orderId. Keep the permanent identity for
    # Account execution matching; the original query still carries the API ID
    # used by the current client's cancellation port.
    native_id = "IBKR_PERM_ID:" + str(order["permId"])
    state = CashOrderState(request, native_id, STATUS.get(item["state"]["status"], "UNKNOWN"), filled,
        sha256(raw).hexdigest(), now_wire())
    return CashOrderEvidence(state, raw), item


async def read(adapter, request):
    async def operation(ib, bound):
        evidence, _ = await native_read(adapter, ib, bound, request)
        return evidence
    return await adapter._option_read(request, operation)


async def cancel(adapter, request, gate):
    check(type(gate) is SubmissionGate and gate.retain_native is not None,
          "Cash cancellation requires the durable Runner gate")
    async def operation(ib, bound):
        evidence, item = await native_read(adapter, ib, bound, request)
        if evidence.state.status in {"CANCELLED", "EXPIRED", "REJECTED", "FILLED"}:
            return "ALREADY_TERMINAL"
        order = item["order"]
        check(evidence.state.status == "WORKING" and order["orderId"] > 0
              and order["clientId"] == ib.client.clientId == ib.wrapper.clientId,
              "Cash cancellation needs a working order owned by this API client")
        async with asyncio.timeout(adapter._qualification_timeout):
            while not transport_ready(ib.client):
                await asyncio.sleep(0.01)
        await gate.prepare()
        await gate.record_native(raw_bytes(dict(source="IB_CASH_CANCEL", account=bound.native_account_ref,
            client_id=order["clientId"], order_id=order["orderId"], perm_id=order["permId"],
            order_ref=order["orderRef"], observation_hash=evidence.state.raw_hash)))
        check(transport_ready(ib.client), "Native cancellation transport became queued")
        gate.consume()
        ib.client.cancelOrder(order["orderId"], "")
        return "REQUESTED"
    return await adapter._option_read(request, operation)
