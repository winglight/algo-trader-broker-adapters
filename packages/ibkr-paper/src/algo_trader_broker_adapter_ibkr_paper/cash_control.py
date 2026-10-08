"""Read original STK orders and cancel only the current API client's order."""

import asyncio
from decimal import Decimal
from hashlib import sha256

from algo_trader_broker_sdk import SubmissionGate, BrokerContractError
from algo_trader_broker_sdk.cash_control import CashOrderQuery, CashOrderState, CashOrderEvidence
from algo_trader_broker_sdk.options import check

from .options_account_native import open_orders, completed_orders, raw_bytes
from .options_orders import transport_ready
from .options_reads import native_decimal, now_wire

STATUS = {"Submitted": "WORKING", "PreSubmitted": "WORKING", "PendingSubmit": "WORKING",
          "PendingCancel": "CANCEL_PENDING", "Cancelled": "CANCELLED", "ApiCancelled": "CANCELLED",
          "Filled": "FILLED"}


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
    native_id = str(order["orderId"]) if order["orderId"] > 0 else request.broker_order_id or str(order["permId"])
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
