"""Native what-if observations; never an admission or submission authority."""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import OptionExecutionRequest, OrderPreview, check, timestamp

from .options_account import signed_decimal
from .options_account_native import account_values, observe_callbacks, raw_bytes, raw_value
from .options_orders import build_order, wait_transport
from .options_quotes import amount
from .options_reads import now_wire


async def preview_order(adapter, request):
    check(type(request) is OptionExecutionRequest, "IB preview needs an exact option order")

    async def read(ib, bound):
        async with adapter._option_account_read_lock:
            values = await account_values(ib, bound.native_account_ref, timeout=adapter._qualification_timeout)
            usd = any(row["tag"] == "Currency" and row["currency"] == "BASE" and row["value"] == "USD"
                      for row in values["values"])
            contracts = []
            for leg in request.legs:
                cached = adapter._option_catalog.get(leg.binding.broker_contract_id)
                check(cached is not None and cached[0] == leg.binding
                      and 0 <= (datetime.now(timezone.utc) - timestamp(leg.binding.qualified_at)).total_seconds() < 30,
                      "IB preview requires current exact contract bindings")
                contracts.append(cached[2].contract)
            contract, order = build_order(request, contracts, bound.native_account_ref,
                ib.client.clientId, ib.client.getReqId(), None, preview=True)
            # A preview gets its own native request ID and can never turn into
            # a live submission, including after a timeout or reconnect.
            result, ready, errors = [], asyncio.Event(), []
            def opened(order_id, native_contract, native_order, state):
                if order_id != order.orderId:
                    return
                result.append((raw_value(native_contract), raw_value(native_order), raw_value(state)))
                ready.set()
            def error(req_id, code, message, *args):
                if req_id == order.orderId:
                    errors.append(f"IB preview rejected: {code}")
                    ready.set()
            await wait_transport(ib.client, request)
            connection = adapter._client.connection_state_snapshot()
            check(adapter._option_account_binding == bound and connection.get("connected")
                  and connection.get("connected_since") == adapter._option_connection
                  and ib.isConnected() and bound.native_account_ref in ib.managedAccounts()
                  and datetime.now(timezone.utc) < timestamp(request.valid_until) and order.whatIf is True,
                  "IB preview connection or request expired")
            ib.errorEvent += error
            try:
                with observe_callbacks(ib.wrapper, {"openOrder": opened}):
                    ib.client.placeOrder(order.orderId, contract, order)
                    await asyncio.wait_for(ready.wait(), adapter._qualification_timeout)
            finally:
                ib.errorEvent -= error
            observed = now_wire()
            adapter._option_account_evidence.append(raw_bytes(dict(source="IB_OPTION_WHAT_IF", observed_at=observed,
                account=values, contract=contract, order=order, observations=result, errors=errors)))
            check(not errors and len(result) == 1, "IB preview did not return one unambiguous what-if response")
            native_contract, native_order, state = result[0]
            check(native_order["whatIf"] is True and native_order["account"] == bound.native_account_ref
                  and native_order["orderId"] == order.orderId and native_order["clientId"] == order.clientId
                  and native_order["orderRef"] == request.client_order_id
                  and native_order["action"] == order.action
                  and Decimal(str(native_order["totalQuantity"])) == request.groups
                  and Decimal(native_order["lmtPrice"]) == Decimal(str(order.lmtPrice))
                  and (native_order["orderType"], native_order["tif"], native_order["openClose"], native_order["outsideRth"]) ==
                      (order.orderType, order.tif, order.openClose, False)
                  and native_contract["secType"] == contract.secType,
                  "IB what-if response changed its account or order")
            if len(contracts) == 1:
                check(native_contract["conId"] == contract.conId, "IB preview changed the qualified option")
            else:
                check([(leg["conId"], leg["ratio"], leg["action"], leg["exchange"], leg["openClose"])
                       for leg in native_contract["comboLegs"]] ==
                      [(leg.conId, leg.ratio, leg.action, leg.exchange, leg.openClose) for leg in contract.comboLegs],
                      "IB preview changed the exact combo vector")
            warnings = ["IB_INITIAL_MARGIN_CHANGE_ONLY", "INTERIM_ASSIGNMENT_FUNDING_NOT_ESTIMATED",
                        "EMERGENCY_CLOSE_FEES_NOT_INCLUDED"]
            try:
                margin = signed_decimal(state["initMarginChange"]) if usd else None
            except BrokerContractError:
                margin = None
            if margin is None:
                warnings.append("IB_USD_MARGIN_UNAVAILABLE")
            else:
                margin = str(max(Decimal(0), Decimal(margin)))
            fee = amount(state["commission"]) if state["commissionCurrency"] == "USD" else None
            if fee is None:
                warnings.append("IB_USD_COMMISSION_UNAVAILABLE")
            if state["warningText"]:
                warnings.append(state["warningText"])
            return OrderPreview("BROKER_WHAT_IF", margin, fee, "USD", tuple(warnings), observed)

    return await adapter._option_read(request, read)
