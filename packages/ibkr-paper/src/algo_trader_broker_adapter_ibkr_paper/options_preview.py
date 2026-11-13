"""Native what-if observations; never an admission or submission authority."""

import asyncio
import logging
from dataclasses import replace
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation, ROUND_CEILING, localcontext

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import (
    NativeOptionCostObservation, OptionExecutionRequest, OrderPreview, check,
    decimal_wire, option_request_hash, timestamp,
)

from .options_account_native import account_values, base_currency_summary, observe_callbacks, raw_bytes, raw_value
from .options_orders import build_order, matches_open_close, wait_transport
from .options_reads import now_wire


def conservative_margin_cash(value):
    """Project a finite native margin change upward onto the cash wire.

    IB sends binary64 calculation tails beyond twelve decimal places. Margin
    is a funding bound, not a contract price or tick: rounding upward cannot
    understate a positive native change. Keep the original callback in the raw
    evidence, reject unset/invalid/out-of-range values before clamping, and use
    zero only for a valid nonpositive incremental change.
    """
    check(type(value) in (float, int, str, Decimal), "Invalid native IB margin amount")
    try:
        amount = Decimal(str(value))
        check(amount.is_finite() and amount.copy_abs() < Decimal('1e53'),
              "Native IB margin amount is unset or outside cash wire range")
        with localcontext() as context:
            context.prec = 80
            bound = max(Decimal(0), amount).quantize(Decimal('0.000000000001'), rounding=ROUND_CEILING)
        wire = format(bound, 'f').rstrip('0').rstrip('.') if bound else '0'
        decimal_wire(wire, nonnegative=True)
        return wire
    except (InvalidOperation, ValueError) as exc:
        raise BrokerContractError("Invalid native IB margin amount") from exc


async def preview_order(adapter, request):
    check(type(request) is OptionExecutionRequest, "IB preview needs an exact option order")

    async def read(ib, bound):
        async with adapter._option_account_read_lock:
            values = await account_values(ib, bound.native_account_ref, timeout=adapter._qualification_timeout)
            usd = any(row["tag"] == "Currency" and row["currency"] == "BASE" and row["value"] == "USD"
                      for row in values["values"])
            base_summary = None
            if not usd:
                base_summary = await base_currency_summary(ib, bound.native_account_ref,
                    timeout=adapter._qualification_timeout)
                usd = base_summary['base_currency'] == 'USD'
            contracts = []
            for leg in request.legs:
                cached = adapter._option_catalog.get(leg.binding.broker_contract_id)
                check(cached is not None and replace(cached[0], qualified_at=leg.binding.qualified_at) == leg.binding
                      and timedelta(0) <= datetime.now(timezone.utc) - timestamp(cached[0].qualified_at) < timedelta(days=1),
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
                    detail = str(message).replace(bound.native_account_ref, '[account]')[:1000]
                    errors.append(f"IB preview rejected: {code}: {detail}")
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
                account=values, base_currency_summary=base_summary,
                contract=contract, order=order, observations=result, errors=errors)))
            phase = ','.join(sorted({leg.position_effect for leg in request.legs}))
            check(not errors and len(result) == 1,
                  "IB preview did not return one unambiguous what-if response"
                  + (f" ({phase}; {'; '.join(errors)})" if errors else ''))
            native_contract, native_order, state = result[0]
            check(state.get("status") not in {"Inactive", "Cancelled", "ApiCancelled", "Rejected"}
                  and not state.get("rejectReason"), "IB what-if response rejected the exact order")
            check(native_order["whatIf"] is True and native_order["account"] == bound.native_account_ref
                  and native_order["orderId"] == order.orderId and native_order["clientId"] == order.clientId
                  and native_order["orderRef"] == request.client_order_id
                  and native_order["action"] == order.action
                  and Decimal(str(native_order["totalQuantity"])) == request.groups
                  and Decimal(native_order["lmtPrice"]) == Decimal(str(order.lmtPrice))
                  and (native_order["orderType"], native_order["tif"], native_order["outsideRth"]) ==
                      (order.orderType, order.tif, False)
                  and matches_open_close(native_order["openClose"], order.openClose)
                  and native_contract["secType"] == contract.secType,
                  "IB what-if response changed its account or order")
            if len(contracts) == 1:
                check(native_contract["conId"] == contract.conId, "IB preview changed the qualified option")
            else:
                actual = [(leg["conId"], leg["ratio"], leg["action"], leg["exchange"], leg["openClose"])
                          for leg in native_contract["comboLegs"]]
                expected = [(leg.conId, leg.ratio, leg.action, leg.exchange, leg.openClose) for leg in contract.comboLegs]
                # With no per-leg order prices, a BAG is an economic vector,
                # independent of callback row order. Keep duplicates and every
                # signed contract/route/position-effect component in the match.
                if sorted(actual) != sorted(expected):
                    logging.getLogger(__name__).warning('Native what-if combo mismatch actual=%s expected=%s description=%s',
                        actual, expected, native_contract.get('comboLegsDescrip'))
                check(sorted(actual) == sorted(expected),
                      "IB preview changed the exact combo vector")
            warnings = ["IB_INITIAL_MARGIN_CHANGE_ONLY", "INTERIM_ASSIGNMENT_FUNDING_NOT_ESTIMATED",
                        "EMERGENCY_CLOSE_FEES_NOT_INCLUDED"]
            if not native_order["openClose"]:
                warnings.append("IB_OPEN_CLOSE_NOT_REPORTED")
            try:
                margin = conservative_margin_cash(state["initMarginChange"]) if usd else None
            except BrokerContractError:
                margin = None
            if margin is None:
                warnings.append("IB_USD_MARGIN_UNAVAILABLE")
                logging.getLogger(__name__).warning(
                    'Native option margin unavailable phase=%s initMarginChange=%s base_currency_rows=%s summary_currencies=%s',
                    phase, state.get('initMarginChange'),
                    [(row['currency'], row['value']) for row in values['values'] if row['tag'] == 'Currency'],
                    None if base_summary is None else [row['currency'] for row in base_summary['values']])
            # Preserve actual API property names. TWS API 10.33 renamed commission
            # fields to commissionAndFees; this does not prove a future all-in cap.
            from ib_async import __version__
            server_version = ib.client.serverVersion()
            api_version = f"ib_async/{__version__};tws_server/{server_version}"
            native_fields = {name: state[name] for name in (
                "commission", "minCommission", "maxCommission", "commissionCurrency",
                "commissionAndFees", "minCommissionAndFees", "maxCommissionAndFees",
                "commissionAndFeesCurrency",
            ) if name in state}
            native_costs = NativeOptionCostObservation.from_native(
                scope=bound.scope, request_hash=option_request_hash(request),
                observed_at=observed, api_version=api_version, native_fields=native_fields)
            native_costs.require_request(request)
            fee = native_costs.estimate_cash
            if fee is None:
                warnings.append("IB_USD_COMMISSION_UNAVAILABLE")
            if native_costs.max_cash is None:
                warnings.append("IB_USD_MAX_COMMISSION_UNAVAILABLE")
            if state["warningText"]:
                warnings.append(state["warningText"])
            return OrderPreview("BROKER_WHAT_IF", margin, fee, "USD", tuple(warnings), observed, native_costs)

    return await adapter._option_read(request, read)

