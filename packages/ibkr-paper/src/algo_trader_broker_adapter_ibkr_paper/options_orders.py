"""Exact OPT/BAG preparation and one guarded IB submission, never replayed."""

import asyncio
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import json
from types import SimpleNamespace

from algo_trader_broker_sdk import BrokerCapabilityError, BrokerContractError, BrokerOrderError, SubmissionGate, dataclass_to_payload
from algo_trader_broker_sdk.options import (
    OptionExecutionRequest, OptionLegOrderState, OptionOrderState, OptionScope, OptionVerifiedAccount,
    check, option_from_payload, timestamp,
)
from algo_trader_broker_sdk.options_capabilities import OptionShapeCapability
from algo_trader_broker_sdk.options_events import OptionNativeInterpretation, OptionNativeLeg, OptionRawEvent
from algo_trader_broker_sdk.submission_gate import order_fingerprint

from .options_account_native import observe_callbacks, raw_bytes, raw_value
from .options_reads import now_wire


def build_order(request, contracts, account, client_id, order_id, shape):
    from ib_async import ComboLeg, Contract, Order
    check(type(request) is OptionExecutionRequest and request.environment == "paper"
          and request.max_slippage == "0", "IB needs an exact authorized Paper limit request")
    check(type(shape) is OptionShapeCapability and shape.route == "NATIVE"
          and shape.capability.status == "PAPER_CERTIFIED" and shape.net_tick is not None,
          "IB requires the Runner-selected certified execution shape")
    check(len(contracts) == len(request.legs) <= shape.max_legs
          and all(leg.ratio <= shape.max_ratio for leg in request.legs), "IB shape quantity limits exceeded")
    check(request.groups <= 2**53 - 1 and all(request.groups * leg.ratio <= 2**53 - 1 for leg in request.legs),
          "IB order quantity loses integer precision")
    price = Decimal(request.signed_limit)
    with localcontext() as context:
        context.prec = 80
        check(price % Decimal(shape.net_tick) == 0, "IB net price is off its certified route tick")
    check(price != 0 or shape.zero_net_price, "IB route has no zero-price certification")
    check(type(order_id) is int and order_id > 0 and type(client_id) is int and client_id >= 0,
          "IB native order/client ID must come from the current client")
    check(len({c.symbol for c in contracts}) == 1 and all(c.secType == "OPT" and c.currency == "USD" for c in contracts),
          "IB option legs must belong to one standard USD underlying")
    for leg, contract in zip(request.legs, contracts):
        check(leg.binding.adapter_id == "ibkr_paper" and leg.binding.broker_contract_id == str(contract.conId)
              and leg.binding.local_symbol == contract.localSymbol and leg.binding.exchange == "SMART",
              "IB native contract differs from exact qualification")
    combo = len(contracts) > 1
    if combo:
        check(shape.native_combo, "IB route has no native combo certification")
        contract = Contract(secType="BAG", symbol=contracts[0].symbol, currency="USD", exchange="SMART",
            comboLegs=[ComboLeg(conId=c.conId, ratio=leg.ratio, action=leg.side, exchange="SMART", openClose=0)
                       for c, leg in zip(contracts, request.legs)])
        action = "BUY"
    else:
        contract, action = deepcopy(contracts[0]), request.legs[0].side
        price = price.copy_abs()
    native_price = float(price)
    check(Decimal(str(native_price)) == price, "IB native double loses limit-price precision")
    order = Order(orderId=order_id, clientId=client_id, account=account, orderRef=request.client_order_id,
        action=action, totalQuantity=request.groups, orderType="LMT", lmtPrice=native_price, tif="DAY",
        openClose="O" if request.legs[0].position_effect == "OPEN" else "C", outsideRth=False, transmit=True,
        smartComboRoutingParams=[], orderComboLegs=[])
    return contract, order


def preparation(request, contract, order):
    return dict(source="IB_OPTION_PREPARATION_V1", command_id=request.command_id,
        request_fingerprint=order_fingerprint(request), native_account_ref=order.account,
        client_id=order.clientId, order_id=order.orderId, order_ref=order.orderRef,
        contract=raw_value(contract), order=raw_value(order), request=dataclass_to_payload(request))


def transport_ready(client):
    # ib_async's sendMsg otherwise queues after the guard. Require a free slot
    # immediately before the synchronous placeOrder -> socket call instead.
    now = asyncio.get_running_loop().time()
    used = sum(now - at <= client.RequestsInterval for at in client._timeQ)
    return not client._msgQ and (not client.MaxRequests or used < client.MaxRequests)


async def wait_transport(client, request):
    while not transport_ready(client):
        check(datetime.now(timezone.utc) < timestamp(request.valid_until), "IB order expired waiting for transport")
        await asyncio.sleep(0.01)


class IBOptionOrders:
    option_native_preparation_version = 1

    def set_option_event_handler(self, context, handler):
        check(type(context) is OptionVerifiedAccount, "IB option events need a verified scope")
        if handler is None:
            if getattr(self, "_option_event_binding", None) == context:
                self._option_event_binding = None
                self._option_evidence_handler = None
            return
        self.bind_option_account(context)
        self._option_event_binding, self._option_evidence_handler = context, handler

    async def submit_option_order(self, request):
        raise BrokerCapabilityError("IB option submissions require the Runner gate", code="BROKER_SUBMISSION_GUARD_REQUIRED")

    async def decode_option_event(self, event, resolve_contract):
        check(event.source == "IB_OPTION_SUBMISSION", "IB option source is not implemented")
        try:
            raw = json.loads(event.raw_payload)
            prepared = json.loads(raw["preparation"])
            request = option_from_payload(OptionExecutionRequest, prepared["request"])
            scope = OptionScope(**{name: getattr(request, name) for name in OptionScope.__dataclass_fields__})
            check(scope == event.scope and prepared["request_fingerprint"] == order_fingerprint(request),
                  "IB retained submission changed its observed scope or command")
            state = acknowledgement(request, SimpleNamespace(**prepared["order"]), raw["observations"])
            check(state.parent_order_ref is not None and state.status != "SUBMISSION_UNKNOWN", "IB native acknowledgement is unresolved")
            legs = []
            for intent in request.legs:
                binding = intent.binding
                resolved = await resolve_contract(binding.broker_contract_id, binding.local_symbol)
                check(resolved.status == "EXACT" and resolved.binding is not None and resolved.contract is not None
                    and (resolved.binding.adapter_id, resolved.binding.account_scope, resolved.binding.environment,
                         resolved.binding.broker_contract_id, resolved.binding.local_symbol, resolved.canonical_id) ==
                    ("ibkr_paper", scope.account, scope.environment, binding.broker_contract_id, binding.local_symbol, binding.canonical_id),
                    "IB acknowledgement lacks retained exact qualification")
                # IB has no separate BAG child-order ID. This native composite
                # identifies a leg within its permanent parent; it is not a fill.
                leg_ref = state.parent_order_ref if len(request.legs) == 1 else state.parent_order_ref + ":" + binding.broker_contract_id
                legs.append(OptionNativeLeg(leg_ref, binding.canonical_id, intent.side, resolved.contract.key.multiplier))
            return OptionNativeInterpretation(scope, state.parent_order_ref, request.client_order_id, "IBKR_PERM_ID",
                state.status, raw["observations"][-1]["observed_at"], tuple(legs), ())
        except (ValueError, TypeError, AttributeError, KeyError) as exc:
            raise BrokerContractError("IB submission evidence remains unresolved") from exc

    async def submit_option_order_guarded(self, request, gate):
        check(type(request) is OptionExecutionRequest and type(gate) is SubmissionGate
              and gate.retain_native is not None, "IB option submission needs a durable native preparation gate")

        async def send(ib, bound):
            check(getattr(self, "_option_event_binding", None) == bound and callable(self._option_evidence_handler),
                  "IB option submission requires a durable evidence sink")
            settings = self._settings
            from collections.abc import Mapping
            read_only = settings.get("ib_read_only") if isinstance(settings, Mapping) else settings.read_only
            check(str(read_only).lower() not in {"true", "1", "yes", "on"}, "IB connection is read-only")
            sink = self._option_evidence_handler
            contracts = []
            for leg in request.legs:
                cached = self._option_catalog.get(leg.binding.broker_contract_id)
                check(cached is not None and cached[0] == leg.binding
                      and 0 <= (datetime.now(timezone.utc) - timestamp(leg.binding.qualified_at)).total_seconds() < 30,
                      "IB order requires current exact contract bindings")
                contracts.append(cached[2].contract)
            contract, order = build_order(request, contracts, bound.native_account_ref,
                ib.client.clientId, ib.client.getReqId(), gate.option_shape)
            prepared = raw_bytes(preparation(request, contract, order))
            records, ready = [], asyncio.Event()

            def current():
                check(self._option_account_binding == bound and self._option_event_binding == bound
                    and self._option_evidence_handler is sink and ib.isConnected()
                    and self._client.connection_state_snapshot().get("connected_since") == self._option_connection
                    and ib.client.clientId == ib.wrapper.clientId == order.clientId
                    and bound.native_account_ref in ib.managedAccounts()
                    and datetime.now(timezone.utc) < timestamp(request.valid_until)
                    and raw_bytes(preparation(request, contract, order)) == prepared,
                    "IB connection or prepared payload changed before native send")

            def opened(order_id, native_contract, native_order, state):
                if order_id == order.orderId and native_order.clientId == order.clientId:
                    records.append(dict(kind="openOrder", order_id=order_id, contract=raw_value(native_contract),
                        order=raw_value(native_order), state=raw_value(state), observed_at=now_wire()))
                    ready.set()

            def error(req_id, code, message, *args):
                if req_id == order.orderId:
                    records.append(dict(kind="error", order_id=req_id, code=code, message=message, observed_at=now_wire()))
                    ready.set()

            await wait_transport(ib.client, request)
            await gate.prepare()
            current()
            await gate.record_native(prepared)
            await wait_transport(ib.client, request)
            current()
            ib.errorEvent += error
            try:
                with observe_callbacks(ib.wrapper, {"openOrder": opened}):
                    gate.consume()
                    # No await/reconnect helper is permitted between the final
                    # gate and this synchronous native send.
                    ib.placeOrder(contract, order)
                    try:
                        await asyncio.wait_for(ready.wait(), 2.0)
                    except asyncio.TimeoutError:
                        pass
            except Exception as exc:
                raise BrokerOrderError("IB option outcome is unknown; reconcile without resending",
                                       code="broker_order_outcome_unknown") from exc
            finally:
                ib.errorEvent -= error
                if gate.consumed:
                    await sink(OptionRawEvent(bound.scope, "IB_OPTION_SUBMISSION", raw_bytes(dict(
                        preparation=prepared.decode(), observations=records, observed_at=now_wire())), request.client_order_id))
            return acknowledgement(request, order, records)

        return await self._option_read(request, send)


def acknowledgement(request, prepared_order, records):
    parent, status = None, "SUBMISSION_UNKNOWN"
    for record in records:
        if record["kind"] == "error":
            if record["code"] == 201:
                status = "REJECTED"
            continue
        order, contract = record["order"], record["contract"]
        check(order["account"] == prepared_order.account and order["orderRef"] == request.client_order_id
              and order["orderId"] == prepared_order.orderId and order["clientId"] == prepared_order.clientId
              and Decimal(str(order["totalQuantity"])) == request.groups
              and Decimal(order["lmtPrice"]) == Decimal(str(prepared_order.lmtPrice))
              and order["action"] == prepared_order.action and order["openClose"] == prepared_order.openClose,
              "IB acknowledgement changed the prepared command")
        check(order["orderType"] == "LMT" and order["tif"] == "DAY" and not order["outsideRth"],
              "IB acknowledgement changed its order policy")
        if len(request.legs) == 1:
            check(contract["secType"] == "OPT" and str(contract["conId"]) == request.legs[0].binding.broker_contract_id,
                  "IB acknowledgement changed its option contract")
        else:
            check(contract["secType"] == "BAG" and [(str(leg["conId"]), leg["ratio"], leg["action"], leg["openClose"])
                for leg in contract["comboLegs"]] == [(leg.binding.broker_contract_id, leg.ratio, leg.side, 0) for leg in request.legs],
                "IB acknowledgement changed its combo vector")
        if type(order["permId"]) is int and order["permId"] > 0:
            parent = str(order["permId"])
            status = {"Submitted": "ACKNOWLEDGED", "PreSubmitted": "ACKNOWLEDGED", "PendingCancel": "CANCEL_PENDING",
                      "Cancelled": "CANCELED", "ApiCancelled": "CANCELED"}.get(record["state"]["status"], "SUBMISSION_UNKNOWN")
    # Parent/BAG summaries never fabricate per-leg fills. Actual OPT execution
    # callbacks are reconciled by the separate event source.
    return OptionOrderState(request.command_id, request.client_order_id, parent, status, request.groups, 0, request.groups,
        tuple(OptionLegOrderState(leg.leg_id, leg.binding.canonical_id, None, 0, request.groups * leg.ratio) for leg in request.legs),
        False, now_wire())
