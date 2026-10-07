"""Exact Alpaca option order mapping; parent summaries never create fills."""

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import OptionExecutionRequest, OptionLegOrderState, OptionOrderState, check, decimal_wire
from algo_trader_broker_sdk.options_events import OptionNativeInterpretation, OptionNativeLeg

from .options_codec import _decimal, _time, _uuid, _ResolutionFailure
from .raw_stream import decode_native

STATUSES = {"accepted": "ACKNOWLEDGED", "new": "ACKNOWLEDGED", "pending_new": "ACKNOWLEDGED",
    "accepted_for_bidding": "ACKNOWLEDGED", "partially_filled": "PARTIALLY_FILLED", "filled": "FILLED",
    "pending_cancel": "CANCEL_PENDING", "canceled": "CANCELED", "expired": "EXPIRED", "rejected": "REJECTED"}


def order_payload(request):
    check(type(request) is OptionExecutionRequest and request.environment == "paper", "A scoped Paper option request is required")
    check(all(leg.binding.adapter_id == "alpaca_paper" for leg in request.legs), "Order belongs to another adapter")
    check(request.max_slippage == "0", "The authorized limit already incorporates slippage")
    common = dict(qty=str(request.groups), type="limit", time_in_force="day", client_order_id=request.client_order_id)
    if len(request.legs) == 1:
        leg = request.legs[0]
        common.update(order_class="simple", symbol=leg.binding.local_symbol, side=leg.side.lower(),
            position_intent=leg.side.lower() + "_to_" + leg.position_effect.lower(),
            limit_price=_decimal(decimal_wire(request.signed_limit).copy_abs()))
    else:
        common.update(order_class="mleg", limit_price=request.signed_limit,
            legs=[dict(symbol=leg.binding.local_symbol, ratio_qty=str(leg.ratio), side=leg.side.lower(),
                position_intent=leg.side.lower() + "_to_" + leg.position_effect.lower()) for leg in request.legs])
    return common


def _contracts(value):
    number = Decimal(_decimal(value))
    check(number == number.to_integral_value() and number <= 2**53 - 1, "Native option quantity must be whole contracts")
    return int(number)


def order_state(request, raw):
    order = decode_native(raw)
    check(type(order) is dict and order.get("client_order_id") == request.client_order_id,
          "Native order response changed the stable client reference")
    parent = _uuid(order["id"])
    check(order.get("status") in STATUSES and _contracts(order["qty"]) == request.groups,
          "Native order response has another quantity or unsupported status")
    combo = len(request.legs) > 1
    check(order.get("order_class") == ("mleg" if combo else "simple"), "Native order class differs from the request")
    native_legs = order.get("legs") if combo else [order]
    check(type(native_legs) is list and len(native_legs) == len(request.legs), "Native response does not contain every exact option leg")
    by_asset = {_uuid(item["asset_id"]): item for item in native_legs}
    check(len(by_asset) == len(native_legs), "Duplicate native option contract")
    legs, counts = [], []
    for leg in request.legs:
        native = by_asset.get(_uuid(leg.binding.broker_contract_id))
        target = request.groups * leg.ratio
        check(native is not None and native.get("asset_class") == "us_option" and native.get("symbol") == leg.binding.local_symbol
              and native.get("side") == leg.side.lower() and _contracts(native["qty"]) == target,
              "Native leg differs from its exact qualified request")
        check(native.get("position_intent") == leg.side.lower() + "_to_" + leg.position_effect.lower(), "Native leg changed its position intent")
        filled = _contracts(native["filled_qty"])
        check(filled <= target, "Native quantity requires overfill reconciliation")
        legs.append(OptionLegOrderState(leg.leg_id, leg.binding.canonical_id, _uuid(native["id"]), filled, target - filled))
        counts.append(filled // leg.ratio)
    matched = min(counts)
    residual = any(leg.filled_contracts != matched * intent.ratio for leg, intent in zip(legs, request.legs))
    status = STATUSES[order["status"]]
    check(status != "FILLED" or (matched == request.groups and not residual), "Parent fill summary lacks matching actual leg quantities")
    return OptionOrderState(request.command_id, request.client_order_id, parent, status, request.groups, matched,
        request.groups - matched, tuple(legs), residual, _time(order["updated_at"] or order["created_at"]))


def rejected_state(request):
    return OptionOrderState(request.command_id, request.client_order_id, None, "REJECTED", request.groups, 0, request.groups,
        tuple(OptionLegOrderState(leg.leg_id, leg.binding.canonical_id, None, 0, request.groups * leg.ratio) for leg in request.legs),
        False, datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))


async def decode_order_evidence(event, resolve_contract):
    """Original REST acknowledgement supplies links/status, never executions."""
    try:
        return await _decode_order_evidence(event, resolve_contract)
    except _ResolutionFailure as exc:
        raise exc.error
    except (ValueError, TypeError, AttributeError, KeyError, InvalidOperation, RecursionError) as exc:
        raise BrokerContractError("Native option order evidence remains unresolved") from exc


async def _decode_order_evidence(event, resolve_contract):
    check(event.source == "ALPACA_ORDER_DETAIL" and event.scope.environment == "paper", "Unsupported native option order source")
    order = decode_native(event.raw_payload)
    check(type(order) is dict and order.get("status") in STATUSES, "Unsupported native option order evidence")
    root = _uuid(order["id"])
    combo = order.get("order_class") == "mleg" and order.get("asset_class") in {None, ""}
    native_legs = order.get("legs") if combo else [order]
    check(type(native_legs) is list and 1 <= len(native_legs) <= 4, "Missing native order legs")
    legs = []
    for native in native_legs:
        check(native.get("asset_class") == "us_option", "Native record is not an option")
        try:
            resolved = await resolve_contract(_uuid(native["asset_id"]), native["symbol"])
        except Exception as exc:
            raise _ResolutionFailure(exc) from exc
        binding, contract = resolved.binding, resolved.contract
        check(resolved.status == "EXACT" and binding is not None and contract is not None and
            (binding.adapter_id, binding.account_scope, binding.environment, binding.broker_contract_id, binding.local_symbol) ==
            ("alpaca_paper", event.scope.account, event.scope.environment, native["asset_id"], native["symbol"]),
            "Native order lacks exact qualification")
        check(binding.canonical_id == contract.canonical_id == resolved.canonical_id, "Native order economic identity differs")
        check(native.get("side") in {"buy", "sell"}, "Native order lacks a side")
        legs.append(OptionNativeLeg(_uuid(native["id"]), contract.canonical_id, native["side"].upper(), contract.key.multiplier))
    return OptionNativeInterpretation(event.scope, root, order["client_order_id"], "ALPACA_ORDER_UUID", STATUSES[order["status"]],
        _time(order["updated_at"] or order["created_at"]), tuple(legs), ())
