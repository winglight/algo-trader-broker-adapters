"""Native trade_updates interpretation from retained bytes and qualifications.

No broker calls, inferred OCC contracts, cumulative-quantity deltas, synthetic
fees, parent-net fills, or REST activity ID truncation belong in this codec.
"""

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import re
from uuid import UUID

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import check, decimal_wire
from algo_trader_broker_sdk.options_events import OptionNativeExecution, OptionNativeInterpretation, OptionNativeLeg

from .raw_stream import decode_native

_STATUS = {"accepted": "ACKNOWLEDGED", "new": "ACKNOWLEDGED", "pending_new": "ACKNOWLEDGED", "partial_fill": "PARTIALLY_FILLED",
           "fill": "FILLED", "pending_cancel": "CANCEL_PENDING", "canceled": "CANCELED",
           "expired": "EXPIRED", "rejected": "REJECTED"}


class _ResolutionFailure(Exception):
    def __init__(self, error):
        self.error = error


def _uuid(value):
    check(type(value) is str and str(UUID(value)) == value, "Native order/contract UUID is invalid")
    return value


def _time(value):
    check(type(value) is str and re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})", value) is not None,
          "Native event timestamp is invalid")
    # Domain storage uses microseconds. Floor native nanoseconds (never round
    # forward); the complete source timestamp remains in the immutable bytes.
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _decimal(value):
    check(type(value) in (str, int, Decimal), "Native amount must not come from float coercion")
    number = Decimal(value)
    check(number.is_finite() and number >= 0 and number < Decimal("1e53"), "Native amount is invalid")
    if number == 0:
        return "0"
    digits, exponent = number.as_tuple().digits, number.as_tuple().exponent
    trailing = 0
    for digit in reversed(digits):
        if digit != 0: break
        trailing += 1
    check(exponent + trailing >= -12 and exponent >= -256, "Native amount exceeds exact decimal precision")
    wire = format(number, "f")
    wire = wire.rstrip("0").rstrip(".") if "." in wire else wire
    return "0" if number == 0 else wire


async def decode_trade_update(event, resolve_contract):
    try:
        return await _decode(event, resolve_contract)
    except _ResolutionFailure as exc:
        # Preserve the Runner resolver's missing/ambiguous/corrupt evidence
        # reason and storage failures without importing its implementation.
        raise exc.error
    except (ValueError, TypeError, AttributeError, KeyError, InvalidOperation, RecursionError) as exc:
        raise BrokerContractError("Native option event remains unresolved") from exc


async def _decode(event, resolve_contract):
    check(event.source == "ALPACA_TRADE_UPDATES" and event.scope.environment == "paper", "Unsupported native option event source")
    message = decode_native(event.raw_payload)
    check(type(message) is dict and message.get("stream") == "trade_updates", "Expected a native trade_updates frame")
    data = message["data"]
    check(type(data) is dict and data.get("event") in _STATUS, "Unsupported native event kind requires review")
    order = data["order"]
    check(type(order) is dict, "Missing native order")
    root = _uuid(order["id"])
    parent = order.get("order_class") == "mleg" and order.get("asset_class") in ("", None)
    child = order.get("order_class") == "mleg" and order.get("asset_class") == "us_option"
    check(parent or order.get("asset_class") == "us_option", "Native record is not an option order")
    native_client = order.get("client_order_id")
    # Generated child client references are not the durable ATI parent label.
    # Parent linkage, once observed, resolves its native UUID independently.
    client = None if child else native_client
    check(client is None or (type(client) is str and 0 < len(client) <= 48 and client.strip() == client), "Invalid native client reference")
    effective = _time(data.get("timestamp") or data.get("at"))
    native_legs = ([] if order.get("legs") is None else order["legs"]) if parent else [order]
    check(type(native_legs) is list and len(native_legs) <= 4, "Invalid native option leg collection")
    legs, by_order = [], {}
    for native in native_legs:
        check(type(native) is dict and native.get("asset_class") == "us_option", "Unproven native option leg")
        native_id, asset_id, symbol = _uuid(native["id"]), _uuid(native["asset_id"]), native["symbol"]
        check(type(symbol) is str and bool(symbol), "Missing native symbol cross-check")
        try:
            qualified = await resolve_contract(asset_id, symbol)
        except Exception as exc:
            raise _ResolutionFailure(exc) from exc
        binding, contract = qualified.binding, qualified.contract
        check(qualified.status == "EXACT" and binding is not None and contract is not None,
              "Execution contract lacks exact qualification")
        check((binding.adapter_id, binding.environment, binding.account_scope, binding.broker_contract_id, binding.local_symbol) ==
              ("alpaca_paper", event.scope.environment, event.scope.account, asset_id, symbol), "Execution differs from its retained qualification")
        check(binding.canonical_id == contract.canonical_id == qualified.canonical_id, "Execution economic identity differs")
        check(native.get("side") in ("buy", "sell"), "Missing native execution side")
        check(native_id not in by_order, "Duplicate native child order")
        by_order[native_id] = native
        legs.append(OptionNativeLeg(native_id, contract.canonical_id, native["side"].upper(), contract.key.multiplier))
    executions = []
    if data["event"] in ("fill", "partial_fill"):
        items = ([] if data.get("legs") is None else data["legs"]) if parent else [data]
        check(type(items) is list and len(items) <= 4, "Invalid native execution collection")
        for execution in items:
            check(type(execution) is dict, "Malformed native execution")
            order_id = _uuid(execution["order_id"]) if parent else root
            check(order_id in by_order, "Execution does not match a supplied native child order")
            native = by_order[order_id]
            check(not parent or execution.get("symbol") == native["symbol"], "Execution symbol and native child disagree")
            check(not parent or order_id != root, "Parent net summary cannot be a leg execution")
            quantity = Decimal(_decimal(execution["qty"]))
            check(quantity == quantity.to_integral_value() and 0 < quantity <= 2**53 - 1, "Executed option quantity must be positive whole contracts")
            price = _decimal(execution["price"])
            decimal_wire(price, nonnegative=True)
            executions.append(OptionNativeExecution(order_id, execution["execution_id"], int(quantity), price,
                _time(execution["timestamp"])))
    return OptionNativeInterpretation(event.scope, root, client, "ALPACA_ORDER_UUID",
        _STATUS[data["event"]], effective, tuple(legs), tuple(executions), legs[0].contract_id if child else None)
