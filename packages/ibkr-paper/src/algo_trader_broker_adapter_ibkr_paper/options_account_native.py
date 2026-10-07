"""Request-scoped IB account observations, without cached account fallbacks."""

import asyncio
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from datetime import datetime, timezone
import json
from hashlib import sha256

from algo_trader_broker_sdk.options import check

from .options_reads import now_wire


def raw_value(value):
    """Keep decoded native doubles as strings; never round a financial field."""
    if is_dataclass(value):
        return raw_value(asdict(value))
    if isinstance(value, dict):
        return {str(key): raw_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [raw_value(item) for item in value]
    if type(value) in (float, Decimal):
        return str(value)
    if isinstance(value, datetime):
        check(value.tzinfo is not None, "Native IB time needs an explicit timezone")
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return value


def raw_bytes(value):
    return json.dumps(raw_value(value), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


@contextmanager
def observe_callbacks(wrapper, observers):
    """Tee native callbacks while preserving ib_async's existing consumers.

    Decoder resolves wrapper methods at delivery time in the supported client.
    Account reads are serialized by the adapter; request IDs isolate callbacks.
    """
    originals = {}
    for name, observer in observers.items():
        original = getattr(wrapper, name)
        def callback(*args, original=original, observer=observer):
            observer(*args)
            return original(*args)
        originals[name] = (original, name in wrapper.__dict__)
        setattr(wrapper, name, callback)
    try:
        yield
    finally:
        for name, (original, had_override) in originals.items():
            if had_override:
                setattr(wrapper, name, original)
            else:
                delattr(wrapper, name)


async def account_values(ib, account, *, timeout):
    req_id = ib.client.getReqId()
    rows, errors = [], []
    started = now_wire()
    future = ib.wrapper.startReq(req_id)

    def value(received_id, received_account, model, tag, amount, currency):
        if received_id != req_id:
            return
        if received_account != account or model != "":
            errors.append("IB account callback changed account/model")
            return
        if len(rows) >= 10000:
            errors.append("IB account callback exceeded bound")
            return
        rows.append(dict(tag=tag, value=amount, currency=currency, received_at=now_wire()))

    def error(received_id, code, message, *args):
        if received_id == req_id:
            errors.append(f"IB account request failed: {code}")

    ib.errorEvent += error
    try:
        with observe_callbacks(ib.wrapper, {"accountUpdateMulti": value}):
            # False requests all account values, not only ledger/NLV.
            ib.client.reqAccountUpdatesMulti(req_id, account, "", False)
            await asyncio.wait_for(future, timeout)
            check(not errors, errors[0] if errors else "Invalid account callback")
            return dict(source="IB_ACCOUNT_UPDATE_MULTI", account=account, request_id=req_id,
                        started_at=started, completed_at=now_wire(), values=rows)
    finally:
        ib.errorEvent -= error
        try:
            ib.client.cancelAccountUpdatesMulti(req_id)
        finally:
            ib.wrapper._endReq(req_id)


async def positions(ib, account, *, timeout):
    req_id = ib.client.getReqId()
    rows, errors = {}, []
    started = now_wire()
    future = ib.wrapper.startReq(req_id)

    def position(received_id, received_account, model, contract, quantity, average_cost):
        if received_id != req_id:
            return
        if received_account != account or model != "":
            errors.append("IB position callback changed account/model")
            return
        key = contract.conId if contract.conId > 0 else sha256(raw_bytes(contract)).hexdigest()
        if len(rows) >= 10000 and key not in rows:
            errors.append("IB position callback exceeded bound")
            return
        rows[key] = dict(contract=raw_value(contract), quantity=str(quantity),
                                   average_cost=str(average_cost), received_at=now_wire())

    def end(received_id):
        if received_id == req_id:
            # ib_async 2.0.1 leaves positionMulti[End] unimplemented.
            ib.wrapper._endReq(req_id)

    def error(received_id, code, message, *args):
        if received_id == req_id:
            errors.append(f"IB position request failed: {code}")

    ib.errorEvent += error
    try:
        with observe_callbacks(ib.wrapper, {"positionMulti": position, "positionMultiEnd": end}):
            ib.client.reqPositionsMulti(req_id, account, "")
            await asyncio.wait_for(future, timeout)
            check(not errors, errors[0] if errors else "Invalid position callback")
            return dict(source="IB_POSITION_MULTI", account=account, request_id=req_id,
                        started_at=started, completed_at=now_wire(), positions=list(rows.values()))
    finally:
        ib.errorEvent -= error
        try:
            ib.client.cancelPositionsMulti(req_id)
        finally:
            ib.wrapper._endReq(req_id)


async def open_orders(ib, account, *, timeout):
    # IB's open-order download has no request ID. Never replace an in-flight
    # legacy reader's shared future or treat its response as our own snapshot.
    check("openOrders" not in ib.wrapper._futures, "IB open-order download already in progress")
    rows, errors = {}, []
    started = now_wire()

    def order(order_id, contract, native_order, order_state):
        if native_order.whatIf:
            return
        if not native_order.account:
            errors.append("IB open order has no account")
            return
        if native_order.account != account:
            return
        key = (native_order.clientId, order_id, native_order.permId)
        if len(rows) >= 10000 and key not in rows:
            errors.append("IB open-order download exceeded bound")
            return
        rows[key] = dict(order_id=order_id, contract=raw_value(contract), order=raw_value(native_order),
                         state=raw_value(order_state), received_at=now_wire())

    with observe_callbacks(ib.wrapper, {"openOrder": order}):
        future = ib.reqAllOpenOrdersAsync()
        try:
            await asyncio.wait_for(future, timeout)
            check(not errors, errors[0] if errors else "Invalid order callback")
            return dict(source="IB_ALL_OPEN_ORDERS", account=account, started_at=started,
                        completed_at=now_wire(), orders=list(rows.values()))
        finally:
            # Do not cancel orders or another consumer's replaced future.
            if ib.wrapper._futures.get("openOrders") is future:
                ib.wrapper._endReq("openOrders")
