"""Original REST FILL + original native order + independent qualification."""

from dataclasses import replace
import json

import pytest

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options_events import OptionNonOptionActivity, OptionRawEvent
from algo_trader_broker_adapter_alpaca_paper.options_codec import decode_fill_activity
from algo_trader_broker_adapter_alpaca_paper.options_codec import decode_trade_update
from .test_options_codec import fixture, IDS, TIME
from .test_option_raw_events import SCOPE

EXEC = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def inputs():
    data, _, qualify = fixture()
    order = data["order"]
    activity = dict(activity_type="FILL", type="partial_fill", id="native-date-prefix::" + EXEC,
        order_id=order["id"], symbol=order["legs"][0]["symbol"], side="buy", qty="2", price="2.123456789012", transaction_time=TIME,
        cum_qty="999", leaves_qty="777")
    return activity, order, qualify


async def decode(activity, order, qualify):
    event = OptionRawEvent(SCOPE, "ALPACA_ACTIVITY_FILL", json.dumps(activity).encode().replace(b'"2.123456789012"', b'2.123456789012'))
    async def resolve(order_id):
        assert order_id == activity["order_id"]
        return OptionRawEvent(SCOPE, "ALPACA_ORDER_DETAIL", json.dumps(order).encode(), order_id)
    return await decode_fill_activity(event, qualify, resolve)


@pytest.mark.asyncio
async def test_parent_activity_uses_proven_child_premium_and_keeps_full_activity_id_without_invented_status():
    activity, order, qualify = inputs()
    value = await decode(activity, order, qualify)
    assert value.status is None and value.order_id == IDS[0] and value.client_order_ref == order["client_order_id"]
    assert len(value.executions) == 1 and value.executions[0].order_id == IDS[1]
    assert value.executions[0].execution_id == activity["id"] and value.executions[0].price == "2.123456789012"
    assert value.executions[0].contracts == 2 and value.executions[0].origin.native_contract_id == IDS[3]


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["single", "child", "equity"])
async def test_single_child_and_explicit_equity_orders_require_exact_native_evidence(shape):
    activity, parent, qualify = inputs()
    order = parent["legs"][0]
    activity["order_id"] = order["id"]
    if shape == "single": order["order_class"] = "simple"
    if shape == "equity": order["asset_class"] = "us_equity"
    value = await decode(activity, order, qualify)
    if shape == "equity": assert type(value) is OptionNonOptionActivity
    else:
        assert value.client_order_ref == (order["client_order_id"] if shape == "single" else None)
        assert value.executions[0].order_id == order["id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["order", "side", "symbol", "asset", "qty", "price", "id", "type", "parent-summary", "duplicate-child"])
async def test_uncertain_activity_evidence_cannot_be_promoted(change):
    activity, order, qualify = inputs()
    if change == "order": order["id"] = IDS[7]
    if change == "side": activity["side"] = "sell"
    if change == "symbol": activity["symbol"] = "UNKNOWN"
    if change == "asset": order["legs"][0]["asset_id"] = IDS[7]
    if change == "qty": activity["qty"] = "1.1"
    if change == "price": activity["price"] = "1.0000000000001"
    if change == "id": activity["id"] = EXEC
    if change == "type": activity["activity_type"] = "OPASN"
    if change == "parent-summary": order.pop("legs")
    if change == "duplicate-child": order["legs"].append(dict(order["legs"][0]))
    with pytest.raises(BrokerContractError): await decode(activity, order, qualify)


@pytest.mark.asyncio
async def test_order_resolver_failure_and_wrong_scope_are_not_silently_converted_to_another_account():
    activity, order, qualify = inputs()
    event = OptionRawEvent(SCOPE, "ALPACA_ACTIVITY_FILL", json.dumps(activity).encode())
    async def unavailable(_): raise RuntimeError("fixture DB unavailable")
    with pytest.raises(RuntimeError, match="DB unavailable"): await decode_fill_activity(event, qualify, unavailable)
    async def another(order_id): return OptionRawEvent(replace(SCOPE, account="other"), "ALPACA_ORDER_DETAIL", json.dumps(order).encode(), order_id)
    with pytest.raises(BrokerContractError): await decode_fill_activity(event, qualify, another)


@pytest.mark.asyncio
async def test_ws_uuid_carries_exact_native_leg_evidence_for_orders_cross_source_matching():
    data, _, qualify = fixture()
    data["legs"][0]["execution_id"] = EXEC
    value = await decode_trade_update(OptionRawEvent(SCOPE, "ALPACA_TRADE_UPDATES", json.dumps(dict(stream="trade_updates", data=data)).encode()), qualify)
    origin = value.executions[0].origin
    assert origin.source == "ALPACA_TRADE_UPDATES" and origin.order_id == IDS[1] and origin.native_contract_id == IDS[3]
    assert value.executions[0].execution_id == EXEC
