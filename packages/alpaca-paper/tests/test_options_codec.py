"""Synthetic versions of documented native fields, never broker certification."""

from copy import deepcopy
from dataclasses import replace
import json

import pytest

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import BrokerOptionBinding, OptionContract, OptionContractKey, QualificationResult
from algo_trader_broker_sdk.options_events import OptionRawEvent
from algo_trader_broker_adapter_alpaca_paper.options_codec import decode_trade_update
from .test_option_raw_events import SCOPE


IDS = [f"00000000-0000-4000-8000-{number:012d}" for number in range(8)]
TIME = "2026-10-05T14:00:00.123456789Z"


def fixture():
    qualified, native, executions = {}, [], []
    for index in range(2):
        identity = "opt_" + "ab"[index] * 64
        symbol = f"SPY261009C00{600+index*5}000"
        key = OptionContractKey("SPY", "2026-10-09", "C", str(600 + index*5), "USD", 100, "AMERICAN", "PHYSICAL", "STANDARD_100_SHARES")
        contract = OptionContract(identity, key, None, None, None, "ACTIVE")
        binding = BrokerOptionBinding("alpaca_paper", "0.1.0", "paper", SCOPE.account, identity,
            IDS[index+3], symbol, None, "2026-10-05T13:00:00Z", "synthetic-v1")
        qualified[(binding.broker_contract_id, symbol)] = QualificationResult(identity, "EXACT", binding, contract, ())
        native.append(dict(id=IDS[index+1], asset_id=binding.broker_contract_id, symbol=symbol, asset_class="us_option",
            side="buy" if index == 0 else "sell", order_class="mleg", client_order_id="generated-child", filled_qty="999", filled_avg_price="888"))
        executions.append(dict(order_id=IDS[index+1], execution_id="full::exec-"+str(index), symbol=symbol,
            qty="2", price="2.123456789012" if index == 0 else "0.82", timestamp=TIME))
    data = dict(event="fill", timestamp=TIME, order=dict(id=IDS[0], client_order_id="atiopt_synthetic_parent",
        asset_class="", order_class="mleg", legs=native, filled_qty="999", filled_avg_price="777"),
        legs=executions, qty="999", price="666")
    async def resolve(asset_id, symbol):
        if (asset_id, symbol) not in qualified: raise BrokerContractError("No retained qualification")
        return qualified[(asset_id, symbol)]
    return data, qualified, resolve


def event(data):
    return OptionRawEvent(SCOPE, "ALPACA_TRADE_UPDATES", json.dumps(dict(stream="trade_updates", data=data)).encode())


@pytest.mark.asyncio
async def test_exact_multileg_quantities_ids_and_prices_ignore_parent_and_child_cumulative_summaries():
    data, _, resolve = fixture()
    result = await decode_trade_update(event(data), resolve)
    assert result.status == "FILLED" and result.order_id == IDS[0]
    assert result.session_key == "ALPACA_ORDER_UUID" and result.scope == SCOPE
    assert [leg.side for leg in result.legs] == ["BUY", "SELL"]
    assert [execution.price for execution in result.executions] == ["2.123456789012", "0.82"]
    assert [execution.contracts for execution in result.executions] == [2, 2]
    assert [execution.execution_id for execution in result.executions] == ["full::exec-0", "full::exec-1"]
    assert all(execution.effective_at == "2026-10-05T14:00:00.123456Z" for execution in result.executions)


@pytest.mark.asyncio
async def test_native_json_numeric_precision_and_nanoseconds_remain_in_raw_bytes():
    data, _, resolve = fixture()
    raw = event(data).raw_payload.replace(b'"2.123456789012"', b'2.123456789012')
    result = await decode_trade_update(OptionRawEvent(SCOPE, "ALPACA_TRADE_UPDATES", raw), resolve)
    assert result.executions[0].price == "2.123456789012"
    assert TIME.encode() in raw


@pytest.mark.asyncio
async def test_parent_status_without_leg_executions_never_fabricates_any_financial_fact():
    data, _, resolve = fixture()
    del data["legs"]
    result = await decode_trade_update(event(data), resolve)
    assert result.status == "FILLED" and result.executions == ()


@pytest.mark.asyncio
async def test_single_option_and_child_execution_have_distinct_client_correlation_rules():
    data, _, resolve = fixture()
    execution = data["legs"][0]
    data.update(order=data["order"]["legs"][0], **{key: execution[key] for key in ("execution_id", "qty", "price", "timestamp")})
    data.pop("legs")
    child = await decode_trade_update(event(data), resolve)
    assert child.client_order_ref is None and child.status == "FILLED" and len(child.executions) == 1
    assert child.status_contract_id == child.legs[0].contract_id
    data["order"].update(order_class="simple", client_order_id="atiopt_single")
    single = await decode_trade_update(event(data), resolve)
    assert single.client_order_ref == "atiopt_single" and single.status == "FILLED"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["new", "pending_new"])
async def test_nonfinancial_child_acknowledgement_is_a_leg_observation_not_an_unresolved_or_parent_fill(kind):
    data, _, resolve = fixture()
    data.update(event=kind, order=data["order"]["legs"][0])
    data.pop("legs")
    child = await decode_trade_update(event(data), resolve)
    assert child.status == "ACKNOWLEDGED" and child.status_contract_id == child.legs[0].contract_id
    assert child.executions == () and child.client_order_ref is None


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["symbol", "child_id", "asset", "side", "fraction", "precision", "negative", "huge_exponent",
    "duplicate", "missing_price", "missing_time", "missing_exec", "unknown_event", "unknown_asset", "malformed_legs"])
async def test_incomplete_or_ambiguous_native_evidence_cannot_be_normalized(change):
    data, _, resolve = fixture()
    item = data["legs"][0]
    if change == "symbol": item["symbol"] = "QQQ"
    if change == "child_id": item["order_id"] = IDS[7]
    if change == "asset": data["order"]["legs"][0]["asset_id"] = IDS[7]
    if change == "side": data["order"]["legs"][0].pop("side")
    if change == "fraction": item["qty"] = "1.2"
    if change == "precision": item["price"] = "1.0000000000001"
    if change == "negative": item["price"] = "-1"
    if change == "huge_exponent": item["price"] = "1e-999999999"
    if change == "duplicate": data["legs"].append(deepcopy(item))
    if change == "missing_price": item.pop("price")
    if change == "missing_time": item.pop("timestamp")
    if change == "missing_exec": item.pop("execution_id")
    if change == "unknown_event": data["event"] = "trade_bust"
    if change == "unknown_asset": data["order"]["legs"][0]["asset_class"] = "us_equity"
    if change == "malformed_legs": data["legs"] = {}
    with pytest.raises(BrokerContractError): await decode_trade_update(event(data), resolve)


@pytest.mark.asyncio
async def test_binding_cannot_cross_accounts_or_relabel_contract_identity():
    data, qualifications, resolve = fixture()
    key = next(iter(qualifications))
    value = qualifications[key]
    qualifications[key] = replace(value, binding=replace(value.binding, account_scope="other"))
    with pytest.raises(BrokerContractError): await decode_trade_update(event(data), resolve)
