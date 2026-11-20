import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from ib_async import Contract, OrderState, StopOrder

from algo_trader_broker_adapter_ibkr_paper.adapter import IBKRPaperAdapter
from algo_trader_broker_adapter_ibkr_paper.client import (
    IBAsyncClient, IBOrderError, _ib_with_stop_session_readback, _order_to_dict,
)
from algo_trader_broker_sdk import FutureOrderRequest


def test_stop_modification_copies_native_fields_and_preserves_identity():
    async def scenario():
        adapter = object.__new__(IBAsyncClient)
        adapter._open_orders_lock = asyncio.Lock()
        contract = Contract(conId=11)
        existing = StopOrder("BUY", 2, 105, orderId=89, account="DU1", tif="GTC", permId=42,
                             parentId=13, ocaGroup="bracket-13", outsideRth=True, triggerMethod=4)
        trade = SimpleNamespace(order=existing, contract=contract, orderStatus=SimpleNamespace(status="PreSubmitted"))
        ib = SimpleNamespace(reqOpenOrdersAsync=AsyncMock(return_value=[trade]))
        request = StopOrder("BUY", 3, 105.75, orderId=89, account="DU1")
        modified = await adapter._copy_working_stop(ib, contract, request)
        assert (modified.auxPrice, modified.totalQuantity) == (105.75, 3)
        assert (existing.auxPrice, existing.totalQuantity) == (105, 2)
        assert (modified.orderId, modified.permId, modified.parentId) == (89, 42, 13)
        assert (modified.tif, modified.ocaGroup, modified.outsideRth, modified.triggerMethod) == (
            "GTC", "bracket-13", True, 4,
        )
        trade.orderStatus.status = "Filled"
        with pytest.raises(IBOrderError):
            await adapter._copy_working_stop(ib, contract, request)

    asyncio.run(scenario())


@pytest.mark.asyncio
async def test_new_futures_closing_stop_passes_eth_to_native_order():
    adapter = object.__new__(IBKRPaperAdapter)
    client = object.__new__(IBAsyncClient)
    adapter._client = client
    contract = Contract(conId=11, secType="FUT", symbol="MNQ")
    client.ensure_connected = AsyncMock()
    client.qualify_contract = AsyncMock(return_value=contract)
    client._trade_update_handler = client._account_update_handler = None
    client._place_order_async = AsyncMock(return_value=SimpleNamespace(
        order_id=89, perm_id="42", status="PreSubmitted", filled=0, remaining=2,
        avg_fill_price=None, contract={"conId": 11},
    ))

    await adapter.place_future_order(FutureOrderRequest(
        symbol="MNQ", side="BUY", quantity=2, order_type="STP", stop_price=105,
        contract_id=11, last_trade_date="20261218", account="DU1", position_effect="CLOSE",
        tif="GTC", adapter_parameters={"outsideRth": True},
    ))
    placed = client._place_order_async.await_args.args[2]
    assert _order_to_dict(placed)["outsideRth"] is True
    assert placed.tif == "GTC"


def test_native_open_order_refreshes_cached_futures_stop_session():
    ib = _ib_with_stop_session_readback()
    contract = Contract(conId=11, secType="FUT", symbol="MNQ")
    state = OrderState(status="PreSubmitted")
    for outside_rth in (False, True, False):
        order = StopOrder("BUY", 2, 105, orderId=89, clientId=40, permId=42, outsideRth=outside_rth)
        ib.wrapper.openOrder(89, contract, order, state)
        assert _order_to_dict(ib.wrapper.permId2Trade[42].order)["outsideRth"] is outside_rth


@pytest.mark.asyncio
async def test_entry_limit_amends_only_price_on_the_exact_working_order():
    from ib_async import LimitOrder

    adapter = object.__new__(IBAsyncClient)
    adapter._open_orders_lock = asyncio.Lock()
    contract = Contract(conId=11, secType="FUT", symbol="MNQ", currency="USD")
    existing = LimitOrder("BUY", 2, 100, orderId=89, account="DU1", tif="GTC", permId=42,
                          orderRef="copilot-entry", openClose="O", outsideRth=True)
    trade = SimpleNamespace(order=existing, contract=contract, orderStatus=SimpleNamespace(status="Submitted"))
    ib = SimpleNamespace(reqOpenOrdersAsync=AsyncMock(return_value=[trade]))
    request = LimitOrder("BUY", 2, 99.75, orderId=89, account="DU1", orderRef="copilot-entry", openClose="O")
    native, amended = await adapter._copy_working_entry_limit(ib, contract, request)
    assert native is contract and amended.lmtPrice == 99.75 and existing.lmtPrice == 100
    assert (amended.orderId, amended.permId, amended.tif, amended.totalQuantity, amended.outsideRth) == (
        89, 42, "GTC", 2, True,
    )
    request.orderRef = "other-order"
    with pytest.raises(IBOrderError):
        await adapter._copy_working_entry_limit(ib, contract, request)
    request.orderRef = existing.orderRef
    trade.orderStatus.status = "Filled"
    with pytest.raises(IBOrderError):
        await adapter._copy_working_entry_limit(ib, contract, request)
