import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from ib_async import Future, LimitOrder, MarketOrder, StopOrder

from algo_trader_broker_sdk.submission_gate import SubmissionGate
from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient, IBOrderError


@pytest.mark.parametrize('kind', ['MKT', 'LMT', 'STP'])
@pytest.mark.parametrize('side', ['BUY', 'SELL'])
def test_guarded_futures_submit_only_the_requested_order(kind, side):
    async def scenario():
        order = (MarketOrder(side, 1) if kind == 'MKT' else
                 LimitOrder(side, 1, 30000) if kind == 'LMT' else StopOrder(side, 1, 29960))
        order.openClose = 'C' if kind == 'STP' else 'O'
        contract = Future(symbol='MNQ', exchange='CME', conId=123)
        def send(actual_contract, actual_order):
            assert gate.consumed
            assert actual_contract is contract and actual_order is order
            actual_order.orderId = 101
            return SimpleNamespace(order=order, contract=contract, orderStatus=SimpleNamespace(
                status='Submitted', filled=0, remaining=1, avgFillPrice=0))
        ib = SimpleNamespace(isConnected=lambda: True, placeOrder=Mock(side_effect=send))
        client = object.__new__(IBAsyncClient)
        client.ensure_connected = AsyncMock(return_value=ib)
        client._trade_update_handler = client._account_update_handler = None
        gate = SubmissionGate(AsyncMock(), lambda: None)
        result = await client._submit_order(contract, order, submission_gate=gate)
        ib.placeOrder.assert_called_once_with(contract, order)
        assert result.order_id == 101 and result.status == 'Submitted'
        assert order.transmit and order.parentId == 0
    asyncio.run(scenario())


def test_rejected_gate_never_sends_a_futures_order():
    async def scenario():
        client = object.__new__(IBAsyncClient)
        ib = SimpleNamespace(isConnected=lambda: True, placeOrder=Mock())
        client.ensure_connected = AsyncMock(return_value=ib)
        client._trade_update_handler = client._account_update_handler = None
        gate = SubmissionGate(AsyncMock(side_effect=ValueError('budget denied')), lambda: None)
        with pytest.raises(ValueError, match='budget denied'):
            await client._submit_order(Future(symbol='MNQ'), MarketOrder('BUY', 1), submission_gate=gate)
        ib.placeOrder.assert_not_called()
        assert not gate.consumed
    asyncio.run(scenario())


def test_unknown_futures_submission_never_sends_another_order():
    async def scenario():
        client = object.__new__(IBAsyncClient)
        ib = SimpleNamespace(isConnected=lambda: True, placeOrder=Mock(side_effect=TimeoutError()))
        client.ensure_connected = AsyncMock(return_value=ib)
        client._trade_update_handler = client._account_update_handler = None
        gate = SubmissionGate(AsyncMock(), lambda: None)
        with pytest.raises(IBOrderError) as error:
            await client._submit_order(Future(symbol='MNQ'), MarketOrder('BUY', 1), submission_gate=gate)
        assert error.value.code == 'broker_order_outcome_unknown'
        ib.placeOrder.assert_called_once()
    asyncio.run(scenario())
