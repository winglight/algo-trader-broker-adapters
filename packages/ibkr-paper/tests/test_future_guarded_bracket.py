import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from ib_async import Future, LimitOrder
from algo_trader_broker_sdk.submission_gate import SubmissionGate
from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient, IBOrderError
from algo_trader_broker_adapter_ibkr_paper.orders import OrderResult


@pytest.mark.parametrize('market', [False,True])
def test_guarded_native_bracket_stages_parent_and_protection_before_transmit(market):
    async def scenario():
        sent, ids = [], iter([101, 102])
        ib = SimpleNamespace(isConnected=lambda: True,
            client=SimpleNamespace(getReqId=lambda: next(ids)),
            placeOrder=lambda contract, order: sent.append(order) or SimpleNamespace(order=order))
        client = object.__new__(IBAsyncClient)
        client.ensure_connected = AsyncMock(return_value=ib)
        client._trade_update_handler = client._account_update_handler = None
        async def observed(ib, contract, order, *, submitted_trade=None):
            assert submitted_trade is not None
            return OrderResult(order.orderId, 'Submitted', 0, 1, None, {'conId': 123}, perm_id=str(order.orderId+1000))
        client._place_order_async = observed
        gate = SubmissionGate(AsyncMock(), lambda: None)
        contract = Future(symbol='MNQ', exchange='CME', conId=123)
        parent = LimitOrder('BUY', 1, 30000, account='DU_TEST', openClose='O', orderRef='entry')
        if market:
            parent.orderType='MKT'; parent.lmtPrice=0
        result = await client._submit_order(contract, parent, submission_gate=gate, protective_stop_price=29960,
                                            protective_reference_price=30000)
        assert gate.consumed and len(sent) == 2
        assert sent[0].transmit is False and sent[1].transmit is True
        assert sent[1].parentId == sent[0].orderId == 101
        assert sent[1].action == 'SELL' and sent[1].orderType == 'STP' and sent[1].auxPrice == 29960
        assert sent[1].account == 'DU_TEST' and sent[1].totalQuantity == 1
        assert result.protective_order_id == 102
    asyncio.run(scenario())


def test_rejected_native_gate_never_stages_either_leg():
    async def scenario():
        sent, ids = [], iter([101, 102])
        ib = SimpleNamespace(isConnected=lambda: True, client=SimpleNamespace(getReqId=lambda: next(ids)),
                             placeOrder=lambda *args: sent.append(args))
        client = object.__new__(IBAsyncClient)
        client.ensure_connected = AsyncMock(return_value=ib)
        client._trade_update_handler = client._account_update_handler = None
        async def reject(): raise ValueError('budget denied')
        gate = SubmissionGate(reject, lambda: None)
        with pytest.raises(ValueError, match='budget denied'):
            await client._submit_order(Future(symbol='MNQ', exchange='CME', conId=123),
                LimitOrder('BUY', 1, 30000, account='DU_TEST', openClose='O', orderRef='entry'),
                submission_gate=gate, protective_stop_price=29960)
        assert not sent and not gate.consumed
    asyncio.run(scenario())


def test_pending_local_child_is_not_reported_as_broker_confirmed_protection():
    async def scenario():
        sent=[];ids=iter([101,102])
        ib=SimpleNamespace(isConnected=lambda:True,client=SimpleNamespace(getReqId=lambda:next(ids)),
            placeOrder=lambda c,o:sent.append(o) or SimpleNamespace(order=o))
        client=object.__new__(IBAsyncClient);client.ensure_connected=AsyncMock(return_value=ib)
        client._trade_update_handler=client._account_update_handler=None
        client._place_order_async=AsyncMock(return_value=OrderResult(102,'PendingSubmit',0,1,None,{'conId':123},perm_id='0'))
        gate=SubmissionGate(AsyncMock(),lambda:None)
        with pytest.raises(IBOrderError,match='outcome is unknown'):
            await client._submit_order(Future(symbol='MNQ',exchange='CME',conId=123),
                LimitOrder('BUY',1,30000,account='DU_TEST',openClose='O',orderRef='entry'),
                submission_gate=gate,protective_stop_price=29960)
        assert len(sent)==2
        client._place_order_async.assert_awaited_once()
    asyncio.run(scenario())
