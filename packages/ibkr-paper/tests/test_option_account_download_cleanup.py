import asyncio

import pytest
from ib_async import IB
from algo_trader_broker_adapter_ibkr_paper.options_account_native import completed_orders, open_orders
from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient


@pytest.mark.asyncio
@pytest.mark.parametrize('reader,key,method',[
    (completed_orders,'completedOrders','reqCompletedOrders'),
    (open_orders,'openOrders','reqAllOpenOrders'),
])
async def test_timed_out_download_releases_its_actual_native_future_and_allows_recovery(monkeypatch,reader,key,method):
    ib=IB()
    monkeypatch.setattr(ib.client,method,lambda *args:None)
    with pytest.raises(asyncio.TimeoutError):
        await reader(ib,'DU_SYNTHETIC',timeout=0.001)
    assert key not in ib.wrapper._futures and key not in ib.wrapper._results
    monkeypatch.setattr(ib.client,method,lambda *args:ib.wrapper._endReq(key))
    result=await reader(ib,'DU_SYNTHETIC',timeout=1)
    assert result['orders']==[] and key not in ib.wrapper._futures


@pytest.mark.asyncio
async def test_download_cleanup_does_not_end_a_replaced_consumer_future(monkeypatch):
    ib=IB();other=None
    def replace(*args):
        nonlocal other
        original=ib.wrapper._futures['completedOrders']
        other=asyncio.get_running_loop().create_future()
        ib.wrapper._futures['completedOrders']=other
        original.set_result([])
    monkeypatch.setattr(ib.client,'reqCompletedOrders',replace)
    await completed_orders(ib,'DU_SYNTHETIC',timeout=1)
    assert ib.wrapper._futures['completedOrders'] is other and not other.done()
    ib.wrapper._endReq('completedOrders')


@pytest.mark.asyncio
async def test_account_reader_drains_old_download_before_starting_a_complete_capture(monkeypatch):
    ib=IB();pending=ib.wrapper.startReq('completedOrders');calls=[]
    def start(api_only):
        assert pending.done() and api_only is False
        calls.append('new-full-window')
        ib.wrapper._endReq('completedOrders')
    monkeypatch.setattr(ib.client,'reqCompletedOrders',start)
    asyncio.get_running_loop().call_soon(ib.wrapper._endReq,'completedOrders')
    result=await completed_orders(ib,'DU_SYNTHETIC',timeout=1)
    assert calls==['new-full-window'] and result['orders']==[]


@pytest.mark.asyncio
async def test_ordinary_collector_does_not_replace_account_reader_request(monkeypatch):
    ib=IB();pending=ib.wrapper.startReq('completedOrders')
    def forbidden(*args,**kwargs):raise AssertionError('Would orphan native account request')
    monkeypatch.setattr(ib,'reqCompletedOrdersAsync',forbidden)
    client=object.__new__(IBAsyncClient)
    async def run(operation):return await operation(ib)
    monkeypatch.setattr(client,'_run_with_ib_async',run)
    asyncio.get_running_loop().call_soon(ib.wrapper._endReq,'completedOrders')
    assert await client.request_completed_orders()==[]
    assert pending.done()
