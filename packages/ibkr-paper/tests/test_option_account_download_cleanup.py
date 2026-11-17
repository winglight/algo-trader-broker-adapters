import asyncio

import pytest
from ib_async import IB
from algo_trader_broker_adapter_ibkr_paper.options_account_native import (
    CompletedOrderHistoryUnavailable, completed_orders, open_orders,
)
from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient


@pytest.mark.asyncio
async def test_connection_startup_preserves_other_native_reads_but_completed_channel_has_one_owner():
    from ib_async import StartupFetch
    from algo_trader_broker_adapter_ibkr_paper.settings import IBGatewaySettings
    observed = []
    class Native:
        async def connectAsync(self, host, port, *, clientId, timeout, readonly, fetchFields):
            observed.append(fetchFields)
    client = IBAsyncClient(IBGatewaySettings(client_id=40))
    native = Native()
    client._ib_factory = lambda: native
    await client.connect()
    assert observed == [~StartupFetch.ORDERS_COMPLETE]
    assert not observed[0] & StartupFetch.ORDERS_COMPLETE
    assert all(observed[0] & flag for flag in StartupFetch if flag != StartupFetch.ORDERS_COMPLETE)


@pytest.mark.asyncio
@pytest.mark.parametrize('reader,key,method',[
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
async def test_completed_timeout_keeps_native_channel_until_real_end_before_next_capture(monkeypatch):
    ib = IB(); calls = []
    monkeypatch.setattr(ib.client, 'reqCompletedOrders', lambda *_: calls.append('request'))
    with pytest.raises(asyncio.TimeoutError):
        await completed_orders(ib, 'DU_SYNTHETIC', timeout=.001)
    pending = ib.wrapper._futures['completedOrders']
    assert not pending.done()
    with pytest.raises(asyncio.TimeoutError):
        await completed_orders(ib, 'DU_SYNTHETIC', timeout=.001)
    assert calls == ['request'], 'No second unnumbered download can mix with the timed-out source.'
    ib.wrapper.completedOrdersEnd()
    monkeypatch.setattr(ib.client, 'reqCompletedOrders', lambda *_: ib.wrapper.completedOrdersEnd())
    assert (await completed_orders(ib, 'DU_SYNTHETIC', timeout=1))['orders'] == []


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


@pytest.mark.asyncio
async def test_cancelled_startup_history_is_not_request_cancellation_or_a_complete_page(monkeypatch):
    ib = IB()
    pending = ib.wrapper.startReq('completedOrders')
    pending.cancel()  # connectAsync startup timeout leaves this native identity.
    calls = []
    monkeypatch.setattr(ib.client, 'reqCompletedOrders', lambda *_: calls.append('new'))
    with pytest.raises(CompletedOrderHistoryUnavailable):
        await completed_orders(ib, 'DU_SYNTHETIC', timeout=1)
    assert not calls
    assert ib.wrapper._futures['completedOrders'] is pending
    # Only the real late native end marker retires the outstanding download.
    ib.wrapper.completedOrdersEnd()
    monkeypatch.setattr(ib.client, 'reqCompletedOrders', lambda *_: ib.wrapper.completedOrdersEnd())
    assert (await completed_orders(ib, 'DU_SYNTHETIC', timeout=1))['orders'] == []


@pytest.mark.asyncio
async def test_http_caller_cancellation_still_propagates_without_cancelling_shared_history():
    ib = IB()
    pending = ib.wrapper.startReq('completedOrders')
    task = asyncio.create_task(completed_orders(ib, 'DU_SYNTHETIC', timeout=1))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not pending.done() and ib.wrapper._futures['completedOrders'] is pending
    ib.wrapper.completedOrdersEnd()


@pytest.mark.asyncio
async def test_ordinary_collector_keeps_running_after_cancelled_startup_history(monkeypatch):
    ib = IB()
    pending = ib.wrapper.startReq('completedOrders')
    pending.cancel()
    client = object.__new__(IBAsyncClient)
    async def run(operation):
        return await operation(ib)
    monkeypatch.setattr(client, '_run_with_ib_async', run)
    assert await client.request_completed_orders() == []
    assert ib.wrapper._futures['completedOrders'] is pending
    ib.wrapper.completedOrdersEnd()
