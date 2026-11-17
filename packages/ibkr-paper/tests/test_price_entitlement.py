"""BUG-867 native wrapper replay: no Gateway, strategies, accounts or orders."""
import asyncio
import itertools
from datetime import datetime, timezone

import pytest
from ib_async import IB, Future

from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient
from algo_trader_broker_adapter_ibkr_paper.settings import IBGatewaySettings
from algo_trader_broker_adapter_ibkr_paper.exceptions import IBMarketDataError

MESSAGE = ("Requested market data is not subscribed. Check API status by selecting "
           "the Account menu then under Management choose Market Data Subscription Manager "
           "and/or availability of delayed data.Delayed market data is available.VIX NOV'26 (VXX6) /TOP/ALL")


@pytest.fixture
def native(monkeypatch):
    ib = IB()
    contract = Future('VIX', '20261118', 'CFE', conId=857096518,
                      localSymbol='VXX6', tradingClass='VX', multiplier='1000')
    sequence = itertools.count(214346)
    calls, cancelled = [], []
    monkeypatch.setattr(ib.client, 'getReqId', lambda: next(sequence))
    monkeypatch.setattr(ib.client, 'reqMktData', lambda *args: calls.append(args))
    monkeypatch.setattr(ib.client, 'cancelMktData', cancelled.append)
    def forbidden(*args, **kwargs):
        raise AssertionError("No connection, order, subscription mode change or fallback allowed")
    for name in ('reqMarketDataType', 'placeOrder', 'connect', 'connectAsync'):
        if hasattr(ib.client, name):
            monkeypatch.setattr(ib.client, name, forbidden)
    client = IBAsyncClient(IBGatewaySettings())
    async def run(fn):
        return await fn(ib)
    monkeypatch.setattr(client, '_run_with_ib_async', run)
    return ib, contract, client, calls, cancelled


def tick(ib, req_id, value=30.5):
    ib.wrapper.lastTime = datetime.now(timezone.utc)
    ib.wrapper.priceSizeTick(req_id, 4, value, 1)
    ib.wrapper.tcpDataProcessed()


@pytest.mark.asyncio
@pytest.mark.parametrize('after_tick', [False, True])
async def test_354_is_a_scoped_wait_source_failure_with_original_log(native, caplog, after_tick):
    ib, contract, client, calls, cancelled = native
    initial = len(ib.errorEvent)
    stream = await client.stream_real_time_price(contract)
    assert not hasattr(ib.wrapper.reqId2Ticker[stream.req_id], 'reqId')
    if after_tick:
        tick(ib, stream.req_id)
        assert (await anext(stream)).last == 30.5
    ib.wrapper.error(stream.req_id, 354, MESSAGE, '')
    with pytest.raises(IBMarketDataError) as caught:
        await asyncio.wait_for(anext(stream), .5)
    assert caught.value.code == 'broker_market_data_not_subscribed'
    assert caught.value.details['nativeCode'] == 354
    assert caught.value.details['recovery'] == 'WAIT_SOURCE'
    assert MESSAGE in str(caught.value)
    assert any(r.name == 'ib_async.wrapper' and r.levelname == 'ERROR'
               and 'Error 354, reqId 214346' in r.getMessage() for r in caplog.records)
    assert cancelled == [stream.req_id]
    assert len(ib.errorEvent) == initial
    assert ib.wrapper.reqId2Ticker == {}
    assert ib.wrapper._reqId2Contract == {}
    assert calls == [(stream.req_id, contract, '', False, False, [])]
    await stream.aclose()
    with pytest.raises(StopAsyncIteration):
        await anext(stream)
    assert cancelled == [stream.req_id]


@pytest.mark.asyncio
async def test_two_clicks_same_contract_are_isolated_and_reopen(native):
    ib, contract, client, calls, cancelled = native
    retained = ib.wrapper.startTicker(900, contract, 'mktData')
    first = await client.stream_real_time_price(contract)
    second = await client.stream_real_time_price(contract)
    assert first.req_id != second.req_id
    ib.wrapper.error(first.req_id, 354, MESSAGE, '')
    with pytest.raises(IBMarketDataError):
        await anext(first)
    # The other request and existing chart subscription survive.
    tick(ib, second.req_id, 31)
    assert (await anext(second)).last == 31
    assert ib.wrapper.reqId2Ticker[900] is retained
    assert ib.wrapper.ticker2ReqId['mktData'][retained] == 900
    await second.aclose()
    reopened = await client.stream_real_time_price(contract)
    tick(ib, reopened.req_id, 32)
    assert (await anext(reopened)).last == 32
    await reopened.aclose()
    assert cancelled == [first.req_id, second.req_id, reopened.req_id]
    assert ib.wrapper.reqId2Ticker == {900: retained}


@pytest.mark.asyncio
@pytest.mark.parametrize('code,message', [(354, MESSAGE), (10197, 'No market data during competing live session')])
async def test_unrelated_native_request_error_does_not_poison_stream(native, code, message):
    ib, contract, client, _, _ = native
    stream = await client.stream_real_time_price(contract)
    ib.wrapper.error(stream.req_id + 1, code, message, '')
    tick(ib, stream.req_id)
    assert (await asyncio.wait_for(anext(stream), .5)).last == 30.5
    await stream.aclose()


@pytest.mark.asyncio
async def test_synchronous_rejection_before_subscribe_returns_is_not_lost(native, monkeypatch):
    ib, contract, client, _, cancelled = native
    monkeypatch.setattr(ib.client, 'reqMktData',
                        lambda req_id, *args: ib.wrapper.error(req_id, 354, MESSAGE, ''))
    stream = await client.stream_real_time_price(contract)
    with pytest.raises(IBMarketDataError):
        await asyncio.wait_for(anext(stream), .5)
    assert cancelled == [stream.req_id]


@pytest.mark.asyncio
async def test_rejection_wins_when_tick_and_error_arrive_together(native):
    ib, contract, client, _, _ = native
    stream = await client.stream_real_time_price(contract)
    tick(ib, stream.req_id)
    ib.wrapper.error(stream.req_id, 354, MESSAGE, '')
    with pytest.raises(IBMarketDataError):
        await asyncio.wait_for(anext(stream), .5)


@pytest.mark.asyncio
@pytest.mark.parametrize('started', [False, True])
async def test_close_before_first_read_or_during_read_is_idempotent(native, started):
    ib, contract, client, _, cancelled = native
    initial = len(ib.errorEvent)
    stream = await client.stream_real_time_price(contract)
    if started:
        waiting = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
    await stream.aclose()
    await stream.aclose()
    if started:
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(waiting, .5)
    assert cancelled == [stream.req_id]
    assert len(ib.errorEvent) == initial
    assert ib.wrapper.reqId2Ticker == {}


@pytest.mark.asyncio
async def test_cancel_pending_read_cleans_up(native):
    ib, contract, client, _, cancelled = native
    stream = await client.stream_real_time_price(contract)
    waiting = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert cancelled == [stream.req_id]
    assert ib.wrapper.reqId2Ticker == {}


@pytest.mark.asyncio
async def test_competing_session_keeps_existing_error_contract(native):
    ib, contract, client, _, cancelled = native
    stream = await client.stream_real_time_price(contract)
    ib.wrapper.error(stream.req_id, 10197, 'No market data during competing live session', '')
    with pytest.raises(IBMarketDataError) as caught:
        await anext(stream)
    assert caught.value.code == 'screener_market_data_competing_session'
    assert caught.value.details['recovery'] == 'WAIT_SOURCE'
    assert cancelled == [stream.req_id]


@pytest.mark.asyncio
async def test_native_send_failure_detaches_callbacks(native, monkeypatch):
    ib, contract, client, _, _ = native
    initial = len(ib.errorEvent)
    def fail(*args):
        raise RuntimeError('mock send failed')
    monkeypatch.setattr(ib.client, 'reqMktData', fail)
    with pytest.raises(IBMarketDataError, match='Failed to subscribe'):
        await client.stream_real_time_price(contract)
    assert len(ib.errorEvent) == initial
    assert ib.wrapper.reqId2Ticker == {}

@pytest.mark.asyncio
async def test_consecutive_ticks_keep_stream_alive(native):
    ib, contract, client, _, cancelled = native
    stream = await client.stream_real_time_price(contract)
    for value in (1, 2, 3):
        waiting = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        tick(ib, stream.req_id, value)
        assert (await asyncio.wait_for(waiting, .5)).last == value
    await stream.aclose()
    assert cancelled == [stream.req_id]


@pytest.mark.asyncio
@pytest.mark.parametrize('action', ['failure', 'close', 'cancel'])
async def test_cancel_transport_failure_keeps_primary_result(native, monkeypatch, caplog, action):
    ib, contract, client, _, cancelled = native
    stream = await client.stream_real_time_price(contract)
    def fail(req_id):
        cancelled.append(req_id)
        raise ConnectionError('mock cancel transport failure')
    monkeypatch.setattr(ib.client, 'cancelMktData', fail)
    if action == 'failure':
        ib.wrapper.error(stream.req_id, 354, MESSAGE, '')
        with pytest.raises(IBMarketDataError) as caught:
            await anext(stream)
        assert caught.value.details['nativeCode'] == 354
    elif action == 'cancel':
        waiting = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
    else:
        await stream.aclose()
    await stream.aclose()
    assert cancelled == [stream.req_id]
    assert ib.wrapper.reqId2Ticker == {}
    assert any('Failed to cancel closed IB real-time price request' in r.message
               for r in caplog.records)


@pytest.mark.asyncio
async def test_snapshot_flag_keeps_native_request_shape(native):
    ib, contract, client, calls, cancelled = native
    stream = await client.stream_real_time_price(contract, snapshot=True)
    assert calls == [(stream.req_id, contract, '', True, False, [])]
    tick(ib, stream.req_id)
    assert (await anext(stream)).last == 30.5
    await stream.aclose()
    assert cancelled == [stream.req_id]
