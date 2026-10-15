"""Native wrapper callbacks verify the finite BBO path and request isolation."""
import asyncio
from datetime import datetime, timezone

import pytest
from ib_async import IB, Stock

from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient
from algo_trader_broker_adapter_ibkr_paper.settings import IBGatewaySettings


@pytest.mark.asyncio
@pytest.mark.parametrize('feed,expected', [(1, 'IBKR_LIVE'), (2, 'IBKR_FROZEN'), (3, 'IBKR_DELAYED')])
async def test_bbo_returns_without_snapshot_end_and_keeps_existing_stream(monkeypatch, feed, expected):
    ib, contract = IB(), Stock('QQQ', 'SMART', 'USD', conId=100)
    existing = ib.wrapper.startTicker(900, contract, 'mktData')
    existing.bid, existing.ask = 1, 2
    canceled, stamps = [], []
    monkeypatch.setattr(ib.client, 'getReqId', lambda: 1000)
    monkeypatch.setattr(ib.client, 'cancelMktData', canceled.append)

    def subscribe(req_id, native, generic, snapshot, regulatory, options):
        assert (req_id, native, generic, snapshot, regulatory, options) == (1000, contract, '', False, False, [])
        def bid():
            stamp = datetime.now(timezone.utc)
            stamps.append(stamp)
            ib.wrapper.lastTime = stamp
            ib.wrapper.marketDataType(req_id, feed)
            ib.wrapper.priceSizeTick(req_id, 1, 743.85, 5)
            ib.wrapper.tcpDataProcessed()
        def ask_and_last():
            ib.wrapper.lastTime = datetime.now(timezone.utc)
            ib.wrapper.priceSizeTick(req_id, 2, 743.93, 7)
            ib.wrapper.priceSizeTick(req_id, 4, 743.90, 3)
            ib.wrapper.tcpDataProcessed()
        asyncio.get_running_loop().call_soon(bid)
        asyncio.get_running_loop().call_later(.01, ask_and_last)
    monkeypatch.setattr(ib.client, 'reqMktData', subscribe)
    async def forbidden(*args, **kwargs):
        raise AssertionError('Must not wait for snapshot-end or reuse contract tickers')
    monkeypatch.setattr(ib, 'reqTickersAsync', forbidden)
    client = IBAsyncClient(IBGatewaySettings())
    async def run(fn): return await fn(ib)
    monkeypatch.setattr(client, '_run_with_ib_async', run)
    result = await asyncio.wait_for(client.request_market_snapshot(contract, snapshot_timeout=1), .5)
    assert (result['symbol'], result['dataFeed'], result['bid'], result['ask']) == ('QQQ', expected, '743.85', '743.93')
    assert result['timestamp'] == stamps[0].isoformat() and 'quoteTimestamp' not in result
    assert (result['bidSize'], result['askSize']) == ('5', '7')
    assert canceled == [1000] and ib.wrapper.reqId2Ticker == {900: existing}
    assert ib.wrapper.ticker2ReqId['mktData'][existing] == 900
    assert (existing.bid, existing.ask) == (1, 2)


@pytest.mark.asyncio
@pytest.mark.parametrize('cancel', [False, True])
async def test_timeout_and_cancellation_release_only_owned_request(monkeypatch, cancel):
    ib, contract = IB(), Stock('SPY', 'SMART', 'USD', conId=100)
    initial_handlers = len(ib.errorEvent)
    existing = ib.wrapper.startTicker(900, contract, 'mktData')
    canceled = []
    monkeypatch.setattr(ib.client, 'getReqId', lambda: 1000)
    monkeypatch.setattr(ib.client, 'cancelMktData', canceled.append)
    monkeypatch.setattr(ib.client, 'reqMktData', lambda *args: None)
    client = IBAsyncClient(IBGatewaySettings())
    async def run(fn): return await fn(ib)
    monkeypatch.setattr(client, '_run_with_ib_async', run)
    if cancel:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(client.request_market_snapshot(contract, snapshot_timeout=1), .01)
    else:
        result = await client.request_market_snapshot(contract, snapshot_timeout=.01)
        assert result['timestamp'] is None and result['bid'] is None and result['ask'] is None
        assert result['dataFeed'] == 'UNKNOWN'
    assert canceled == [1000] and ib.wrapper.reqId2Ticker == {900: existing}
    assert ib.wrapper._reqId2Contract == {900: contract}
    assert len(ib.errorEvent) == initial_handlers
