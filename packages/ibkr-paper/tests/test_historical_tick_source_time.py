"""Native historical callbacks never manufacture a missing provider clock."""
from datetime import datetime, timezone
import asyncio
from types import SimpleNamespace as N

import pytest

from algo_trader_broker_adapter_ibkr_paper.client import _build_historical_tick, IBAsyncClient
from algo_trader_broker_adapter_ibkr_paper.settings import IBGatewaySettings


@pytest.mark.parametrize('value', [None, '', 'garbage', '2026-11-13', datetime(2026,11,13),
    float('nan'), float('inf'), True])
def test_missing_or_ambiguous_source_time_is_not_replaced_by_receipt(value):
    tick=N(priceBid=600, priceAsk=601, sizeBid=1, sizeAsk=2)
    if value is not None:tick.time=value
    assert _build_historical_tick(tick) is None


@pytest.mark.parametrize('value', [datetime(2026,11,13,22,tzinfo=timezone.utc),
    '2026-11-13T22:00:00Z', 1794607200])
def test_provider_time_survives_pair_conversion(value):
    tick=_build_historical_tick(N(time=value,priceBid=600,priceAsk=601,sizeBid=1,sizeAsk=2))
    assert tick.time==datetime(2026,11,13,22,tzinfo=timezone.utc)
    assert (tick.bid_price,tick.ask_price,tick.bid_size,tick.ask_size)==(600,601,1,2)


@pytest.mark.asyncio
async def test_actual_client_callback_discards_missing_clock_and_requests_native_bid_ask(monkeypatch):
    from ib_async import Stock
    at=datetime(2026,11,13,22,tzinfo=timezone.utc)
    seen=[]
    async def ticks(*args,**kwargs):
        seen.append((args,kwargs))
        return [N(priceBid=700,priceAsk=701,sizeBid=9,sizeAsk=9),
            N(time=at,priceBid=600,priceAsk=601,sizeBid=1,sizeAsk=2)]
    client=IBAsyncClient(IBGatewaySettings())
    async def run(fn):return await fn(N(reqHistoricalTicksAsync=ticks))
    monkeypatch.setattr(client,'_run_with_ib_async',run)
    result=await client.get_historical_ticks(Stock('QQQ','SMART','USD'),end_datetime=at,
        number_of_ticks=1000,what_to_show='BID_ASK',use_rth=False)
    assert len(result)==1 and result[0].time==at and result[0].bid_price==600
    assert seen[0][0][3:]==(1000,'BID_ASK') and seen[0][1]['useRth'] is False


@pytest.mark.asyncio
@pytest.mark.parametrize('cancel',[False,True])
async def test_canceled_native_read_releases_only_its_own_wrapper_request(monkeypatch,cancel):
    from ib_async import IB,Stock
    ib=IB();stock=Stock('QQQ','SMART','USD')
    other=ib.wrapper.startReq(900,stock)
    requested=asyncio.Event()
    monkeypatch.setattr(ib.client,'getReqId',lambda:1000)
    monkeypatch.setattr(ib.client,'reqHistoricalTicks',lambda *args:requested.set())
    client=IBAsyncClient(IBGatewaySettings())
    async def run(fn):return await fn(ib)
    monkeypatch.setattr(client,'_run_with_ib_async',run)
    task=asyncio.create_task(client.get_historical_ticks(stock,number_of_ticks=1000,what_to_show='BID_ASK',use_rth=False))
    await requested.wait()
    if cancel:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
    else:
        with pytest.raises(TimeoutError):await asyncio.wait_for(task,.01)
    assert ib.wrapper._futures=={900:other}
    assert 1000 not in ib.wrapper._results and 1000 not in ib.wrapper._reqId2Contract
    ib.wrapper.historicalTicksBidAsk(1000,[],True)
    assert ib.wrapper._futures=={900:other}
    ib.wrapper._endReq(900)
