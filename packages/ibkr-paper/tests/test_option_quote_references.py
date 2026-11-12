from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as N
import asyncio
import pytest
from ib_async import IB, Option
from algo_trader_broker_sdk.options import OptionMarketSnapshot, OptionQuote
from algo_trader_broker_adapter_ibkr_paper.options_quote_references import closed_bindings, last_bbo, with_references

CID = 'opt_' + 'a' * 64


def adapter():
    return N(_option_reference_quotes=OrderedDict(), _option_reference_slots=asyncio.Semaphore(8))


def test_references_require_native_closed_calendar_and_research_purpose():
    binding = N(canonical_id=CID, broker_contract_id='123')
    native = N(timeZoneId='US/Eastern', liquidHours='20261111:0930-20261111:1615')
    a = N(_option_catalog={'123': (None, None, native)})
    request = N(bindings=[binding], purpose='RESEARCH')
    assert closed_bindings(a, request, datetime(2026,11,12,0,tzinfo=timezone.utc)) == {CID}
    assert closed_bindings(a, request, datetime(2026,11,11,17,tzinfo=timezone.utc)) == set()
    request.purpose='ENTRY'
    assert closed_bindings(a, request, datetime(2026,11,12,0,tzinfo=timezone.utc)) == set()
    request.purpose='RESEARCH'
    native.liquidHours=''
    assert closed_bindings(a, request, datetime(2026,11,12,0,tzinfo=timezone.utc)) == set()


@pytest.mark.asyncio
async def test_latest_paired_native_quote_keeps_source_time_and_never_uses_removed_or_future_sides(monkeypatch):
    ib, a = IB(), adapter()
    now = datetime.now(timezone.utc)
    source = now-timedelta(hours=3)
    contract = Option('QQQ','20261112',688,'C','SMART',conId=123)
    monkeypatch.setattr(ib.client,'getReqId',lambda: 41)
    calls=[]
    def receive(req_id, native, start, end, count, kind, rth, ignore_size, options):
        calls.append((start,end,count,kind,rth))
        assert native is contract and not start and end and count == 1000 and kind == 'BID_ASK'
        rows=[N(time=source-timedelta(seconds=1),priceBid=1.8,priceAsk=1.9,sizeBid=1,sizeAsk=1),
            N(time=source,priceBid=1.9,priceAsk=2,sizeBid=1,sizeAsk=1),
            N(time=source+timedelta(seconds=1),priceBid=0,priceAsk=0,sizeBid=0,sizeAsk=0),
            N(time=now+timedelta(days=1),priceBid=9,priceAsk=10,sizeBid=1,sizeAsk=1)]
        ib.wrapper._endReq(req_id,result=rows)
    monkeypatch.setattr(ib.client,'reqHistoricalTicks',receive)
    value=await last_bbo(a,ib,contract,asyncio.get_running_loop().time()+1)
    assert value == ('1.9','2',1,1,source)
    assert await last_bbo(a,ib,contract,asyncio.get_running_loop().time()+1) == value
    assert len(calls)==1 and not ib.wrapper._futures


@pytest.mark.asyncio
async def test_timeout_cleans_only_owned_request_and_never_upgrades_execution(monkeypatch):
    ib,a=IB(),adapter()
    contract=N(conId=123)
    monkeypatch.setattr(ib.client,'getReqId',lambda: 41)
    monkeypatch.setattr(ib.client,'reqHistoricalTicks',lambda *args: None)
    other=ib.wrapper.startReq(40)
    assert await last_bbo(a,ib,contract,asyncio.get_running_loop().time()+.01) is None
    assert ib.wrapper._futures == {40:other}
    request=N(purpose='ENTRY')
    snapshot=OptionMarketSnapshot('read',(),(CID,),False,0,datetime.now(timezone.utc).isoformat().replace('+00:00','Z'))
    assert await with_references(a,ib,request,[],snapshot,{CID},0) is snapshot
    ib.wrapper._endReq(40)


@pytest.mark.asyncio
async def test_reference_replaces_missing_sides_without_refreshing_oi_clock(monkeypatch):
    import algo_trader_broker_adapter_ibkr_paper.options_quote_references as module
    now=datetime.now(timezone.utc)
    at=(now-timedelta(hours=3)).isoformat().replace('+00:00','Z')
    oi_at=(now-timedelta(seconds=1)).isoformat().replace('+00:00','Z')
    quote=OptionQuote(CID,None,None,0,0,now.isoformat().replace('+00:00','Z'),now.isoformat().replace('+00:00','Z'),'IBKR','IBKR_LIVE',
        'MISSING','account',None,None,None,None,None,None,None,123,None,oi_observed_at=oi_at)
    snapshot=OptionMarketSnapshot('read',(quote,),(),True,0,now.isoformat().replace('+00:00','Z'))
    async def sourced(*args):return ('1.9','2',1,1,datetime.fromisoformat(at))
    monkeypatch.setattr(module,'last_bbo',sourced)
    result=await with_references(adapter(),None,N(purpose='RESEARCH',bindings=[N(canonical_id=CID)]),
        [N(conId=123)],snapshot,{CID},asyncio.get_running_loop().time()+1)
    reference=result.quotes[0]
    assert reference.bid=='1.9' and reference.ask=='2' and reference.quote_at==at.replace('+00:00','Z')
    assert reference.quality=='RESEARCH_ONLY' and reference.reference_pricing_allowed is True
    assert reference.oi==123 and reference.oi_observed_at==oi_at and quote.bid is None


@pytest.mark.asyncio
async def test_history_starts_alongside_full_live_observation_budget(monkeypatch):
    import algo_trader_broker_adapter_ibkr_paper.options_quote_references as references
    import algo_trader_broker_adapter_ibkr_paper.options_quotes as quotes
    from tests.test_options_reads import read_flow
    started=asyncio.Event()
    monkeypatch.setattr(references,'closed_bindings',lambda a,r,n:{b.canonical_id for b in r.bindings})
    async def history(*args):
        started.set()
        return None
    monkeypatch.setattr(references,'last_bbo',history)
    original=quotes.read_snapshot
    async def live(ib,request,contracts,*,timeout):
        await asyncio.wait_for(started.wait(),.1)
        assert timeout > 2, 'Historical BBO must not truncate OI/Greek receive time'
        return await original(ib,request,contracts,timeout=timeout)
    monkeypatch.setattr(quotes,'read_snapshot',live)
    await read_flow(monkeypatch)
