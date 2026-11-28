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


@pytest.mark.asyncio
async def test_qualified_strikes_share_underlying_and_complete_concurrently(monkeypatch):
    from algo_trader_broker_sdk.options import (
        BrokerOptionBinding, OptionContract, OptionContractKey, QualificationRequest, option_contract_id,
    )
    from algo_trader_broker_adapter_ibkr_paper import options_reads

    keys = [OptionContractKey('QQQ','2026-11-27','C',strike,'USD',100,'AMERICAN','PHYSICAL','STANDARD_100_SHARES')
            for strike in ('649','650')]
    contracts = tuple(OptionContract(option_contract_id(key),key,None,None,None,'UNKNOWN') for key in keys)
    request = QualificationRequest(protocol_version='options/1.0', execution_target='b'*64, profile_id='profile',
        expected_generation=1, account='account', environment='paper', contracts=contracts)
    started, running = [], asyncio.Event()
    underlying_reads = []
    a = N(_option_catalog={}, _option_catalog_reads=asyncio.Semaphore(8))

    async def read(request, operation):
        return await operation(None, None)

    async def underlying(ib, symbol):
        underlying_reads.append(symbol)
        return 123

    async def details(ib, key, *, foreground):
        assert foreground
        started.append(key)
        if len(started) == 2:
            running.set()
        await asyncio.wait_for(running.wait(), .5)
        return [contracts[keys.index(key)]]

    a._option_read, a._underlying, a._catalog_details = read, underlying, details
    a._remember_contract = lambda bound, detail, contract: BrokerOptionBinding(
        'ibkr-paper','1','paper','account',contract.canonical_id,contract.key.strike,
        'NATIVE','SMART',options_reads.now_wire(),'metadata')
    monkeypatch.setattr(options_reads, 'contract_from_details', lambda detail, symbol, under_id: detail)
    result = await options_reads.IBOptionReads.qualify_option_contracts(a, request)
    assert underlying_reads == ['QQQ']
    assert [row.contract for row in result.results] == list(contracts)
    assert all(row.status == 'EXACT' for row in result.results)


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
        assert native is contract and not start and end and count == 32 and kind == 'BID_ASK'
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
async def test_silent_contract_releases_reference_slot_for_the_next_strike(monkeypatch):
    ib, a = IB(), adapter()
    a._option_reference_slots = asyncio.Semaphore(1)
    ids = iter((40, 41))
    at = datetime.now(timezone.utc) - timedelta(hours=1)
    sent = []
    monkeypatch.setattr(ib.client, 'getReqId', lambda: next(ids))

    def historical(req_id, contract, *args):
        sent.append(contract.conId)
        if contract.conId == 2:
            row = N(time=at, priceBid=1.9, priceAsk=2, sizeBid=1, sizeAsk=1)
            ib.wrapper._endReq(req_id, result=[row])

    monkeypatch.setattr(ib.client, 'reqHistoricalTicks', historical)
    deadline = asyncio.get_running_loop().time() + 1
    silent, quote = await asyncio.wait_for(asyncio.gather(
        last_bbo(a, ib, N(conId=1), deadline, max_wait=.01),
        last_bbo(a, ib, N(conId=2), deadline, max_wait=.01)), .5)
    assert sent == [1, 2] and silent is None and quote == ('1.9','2',1,1,at)
    assert not ib.wrapper._futures


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


@pytest.mark.asyncio
async def test_research_reads_overlap_within_the_native_line_budget(monkeypatch):
    from dataclasses import replace
    from algo_trader_broker_sdk.options import BrokerOptionBinding, SnapshotRequest
    from algo_trader_broker_adapter_ibkr_paper.options_reads import IBOptionReads

    a = adapter()
    IBOptionReads._init_option_reads(a)
    observed = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    bindings = tuple(BrokerOptionBinding('ibkr-paper','1','paper','account',f'opt_{i:064x}',
        str(i),'NATIVE','SMART',observed,'metadata') for i in range(1,41))
    large = SnapshotRequest('options/1.0','b'*64,'profile',1,'account','paper',
        bindings,'IBKR_LIVE','RESEARCH',30000,2000)
    page1 = replace(large,bindings=bindings[:16])
    page2 = replace(large,bindings=bindings[16:32])
    started = asyncio.Queue()
    release = asyncio.Event()

    async def run(request, operation):
        await started.put(request)
        await release.wait()

    monkeypatch.setattr(a,'_option_read',run,raising=False)
    async with asyncio.timeout(2):
        async with asyncio.TaskGroup() as tasks:
            first = tasks.create_task(IBOptionReads.option_snapshot(a,page1))
            second = tasks.create_task(IBOptionReads.option_snapshot(a,page2))
            assert await started.get() in (page1,page2)
            assert await started.get() in (page1,page2)
            waiting = tasks.create_task(IBOptionReads.option_snapshot(a,large))
            first.cancel()
            await asyncio.gather(first,return_exceptions=True)
            assert started.empty() and not waiting.done()
            second.cancel()
            assert await started.get() == large
            release.set()
    assert a._option_research_reads == a._option_research_lines == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('requested,closed,expected_timeout', [(True, True, 1.25), (True, False, 3), (False, True, 8)])
async def test_closed_display_snapshot_releases_live_lines_without_shortening_history(
        monkeypatch, requested, closed, expected_timeout):
    from algo_trader_broker_sdk.options import BrokerOptionBinding, SnapshotRequest
    from algo_trader_broker_adapter_ibkr_paper import options_quote_references as references, options_quotes
    from algo_trader_broker_adapter_ibkr_paper.options_reads import IBOptionReads

    now = datetime.now(timezone.utc)
    at = now - timedelta(hours=1)
    observed = now.isoformat().replace('+00:00', 'Z')
    binding = BrokerOptionBinding('ibkr-paper', '1', 'paper', 'account', CID, '123', 'NATIVE',
        'SMART', observed, 'metadata')
    request = SnapshotRequest(protocol_version='options/1.0', execution_target='b' * 64, profile_id='profile',
        account='account', environment='paper', expected_generation=1, bindings=(binding,),
        feed='IBKR_LIVE', purpose='RESEARCH', max_age_ms=30000, max_leg_skew_ms=2000,
        prefer_closed_reference=requested)
    a = adapter()
    IBOptionReads._init_option_reads(a)
    a._qualification_timeout = 8
    a._option_bound_contracts = lambda request, fresh: [N(conId=123)]

    async def run(request, operation):
        return await operation(None, None)

    a._option_read = run
    monkeypatch.setattr(references, 'closed_bindings', lambda *args: {CID} if closed else set())

    async def live(ib, request, contracts, *, timeout):
        assert timeout == expected_timeout
        return OptionMarketSnapshot('test', (), (CID,), False, 0, observed)

    async def history(adapter, ib, contract, deadline, *, max_wait=None):
        assert deadline - asyncio.get_running_loop().time() > 7
        assert max_wait == (2 if requested and closed else None)
        return '1.9', '2', 1, 1, at

    monkeypatch.setattr(options_quotes, 'read_snapshot', live)
    monkeypatch.setattr(references, 'last_bbo', history)
    result = await IBOptionReads.option_snapshot(a, request)
    assert result.complete is closed
    if closed:
        assert result.quotes[0].quality == 'RESEARCH_ONLY'
        assert result.quotes[0].quote_at == at.isoformat().replace('+00:00', 'Z')
