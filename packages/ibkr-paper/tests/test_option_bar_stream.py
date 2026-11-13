"""Native wrapper callbacks only: no socket, account reads, orders or timers polling bars."""
import asyncio
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from itertools import count
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from algo_trader_broker_adapter_ibkr_paper import IBKRPaperAdapter
from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient
from algo_trader_broker_adapter_ibkr_paper.exceptions import IBMarketDataError
from algo_trader_broker_adapter_ibkr_paper.options_bar_stream import native_bar_error
from algo_trader_broker_adapter_ibkr_paper.settings import IBGatewaySettings
from algo_trader_broker_sdk import BrokerConnectionError, BrokerContractError
from algo_trader_broker_sdk.errors import is_transport_failure
from algo_trader_broker_sdk.options import BrokerOptionBinding, OptionScope, OptionVerifiedAccount
from algo_trader_broker_sdk.options_bars import OptionBarSubscription
from ib_async import IB, BarData, ContractDetails, Option


@pytest.fixture
def native(monkeypatch):
    ib = IB()
    monkeypatch.setattr(ib, 'isConnected', lambda: True)
    monkeypatch.setattr(ib, 'managedAccounts', lambda: ['DU-FIXTURE'])
    ids, sent, canceled = count(100), [], []
    monkeypatch.setattr(ib.client, 'getReqId', lambda: next(ids))
    monkeypatch.setattr(ib.client, 'serverVersion', lambda: 178)
    monkeypatch.setattr(ib.client, 'cancelHistoricalData', canceled.append)
    now = datetime.now(UTC)
    at = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
    def row(close=2):
        return BarData(date=str(int(at.timestamp())), open=1, high=3, low=1, close=close, volume=8)
    def subscribe(*args):
        sent.append(args)
        assert args[2:] == ('', '1 D', '1 min', 'TRADES', False, 2, True, [])
        ib.wrapper.historicalData(args[0], row())
        ib.wrapper.historicalDataEnd(args[0], '', '')
    monkeypatch.setattr(ib.client, 'reqHistoricalData', subscribe)
    client = IBAsyncClient(IBGatewaySettings())
    client._ib, client._connected_since = ib, now
    client._connected.set()
    adapter = IBKRPaperAdapter({}, client=client)
    adapter._qualification_timeout = .05
    scope = OptionScope('options/1.0', 'a'*64, 'fixture-profile', 1, 'fixture-account', 'paper')
    bound = OptionVerifiedAccount(scope, 'IBKR', 'DU-FIXTURE')
    adapter.bind_option_account(bound)
    binding = BrokerOptionBinding('ibkr_paper', '0.4.0rc1', 'paper', scope.account,
        'opt_'+'b'*64, '123', 'FIXTURE', 'SMART', now.isoformat().replace("+00:00", "Z"), 'fixture-v1')
    contract = Option('SPY', '20990101', 500, 'C', 'SMART', conId=123)
    day = now.astimezone(ZoneInfo('America/New_York')).strftime('%Y%m%d')
    detail = ContractDetails(contract=contract, timeZoneId='US/Eastern',
        liquidHours=f'{day}:0930-{day}:1615', tradingHours=f'{day}:0930-{day}:1615')
    calls = []
    async def details(c):
        calls.append(c)
        return [detail]
    monkeypatch.setattr(ib, 'reqContractDetailsAsync', details)
    adapter._option_catalog['123'] = (binding, None, detail)
    request = OptionBarSubscription(**asdict(scope), bindings=(binding,), feed='provider_native',
        timeframe='1Min', owner_id='prepared:test', lease_seconds=30)
    return SimpleNamespace(ib=ib, adapter=adapter, request=request, sent=sent, canceled=canceled,
                           row=row, bound=bound, client=client, calls=calls)


@pytest.mark.asyncio
async def test_native_updates_shared_owners_and_final_cancel(native):
    a = native.adapter.stream_option_bars(native.request)
    b = native.adapter.stream_option_bars(replace(native.request, owner_id='chart:test'))
    first, second = await asyncio.gather(anext(a), anext(b))
    assert first.bar == second.bar and first.is_snapshot
    assert first.feed == 'provider_native' and first.bar.volume == 8
    assert len(native.sent) == len(native.calls) == 1
    pending_a, pending_b = asyncio.create_task(anext(a)), asyncio.create_task(anext(b))
    native.ib.wrapper.historicalDataUpdate(native.sent[0][0], native.row(2.5))
    x, y = await asyncio.gather(pending_a, pending_b)
    assert x == y and x.bar.close == '2.5' and not x.is_snapshot
    assert x.bar.time == first.bar.time  # Native correction, no synthetic minute.
    await a.aclose()
    assert native.canceled == []
    await b.aclose()
    assert native.canceled == [native.sent[0][0]]
    assert not native.ib.wrapper.reqId2Subscriber and not native.ib.wrapper._futures
    assert not native.adapter._option_bar_registry


@pytest.mark.asyncio
async def test_failure_is_terminal_not_resubscribed(native):
    a = native.adapter.stream_option_bars(native.request)
    await anext(a)
    native.ib.errorEvent.emit(native.sent[0][0], 354, 'synthetic denied', None)
    with pytest.raises(IBMarketDataError):
        await anext(a)
    assert len(native.sent) == len(native.canceled) == 1


@pytest.mark.asyncio
async def test_timeout_cancels_and_releases(native, monkeypatch):
    monkeypatch.setattr(native.ib.client, 'reqHistoricalData', lambda *args: native.sent.append(args))
    a = native.adapter.stream_option_bars(native.request)
    with pytest.raises(BrokerConnectionError, match="timed out"):
        await anext(a)
    assert len(native.sent) == len(native.canceled) == 1
    assert not native.ib.wrapper.reqId2Subscriber and not native.ib.wrapper._futures


@pytest.mark.asyncio
async def test_binding_change_closes_quiet_stream(native):
    a = native.adapter.stream_option_bars(native.request)
    await anext(a)
    scope = replace(native.bound.scope, expected_generation=2)
    native.adapter.bind_option_account(replace(native.bound, scope=scope))
    with pytest.raises(BrokerConnectionError):
        await anext(a)
    assert len(native.canceled) == 1


@pytest.mark.asyncio
async def test_catalog_change_rejected_before_emission(native):
    a = native.adapter.stream_option_bars(native.request)
    await anext(a)
    binding, contract, detail = native.adapter._option_catalog['123']
    native.adapter._option_catalog['123'] = (replace(binding, metadata_version='changed'), contract, detail)
    native.ib.wrapper.historicalDataUpdate(native.sent[0][0], native.row(2.5))
    with pytest.raises(BrokerContractError):
        await anext(a)
    assert len(native.canceled) == 1


@pytest.mark.asyncio
async def test_disconnect_releases_without_new_ticks(native):
    a = native.adapter.stream_option_bars(native.request)
    await anext(a)
    native.ib.disconnectedEvent.emit()
    with pytest.raises(BrokerConnectionError):
        await anext(a)
    assert len(native.canceled) == 1


@pytest.mark.asyncio
async def test_bad_feed_never_requests_history(native):
    a = native.adapter.stream_option_bars(replace(native.request, feed='IBKR_LIVE'))
    with pytest.raises(BrokerContractError):
        await anext(a)
    assert not native.sent


@pytest.mark.asyncio
async def test_cancellation_during_initial_history(native, monkeypatch):
    monkeypatch.setattr(native.ib.client, 'reqHistoricalData', lambda *args: native.sent.append(args))
    a = native.adapter.stream_option_bars(native.request)
    task = asyncio.create_task(anext(a))
    for _ in range(10):
        await asyncio.sleep(0)
        if native.sent:
            break
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(native.canceled) == 1
    assert not native.adapter._option_bar_registry


@pytest.mark.asyncio
async def test_connection_nonce_change_blocks_queued_update(native):
    a = native.adapter.stream_option_bars(native.request)
    await anext(a)
    native.ib.wrapper.historicalDataUpdate(native.sent[0][0], native.row(2.5))
    await asyncio.sleep(0)
    native.client._connected_since += timedelta(seconds=1)
    with pytest.raises(BrokerContractError):
        await anext(a)
    assert len(native.canceled) == 1


@pytest.mark.asyncio
async def test_reject_scope_and_binding_before_native_request(native):
    for request in (replace(native.request, expected_generation=2),
                    replace(native.request, bindings=(replace(native.request.bindings[0], metadata_version='other'),))):
        with pytest.raises(BrokerContractError):
            await anext(native.adapter.stream_option_bars(request))
    assert not native.sent


@pytest.mark.asyncio
async def test_queue_overflow_is_terminal_and_bounded(native):
    a = native.adapter.stream_option_bars(native.request)
    await anext(a)
    for i in range(300):
        native.ib.wrapper.historicalDataUpdate(native.sent[0][0], native.row(2 + i / 1000))
    await asyncio.sleep(0)
    with pytest.raises(BrokerConnectionError, match='bound'):
        await anext(a)
    assert len(native.canceled) == 1


@pytest.mark.asyncio
async def test_cancel_failure_surfaces(native, monkeypatch):
    a = native.adapter.stream_option_bars(native.request)
    await anext(a)
    def fail(req_id):
        raise RuntimeError('fixture cancel failed')
    monkeypatch.setattr(native.ib.client, 'cancelHistoricalData', fail)
    with pytest.raises(BrokerConnectionError, match='cancellation failed'):
        await a.aclose()
    assert not native.ib.wrapper.reqId2Subscriber


@pytest.mark.asyncio
async def test_old_server_capability_is_explicit(native, monkeypatch):
    monkeypatch.setattr(native.ib.client, 'serverVersion', lambda: 123)
    cap = await native.adapter.option_bar_stream_capability(native.bound.scope)
    assert cap.status == 'UNSUPPORTED' and not cap.feeds
    assert not native.sent


@pytest.mark.asyncio
async def test_empty_bootstrap_fails_closed(native, monkeypatch):
    def empty(*args):
        native.sent.append(args)
        native.ib.wrapper.historicalDataEnd(args[0], '', '')
    monkeypatch.setattr(native.ib.client, 'reqHistoricalData', empty)
    with pytest.raises(IBMarketDataError, match='no bars'):
        await anext(native.adapter.stream_option_bars(native.request))
    assert len(native.canceled) == 1


@pytest.mark.asyncio
async def test_fresh_qualification_shares_native_request_but_preserves_owner_binding(native):
    a = native.adapter.stream_option_bars(native.request)
    first = await anext(a)
    refreshed = replace(native.request.bindings[0], qualified_at=(datetime.now(UTC) + timedelta(seconds=1)).isoformat().replace('+00:00', 'Z'))
    _, contract, detail = native.adapter._option_catalog['123']
    native.adapter._option_catalog['123'] = (refreshed, contract, detail)
    newer = replace(native.request, bindings=(refreshed,), owner_id='chart:new-qualification')
    b = native.adapter.stream_option_bars(newer)
    second = await anext(b)
    assert first.binding == native.request.bindings[0] and second.binding == refreshed
    assert len(native.sent) == 1
    native.ib.wrapper.historicalDataUpdate(native.sent[0][0], native.row(2.5))
    x, y = await asyncio.gather(anext(a), anext(b))
    assert x.binding == first.binding and y.binding == refreshed
    await a.aclose()
    assert not native.canceled
    await b.aclose()
    assert len(native.canceled) == 1


def multiple_history(native, monkeypatch, count=3, after_end=None):
    last = int(native.row().date)
    def subscribe(*args):
        native.sent.append(args)
        for index in range(count):
            bar = native.row()
            bar.date = str(last - (count - 1 - index) * 60)
            bar.volume = index + 1
            native.ib.wrapper.historicalData(args[0], bar)
        native.ib.wrapper.historicalDataEnd(args[0], '', '')
        if after_end:
            after_end(args[0])
    monkeypatch.setattr(native.ib.client, 'reqHistoricalData', subscribe)


@pytest.mark.asyncio
async def test_initial_history_replayed_without_extra_requests(native, monkeypatch):
    multiple_history(native, monkeypatch)
    a = native.adapter.stream_option_bars(native.request)
    rows = [await anext(a) for _ in range(3)]
    assert [r.bar.volume for r in rows] == [1, 2, 3]
    assert all(r.is_snapshot for r in rows)
    assert [r.bar.time for r in rows] == sorted(r.bar.time for r in rows)
    assert len(native.sent) == 1
    await a.aclose()


@pytest.mark.asyncio
async def test_late_owner_replays_latest_native_versions_and_original_receipts(native, monkeypatch):
    multiple_history(native, monkeypatch)
    a = native.adapter.stream_option_bars(native.request)
    old = [await anext(a) for _ in range(3)]
    native.ib.wrapper.historicalDataUpdate(native.sent[0][0], native.row(2.5))
    corrected = await anext(a)
    b = native.adapter.stream_option_bars(replace(native.request, owner_id='late-chart'))
    replay = [await anext(b) for _ in range(3)]
    assert replay[:2] == old[:2]
    assert replay[-1] == replace(corrected, is_snapshot=True)
    assert replay[-1].received_at == corrected.received_at
    assert replay[-1].bar.close == '2.5' and all(r.is_snapshot for r in replay)
    assert len(native.sent) == 1
    await a.aclose()
    await b.aclose()


@pytest.mark.asyncio
async def test_update_during_calendar_wait_follows_all_initial_snapshot_bars(native, monkeypatch):
    multiple_history(native, monkeypatch)
    original = native.ib.reqContractDetailsAsync
    async def details(contract):
        native.ib.wrapper.historicalDataUpdate(native.sent[0][0], native.row(2.5))
        await asyncio.sleep(0)
        return await original(contract)
    monkeypatch.setattr(native.ib, 'reqContractDetailsAsync', details)
    a = native.adapter.stream_option_bars(native.request)
    rows = [await anext(a) for _ in range(3)]
    assert all(r.is_snapshot for r in rows)
    assert rows[-1].bar.close == '2.5'  # Concurrent correction captured in snapshot.
    assert len(native.sent) == 1
    await a.aclose()


@pytest.mark.asyncio
async def test_immediate_update_after_history_end_does_not_prune_bootstrap(native, monkeypatch):
    multiple_history(native, monkeypatch, after_end=lambda req_id:
        native.ib.wrapper.historicalDataUpdate(req_id, native.row(2.5)))
    a = native.adapter.stream_option_bars(native.request)
    rows = [await anext(a) for _ in range(3)]
    assert all(r.is_snapshot for r in rows)
    assert [r.bar.volume for r in rows[:2]] == [1, 2]
    assert rows[2].bar.close == '2.5'
    await a.aclose()


@pytest.mark.asyncio
async def test_snapshot_cache_bounded(native, monkeypatch):
    from algo_trader_broker_adapter_ibkr_paper import options_bar_stream
    monkeypatch.setattr(options_bar_stream, 'SNAPSHOT_LIMIT', 2)
    multiple_history(native, monkeypatch)
    a = native.adapter.stream_option_bars(native.request)
    rows = [await anext(a) for _ in range(2)]
    assert [r.bar.volume for r in rows] == [2, 3]
    entry = next(iter(native.adapter._option_bar_registry.values()))
    assert len(entry.snapshots) == 2
    b = native.adapter.stream_option_bars(replace(native.request, owner_id='late-chart'))
    assert [await anext(b) for _ in range(2)] == rows
    assert len(native.sent) == 1
    await a.aclose()
    await b.aclose()


@pytest.mark.asyncio
async def test_late_replay_cannot_follow_and_overwrite_concurrent_revision(native, monkeypatch):
    multiple_history(native, monkeypatch)
    a = native.adapter.stream_option_bars(native.request)
    initial = [await anext(a) for _ in range(3)]
    b = native.adapter.stream_option_bars(replace(native.request, owner_id='late-chart'))
    first = await anext(b)
    native.ib.wrapper.historicalDataUpdate(native.sent[0][0], native.row(2.5))
    replay = [first] + [await anext(b) for _ in range(3)]
    assert replay[:3] == initial
    assert not replay[-1].is_snapshot and replay[-1].bar.close == '2.5'
    assert replay[-1].received_at >= replay[-2].received_at
    assert len(native.sent) == 1
    await a.aclose()
    await b.aclose()


@pytest.mark.asyncio
async def test_slow_owner_overflow_does_not_terminate_healthy_owner(native):
    slow = native.adapter.stream_option_bars(native.request)
    healthy = native.adapter.stream_option_bars(replace(native.request, owner_id='healthy'))
    await anext(slow)
    await anext(healthy)
    for index in range(258):
        native.ib.wrapper.historicalDataUpdate(native.sent[0][0], native.row(round(2 + index / 1000, 3)))
        # The first value equals bootstrap and wrapper deduplicates it.
        if index:
            update = await anext(healthy)
            assert not update.is_snapshot
    assert not native.canceled
    assert len(next(iter(native.adapter._option_bar_registry.values())).owners) == 1
    with pytest.raises(BrokerConnectionError, match='consumer exceeded'):
        await anext(slow)
    assert not native.canceled
    native.ib.wrapper.historicalDataUpdate(native.sent[0][0], native.row(2.9))
    assert (await anext(healthy)).bar.close == '2.9'
    await healthy.aclose()
    assert len(native.canceled) == 1


@pytest.mark.asyncio
async def test_owner_and_binding_limits_before_native_requests(native, monkeypatch):
    from algo_trader_broker_adapter_ibkr_paper import options_bar_stream
    monkeypatch.setattr(options_bar_stream, 'MAX_BINDINGS', 0)
    with pytest.raises(BrokerContractError, match='32 bindings'):
        await anext(native.adapter.stream_option_bars(native.request))
    monkeypatch.setattr(options_bar_stream, 'MAX_BINDINGS', 32)
    monkeypatch.setattr(options_bar_stream, 'MAX_OWNERS', 0)
    with pytest.raises(BrokerContractError, match='owner limit'):
        await anext(native.adapter.stream_option_bars(native.request))
    assert not native.sent


@pytest.mark.asyncio
async def test_390_bar_snapshot_uses_shared_view_not_live_queue(native, monkeypatch):
    multiple_history(native, monkeypatch, count=390)
    a = native.adapter.stream_option_bars(native.request)
    first = await anext(a)
    entry = next(iter(native.adapter._option_bar_registry.values()))
    view = entry.snapshot_view
    assert len(view) == 390
    assert all(queue.maxsize == 256 and queue.empty() for queue, _, _ in entry.owners.values())
    b = native.adapter.stream_option_bars(replace(native.request, owner_id='second-snapshot'))
    assert await anext(b) == first
    assert entry.snapshot_view is view
    assert all(queue.empty() for queue, _, _ in entry.owners.values())
    assert len([await anext(a) for _ in range(389)]) == 389
    assert len([await anext(b) for _ in range(389)]) == 389
    await a.aclose()
    await b.aclose()


@pytest.mark.asyncio
async def test_global_native_stream_limit_releases_reservation(native, monkeypatch):
    from algo_trader_broker_adapter_ibkr_paper import options_bar_stream
    monkeypatch.setattr(options_bar_stream, 'MAX_NATIVE_STREAMS', 0)
    with pytest.raises(BrokerContractError, match='stream limit'):
        await anext(native.adapter.stream_option_bars(native.request))
    assert not native.sent and not native.adapter._option_bar_owner_tokens


@pytest.mark.parametrize('code,message,reason,reconnect', [
    (162, 'HMDS query returned no data: private-contract', 'NO_DATA', False),
    (162, 'Historical Market Data Service error:No market data permissions for private',
     'MARKET_DATA_PERMISSION', False),
    (162, 'Historical data request pacing violation private', 'PACING_VIOLATION', False),
    (162, 'unexpected private-account response', 'BROKER_REJECTION', False),
    (354, 'private subscription', 'MARKET_DATA_PERMISSION', False),
    (200, 'private contract', 'CONTRACT_UNAVAILABLE', False),
    (10197, 'private session', 'COMPETING_SESSION', False),
    (366, 'private reset', 'TRANSPORT_RESET', True),
    (10182, 'private reset', 'TRANSPORT_RESET', True),
])
def test_native_bar_failure_is_safe_and_not_a_fake_connection_error(code, message, reason, reconnect):
    error = native_bar_error(123, code, message)
    assert error.code == f'OPTION_BAR_{reason}'
    assert error.details == {'nativeCode': code, 'requestId': 123, 'reason': reason,
                             'recovery': 'RECONNECT_STREAM' if reconnect else 'WAIT_SOURCE'}
    assert is_transport_failure(error) is reconnect
    assert 'private' not in str(error.to_payload())


@pytest.mark.asyncio
@pytest.mark.parametrize('raise_request_errors', [False, True])
@pytest.mark.parametrize('bootstrap', [False, True])
async def test_162_cleans_native_request_and_fans_out_terminal_error(native, monkeypatch,
                                                                   bootstrap, raise_request_errors):
    native.ib.RaiseRequestErrors = raise_request_errors
    if bootstrap:
        def denied(*args):
            native.sent.append(args)
            native.ib.wrapper.error(args[0], 162, 'HMDS query returned no data: private-contract', '')
        monkeypatch.setattr(native.ib.client, 'reqHistoricalData', denied)
    a = native.adapter.stream_option_bars(native.request)
    b = native.adapter.stream_option_bars(replace(native.request, owner_id='chart:test'))
    if not bootstrap:
        await asyncio.gather(anext(a), anext(b))
        native.ib.wrapper.error(native.sent[0][0], 162, 'HMDS query returned no data: private-contract', '')
    errors = await asyncio.gather(anext(a), anext(b), return_exceptions=True)
    for error in errors:
        assert isinstance(error, IBMarketDataError)
        assert error.code == 'OPTION_BAR_NO_DATA'
        assert not is_transport_failure(error)
        assert 'private' not in str(error.to_payload())
    assert len(native.sent) == len(native.canceled) == 1
    assert not native.adapter._option_bar_registry
    assert not native.adapter._option_bar_owner_tokens
    assert not native.ib.wrapper.reqId2Subscriber
    assert not native.ib.wrapper._futures


@pytest.mark.asyncio
async def test_other_requests_errors_do_not_kill_native_bars(native):
    stream = native.adapter.stream_option_bars(native.request)
    await anext(stream)
    native.ib.errorEvent.emit(native.sent[0][0] + 500, 162, 'HMDS query returned no data: other')
    native.ib.wrapper.historicalDataUpdate(native.sent[0][0], native.row(2.5))
    assert (await anext(stream)).bar.close == '2.5'
    await stream.aclose()
