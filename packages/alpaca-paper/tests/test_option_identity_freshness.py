from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as N
from unittest.mock import AsyncMock

import pytest
from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_adapter_alpaca_paper.options_reads import AlpacaOptionReads


@pytest.mark.asyncio
@pytest.mark.parametrize('age,backend_changed',[(2,False),(6,False),(-1,False),(2,True)])
async def test_only_same_native_identity_within_five_seconds_avoids_redundant_read(age,backend_changed):
    backend=object();bound=N(scope='exact-scope')
    original=datetime.now(timezone.utc)-timedelta(seconds=age)
    adapter=N(_backend=object() if backend_changed else backend,
        _option_identity_observation=(bound,backend,original),
        _option_still_bound=lambda value: None,_option_bound=AsyncMock())
    await AlpacaOptionReads._option_require_native_identity(adapter,bound)
    if 0<=age<=5 and not backend_changed:
        adapter._option_bound.assert_not_awaited()
        assert adapter._option_identity_observation[2]==original
    else:
        adapter._option_bound.assert_awaited_once_with('exact-scope',read_account=True)


@pytest.mark.asyncio
async def test_connection_change_rejects_even_cached_native_identity():
    def changed(bound):raise BrokerContractError('Connection changed')
    adapter=N(_option_still_bound=changed,_option_bound=AsyncMock())
    with pytest.raises(BrokerContractError):
        await AlpacaOptionReads._option_require_native_identity(adapter,object())
    adapter._option_bound.assert_not_awaited()


@pytest.mark.asyncio
async def test_financial_collections_run_concurrently_and_keep_oldest_native_clock(monkeypatch):
    import asyncio
    import json
    from hashlib import sha256
    from algo_trader_broker_adapter_alpaca_paper import AlpacaPaperAdapter
    from .test_option_raw_events import VERIFIED

    started = set()
    all_started = asyncio.Event()
    position_done, account_done = asyncio.Event(), asyncio.Event()
    account = dict(id=VERIFIED.native_account_ref, currency='USD', equity='1000',
        cash='1000', options_buying_power='1000')
    async def read(path, **kwargs):
        started.add(path)
        if len(started) == 3:
            all_started.set()
        await asyncio.wait_for(all_started.wait(), 1)
        if path == '/v2/positions':
            position_done.set()
            return b'[]'
        if path == '/v2/account':
            await position_done.wait()
            await asyncio.sleep(0)
            account_done.set()
            return json.dumps(account).encode()
        await account_done.wait()
        await asyncio.sleep(0)
        return b'[]'
    backend = N(get_option_resource=AsyncMock(side_effect=read))
    adapter = AlpacaPaperAdapter(dict(alpaca_api_key_id='fixture', alpaca_secret_key='fixture'), backend=backend)
    adapter._connected, adapter._account_id = True, VERIFIED.native_account_ref
    adapter.bind_option_account(VERIFIED)
    clocks = iter(['2026-10-21T00:00:01Z','2026-10-21T00:00:02Z',
        '2026-10-21T00:00:03Z','2026-10-21T00:00:04Z'])
    monkeypatch.setattr('algo_trader_broker_adapter_alpaca_paper.options_reads.now_wire', lambda: next(clocks))
    state, bound, raw = await adapter._option_account_snapshot(VERIFIED.scope,
        retain_evidence=AsyncMock(return_value=sha256(b'[]').hexdigest()))
    assert started == {'/v2/account','/v2/positions','/v2/orders'}
    assert state.observed_at == '2026-10-21T00:00:01Z'
    assert state.received_at == '2026-10-21T00:00:04Z'
    assert state.cash_available == '1000' and bound == VERIFIED and raw == b'[]'
