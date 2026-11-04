"""Exercise ib_async's actual lazy summary cache without a broker connection."""
import asyncio
from unittest.mock import AsyncMock

import pytest
from ib_async import IB
from ib_async.objects import AccountValue
from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient
from algo_trader_broker_adapter_ibkr_paper.settings import IBGatewaySettings


def native():
    ib = IB()
    ib.managedAccounts = lambda: ['fixture-a', 'fixture-b']
    return ib


def client_for(ib):
    client = IBAsyncClient(IBGatewaySettings(client_id=20))
    client.ensure_connected = AsyncMock(return_value=ib)
    return client


def value(account, amount):
    return AccountValue(account, 'NetLiquidation', str(amount), 'USD', '')


@pytest.mark.asyncio
async def test_concurrent_first_reads_and_cancelled_caller_share_one_native_subscription():
    ib, started, finish = native(), asyncio.Event(), asyncio.Event()
    async def load():
        started.set()
        await finish.wait()
        ib.wrapper.acctSummary.update(a=value('fixture-a', 10), b=value('fixture-b', 20))
    ib.reqAccountSummaryAsync = AsyncMock(side_effect=load)
    client = client_for(ib)
    cancelled = asyncio.create_task(client.get_account_summary('fixture-a'))
    await started.wait()
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    reads = [asyncio.create_task(client.get_account_summary('fixture-a' if n % 2 == 0 else 'fixture-b')) for n in range(100)]
    await asyncio.sleep(0)
    assert ib.reqAccountSummaryAsync.await_count == 1
    finish.set()
    results = await asyncio.gather(*reads)
    assert [rows[0].value for rows in results] == ['10' if n % 2 == 0 else '20' for n in range(100)]
    # Native subscription updates remain live; no copied response cache.
    ib.wrapper.acctSummary['a'] = value('fixture-a', 30)
    assert (await client.get_account_summary('fixture-a'))[0].value == '30'
    assert ib.reqAccountSummaryAsync.await_count == 1


@pytest.mark.asyncio
async def test_partial_native_cache_does_not_bypass_the_initial_completion_barrier():
    ib, finish, started = native(), asyncio.Event(), asyncio.Event()
    async def load():
        ib.wrapper.acctSummary['a'] = value('fixture-a', 10)
        started.set()
        await finish.wait()
        ib.wrapper.acctSummary['b'] = value('fixture-b', 20)
    ib.reqAccountSummaryAsync = AsyncMock(side_effect=load)
    client = client_for(ib)
    first = asyncio.create_task(client.get_account_summary('fixture-a'))
    await started.wait()
    second = asyncio.create_task(client.get_account_summary('fixture-b'))
    await asyncio.sleep(0)
    assert not second.done()
    finish.set()
    assert (await second)[0].value == '20'
    await first
    assert ib.reqAccountSummaryAsync.await_count == 1


@pytest.mark.asyncio
async def test_cleared_native_cache_cannot_replay_completed_summary_as_fresh_data():
    ib = native()
    async def load():
        ib.wrapper.acctSummary['a'] = value('fixture-a', 10)
    ib.reqAccountSummaryAsync = AsyncMock(side_effect=load)
    client = client_for(ib)
    assert (await client.get_account_summary('fixture-a'))[0].value == '10'
    ib.wrapper.acctSummary.clear()
    assert await client.get_account_summary('fixture-a') == []
    assert await client.get_account_summary('fixture-b') == []
    assert ib.reqAccountSummaryAsync.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('failed', [False, True])
async def test_empty_or_failed_load_does_not_repeatedly_subscribe_until_new_connection(failed):
    ib = native()
    ib.reqAccountSummaryAsync = AsyncMock(side_effect=RuntimeError('native failure') if failed else None)
    client = client_for(ib)
    for _ in range(3):
        if failed:
            with pytest.raises(RuntimeError, match='native failure'):
                await client.get_account_summary()
        else:
            assert await client.get_account_summary() == []
    assert ib.reqAccountSummaryAsync.await_count == 1
    replacement = native()
    async def load_new():
        replacement.wrapper.acctSummary['a'] = value('fixture-a', 40)
    replacement.reqAccountSummaryAsync = AsyncMock(side_effect=load_new)
    client.ensure_connected.return_value = replacement
    assert (await client.get_account_summary())[0].value == '40'
    replacement.reqAccountSummaryAsync.assert_awaited_once()
