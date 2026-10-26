import asyncio
import httpx
import pytest

from algo_trader_broker_adapter_alpaca_paper.read_pacing import NativeReadPacing


def budget():
    now=[1000.0]
    waits=[]
    async def sleep(seconds):
        waits.append(seconds)
        now[0]+=seconds
        await asyncio.sleep(0)
    return NativeReadPacing(clock=lambda:now[0],wall_clock=lambda:now[0],sleep=sleep),now,waits


@pytest.mark.asyncio
async def test_actual_quota_is_shared_by_concurrent_native_readers():
    gate,now,waits=budget()
    starts=[]
    async def read():
        async with gate.slot():
            starts.append(now[0])
            gate.observed(httpx.Headers({'X-RateLimit-Remaining':str(3-len(starts)),
                'X-RateLimit-Reset':'1006'}),200)
    await asyncio.gather(read(),read(),read())
    assert starts == [1000,1003,1006]
    assert waits == [3,3]


@pytest.mark.asyncio
async def test_rejected_read_waits_until_actual_reset_before_next_independent_call():
    gate,now,waits=budget()
    calls=[]
    async with gate.slot():
        calls.append('original-rejected-read')
        gate.observed(httpx.Headers({'X-RateLimit-Remaining':'0','X-RateLimit-Reset':'1009','Retry-After':'4'}),429)
    assert calls == ['original-rejected-read'] and waits == []
    async with gate.slot():
        calls.append('new-read')
    assert waits == [9] and now[0] == 1009
    assert calls == ['original-rejected-read','new-read']


@pytest.mark.asyncio
async def test_missing_quota_headers_back_off_only_after_real_rejection():
    gate,now,waits=budget()
    async with gate.slot():
        gate.observed(httpx.Headers(),200)
    async with gate.slot():
        gate.observed(httpx.Headers(),429)
    async with gate.slot():
        gate.observed(httpx.Headers(),429)
    async with gate.slot():
        gate.observed(httpx.Headers(),200)
    assert waits == [1,2]


@pytest.mark.asyncio
async def test_slow_network_read_does_not_hold_quota_scheduling_lock():
    gate,now,waits=budget()
    entered=asyncio.Event()
    release=asyncio.Event()
    async def slow_read():
        async with gate.slot():
            entered.set()
            await release.wait()
    task=asyncio.create_task(slow_read())
    await entered.wait()
    try:
        async with asyncio.timeout(1):
            async with gate.slot():
                gate.observed(httpx.Headers({'X-RateLimit-Remaining':'2','X-RateLimit-Reset':'1006'}),200)
        async with gate.slot():
            assert now[0]==1003
        assert waits==[3]
        # A real rejection still governs the next start, even when another
        # request is waiting for its response outside the scheduling lock.
        gate.observed(httpx.Headers({'Retry-After':'9'}),429)
        async with gate.slot():
            assert now[0]==1012
    finally:
        release.set()
        await task
