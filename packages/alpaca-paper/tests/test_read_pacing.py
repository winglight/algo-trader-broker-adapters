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
