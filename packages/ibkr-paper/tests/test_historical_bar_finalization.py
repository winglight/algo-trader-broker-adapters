import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from algo_trader_broker_adapter_ibkr_paper import client as module
from algo_trader_broker_adapter_ibkr_paper.settings import IBGatewaySettings


class Event:
    def __init__(self):
        self.handlers = []

    def __iadd__(self, callback):
        self.handlers.append(callback)
        return self

    def __isub__(self, callback):
        self.handlers.remove(callback)
        return self

    def emit(self, *args):
        for callback in list(self.handlers):
            callback(*args)


class Bars(list):
    def __init__(self, *rows):
        super().__init__(rows)
        self.updateEvent = Event()
        self.reqId = 123


@pytest.mark.asyncio
@pytest.mark.parametrize('minutes', [1, 5])
async def test_closed_stream_waits_for_boundary_and_forwards_final_trade_corrections(monkeypatch, minutes):
    start = datetime(2026, 10, 15, 14, tzinfo=timezone.utc)
    close = start + timedelta(minutes=minutes)

    class Clock:
        current = close - timedelta(milliseconds=500)

    is_closed = module._historical_bar_is_closed
    monkeypatch.setattr(module, '_historical_bar_is_closed',
                        lambda bar, size, **kwargs: is_closed(bar, size, now=Clock.current))
    bars = Bars()
    ib = SimpleNamespace(reqHistoricalDataAsync=AsyncMock(return_value=bars))
    client = module.IBAsyncClient(IBGatewaySettings(client_id=20))
    monkeypatch.setattr(client, 'ensure_connected', AsyncMock(return_value=ib))

    async def run(callback):
        return await callback(ib)

    async def timeout(callback, *args, **kwargs):
        return await callback(*args, **kwargs)

    monkeypatch.setattr(client, '_run_with_ib_async', run)
    monkeypatch.setattr(client, '_call_async_with_timeout', timeout)
    stream = await client.stream_historical_bars(
        object(), bar_size=f'{minutes} min' if minutes == 1 else f'{minutes} mins',
        keep_up_to_date=True, emit_history=False,
    )
    row = SimpleNamespace(date=start, open=1.0, high=2.0, low=0.5, close=1.5,
                          volume=10.0, wap=None, barCount=4)
    pending = asyncio.create_task(anext(stream))
    try:
        bars.append(row)
        bars.updateEvent.emit(bars, False)
        await asyncio.sleep(0.01)
        assert not pending.done(), 'A bar must not close half a second early'
        Clock.current = close + timedelta(milliseconds=100)
        row.volume = 20.0
        bars.updateEvent.emit(bars, False)
        assert (await asyncio.wait_for(pending, 0.5)).volume == 20.0

        pending = asyncio.create_task(anext(stream))
        bars.updateEvent.emit(bars, False)
        await asyncio.sleep(0.01)
        assert not pending.done(), 'Identical updates must remain deduplicated'
        row.volume = 21.0
        row.close = 1.75
        bars.updateEvent.emit(bars, False)
        corrected = await asyncio.wait_for(pending, 0.5)
        assert corrected.volume == 21.0
        assert corrected.close == 1.75
    finally:
        if not pending.done():
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        await stream.aclose()
    assert bars.updateEvent.handlers == []
