from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from algo_trader_broker_adapter_ibkr_paper.settings import IBGatewaySettings
from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient


class Event:
    def __init__(self):
        self.handlers = []

    def __iadd__(self, handler):
        self.handlers.append(handler)
        return self

    def __isub__(self, handler):
        self.handlers.remove(handler)
        return self

    def emit(self, *args):
        for handler in list(self.handlers):
            handler(*args)


@pytest.mark.asyncio
@pytest.mark.parametrize('has_quote', [False, True])
async def test_generic_snapshot_uses_bounded_stream_and_cleans_up_on_timeout(has_quote):
    contract = SimpleNamespace(conId=101, symbol='MNQ', secType='FUT')
    ticker = SimpleNamespace(reqId=7, contract=contract, last=3.0 if has_quote else None)
    requests, cancelled = [], []

    def req_data(*args):
        requests.append(args)
        return ticker

    ib = SimpleNamespace(errorEvent=Event(), isConnected=lambda: True,
                         reqMktData=req_data, cancelMktData=lambda value: cancelled.append(value))
    client = IBAsyncClient(IBGatewaySettings(client_id=20))
    client._ib = ib
    client._connected.set()
    client._loop = asyncio.get_running_loop()
    request = client.request_market_snapshot(contract, generic_tick_list='233,258', snapshot_timeout=5)
    if has_quote:
        await asyncio.wait_for(request, 0.5)
    else:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(request, 0.05)
    assert requests[0][1:] == ('233,258', False, False)
    assert len(cancelled) == 1
    assert ib.errorEvent.handlers == []
