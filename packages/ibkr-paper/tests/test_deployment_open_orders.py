import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient, IBConnectionError


@pytest.mark.parametrize("outcome", ["complete", "disconnect", "fail"])
def test_deployment_counts_only_a_completed_current_native_download(outcome):
    async def scenario():
        client = object.__new__(IBAsyncClient)
        client._open_orders_lock = asyncio.Lock()
        client._connected = asyncio.Event()
        client._connected.set()
        client._connected_since = object()
        ib = SimpleNamespace(wrapper=SimpleNamespace(_futures={}, _endReq=Mock()),
                             isConnected=lambda: client._connected.is_set())
        client._ib = ib

        def download():
            future = asyncio.get_running_loop().create_future()
            ib.wrapper._futures["openOrders"] = future
            if outcome == "fail":
                future.set_exception(TimeoutError("incomplete native response"))
            else:
                future.set_result([SimpleNamespace(order=SimpleNamespace(whatIf=what_if))
                                   for what_if in (False, True)])
                if outcome == "disconnect":
                    client._connected.clear()
            return future

        ib.reqAllOpenOrdersAsync = Mock(side_effect=download)
        if outcome == "complete":
            assert await client.count_open_orders_for_deployment() == 1
        else:
            with pytest.raises(TimeoutError if outcome == "fail" else IBConnectionError):
                await client.count_open_orders_for_deployment()
        ib.reqAllOpenOrdersAsync.assert_called_once()
        ib.wrapper._endReq.assert_called_once_with("openOrders")

    asyncio.run(scenario())
