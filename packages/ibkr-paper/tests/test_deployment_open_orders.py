import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient, IBConnectionError


@pytest.mark.parametrize("outcome", ["complete", "disconnect", "fail"])
@pytest.mark.parametrize("operation", ["deployment", "quantity-limit"])
def test_open_orders_require_a_completed_current_native_download(outcome, operation):
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
                future.set_result([SimpleNamespace(order=SimpleNamespace(whatIf=what_if, clientId=1, orderId=42),
                                                   contract=SimpleNamespace(symbol="MNQ", secType="FUT"))
                                   for what_if in (False, True)])
                if outcome == "disconnect":
                    client._connected.clear()
            return future

        ib.reqAllOpenOrdersAsync = Mock(side_effect=download)
        read = (client.count_open_orders_for_deployment if operation == "deployment"
                else client.request_open_orders_for_quantity_limit)
        if outcome == "complete":
            result = await read()
            if operation == "deployment":
                assert result == 1
            else:
                assert len(result) == 1
                assert result[0].message["order"]["clientId"] == 1
                assert result[0].message["contract"]["symbol"] == "MNQ"
        else:
            with pytest.raises(TimeoutError if outcome == "fail" else IBConnectionError):
                await read()
        ib.reqAllOpenOrdersAsync.assert_called_once()
        ib.wrapper._endReq.assert_called_once_with("openOrders")

    asyncio.run(scenario())
