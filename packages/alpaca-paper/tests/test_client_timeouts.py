from types import SimpleNamespace

import pytest

from algo_trader_broker_adapter_alpaca_paper.clients import AlpacaClients
from algo_trader_broker_sdk import BrokerConnectionError


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["get_account", "get_positions", "get_stock_bars"])
async def test_read_timeout_is_a_broker_connection_error(operation):
    client = AlpacaClients(SimpleNamespace(max_concurrency=1, request_timeout_seconds=1))

    def timed_out():
        raise TimeoutError("upstream timeout")

    with pytest.raises(BrokerConnectionError) as error:
        await client._call(operation, timed_out)
    assert error.value.details == {"operation": operation, "error_type": "TimeoutError"}


@pytest.mark.asyncio
async def test_submit_timeout_preserves_reconciliation_signal():
    client = AlpacaClients(SimpleNamespace(max_concurrency=1, request_timeout_seconds=1))

    def timed_out():
        raise TimeoutError("uncertain submission")

    with pytest.raises(TimeoutError):
        await client._call("submit_order", timed_out)
