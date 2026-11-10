"""Qualification failures must not reach native market-data subscription."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient
from algo_trader_broker_adapter_ibkr_paper.exceptions import IBMarketDataError
from algo_trader_broker_adapter_ibkr_paper.settings import IBGatewaySettings
from ib_async import Future


def make_client(result):
    ib = SimpleNamespace(
        qualifyContractsAsync=AsyncMock(return_value=result),
        reqMktData=Mock(side_effect=AssertionError("invalid contract reached native subscription")),
    )
    client = IBAsyncClient(IBGatewaySettings(client_id=20))
    client.ensure_connected = AsyncMock(return_value=ib)

    async def run_sync(func):
        return func(ib)

    client._run_with_ib = run_sync
    return client, ib


@pytest.mark.asyncio
@pytest.mark.parametrize("result", [None, [], [None]])
async def test_failed_qualification_does_not_reach_native_price_subscription(result):
    client, ib = make_client(result)
    contract = Future(symbol="MNQ", exchange="CME")
    with pytest.raises(IBMarketDataError, match="Failed to qualify contract for real-time prices"):
        await client.stream_real_time_price(contract)
    ib.qualifyContractsAsync.assert_awaited_once_with(contract)
    ib.reqMktData.assert_not_called()


@pytest.mark.asyncio
async def test_direct_qualification_rejects_null_input_before_native_call():
    client, ib = make_client([None])
    with pytest.raises(IBMarketDataError, match="Contract must not be None"):
        await client.qualify_contract(None)
    ib.qualifyContractsAsync.assert_not_called()


@pytest.mark.asyncio
async def test_qualification_preserves_exact_native_contract():
    qualified = Future(conId=12345, symbol="MNQ", exchange="CME")
    client, ib = make_client([qualified])
    original = Future(symbol="MNQ", exchange="CME")
    assert await client.qualify_contract(original) is qualified
    ib.qualifyContractsAsync.assert_awaited_once_with(original)


@pytest.mark.asyncio
@pytest.mark.parametrize("ambiguous", [False, True])
async def test_native_ib_async_unresolved_contract_keeps_requested_identity(ambiguous, caplog):
    from ib_async import IB, ContractDetails

    native = IB()
    details = [
        ContractDetails(contract=Future(conId=12345, symbol="MNQ", exchange="CME")),
        ContractDetails(contract=Future(conId=12346, symbol="MNQ", exchange="CME")),
    ] if ambiguous else []
    native.reqContractDetailsAsync = AsyncMock(return_value=details)
    native.reqMktData = Mock(side_effect=AssertionError("unresolved contract was subscribed"))
    client, _ = make_client(None)
    client.ensure_connected = AsyncMock(return_value=native)
    contract = Future(symbol="MNQ", exchange="CME")
    with pytest.raises(IBMarketDataError, match="Failed to qualify contract for real-time prices"):
        await client.stream_real_time_price(contract)
    native.reqMktData.assert_not_called()
    records = [r for r in caplog.records if r.message == "Failed to request real-time price stream from IB"]
    assert records[-1].contract["symbol"] == "MNQ"
    assert isinstance(records[-1].exc_info[1], IBMarketDataError)
