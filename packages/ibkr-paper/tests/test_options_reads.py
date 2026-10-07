"""Original P0 read flow against native ib_async objects, without a gateway."""

import asyncio
from dataclasses import asdict, replace
from datetime import datetime, timezone
from itertools import count

import pytest
from ib_async import IB, ContractDetails, Option, OptionChain, Stock

from algo_trader_broker_adapter_ibkr_paper import IBKRPaperAdapter
from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient
from algo_trader_broker_adapter_ibkr_paper.settings import IBGatewaySettings
from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import (
    OptionContractQuery, OptionScope, OptionVerifiedAccount, QualificationRequest,
    SnapshotRequest, option_contract_id, option_from_payload,
)
from algo_trader_broker_sdk import dataclass_to_payload


@pytest.mark.asyncio
async def test_standard_contract_discovery_qualification_and_live_snapshot(monkeypatch):
    ib = IB()
    monkeypatch.setattr(ib, "isConnected", lambda: True)
    monkeypatch.setattr(ib, "managedAccounts", lambda: ["DU-OPTIONS-FIXTURE"])
    request_ids, canceled, queries = count(1), [], []
    monkeypatch.setattr(ib.client, "getReqId", lambda: next(request_ids))
    monkeypatch.setattr(ib.client, "cancelMktData", canceled.append)

    async def contract_details(contract):
        queries.append(contract)
        if contract.secType == "STK":
            return [ContractDetails(contract=Stock("SPY", "SMART", "USD", conId=100))]
        assert (contract.symbol, contract.exchange, contract.tradingClass, contract.multiplier) == ("SPY", "SMART", "SPY", "100")
        strike = int(contract.strike)
        native = Option("SPY", "20261016", strike, "C", "SMART", multiplier="100", currency="USD",
                        tradingClass="SPY", conId=1000 + strike, localSymbol=f"SPY   261016C{strike * 1000:08d}")
        return [ContractDetails(contract=native, underConId=100, underSymbol="SPY", underSecType="STK",
                                validExchanges="SMART,CBOE", minTick=0.01, marketRuleIds="26,26")]

    async def parameters(*args):
        assert args == ("SPY", "", "STK", 100)
        return [OptionChain("SMART", 100, "SPY", "100", {"20261016"}, {590.0, 595.0})]

    def subscribe(req_id, contract, generic, snapshot, regulatory, options):
        assert generic == "" and snapshot is False and regulatory is False

        def receive():
            ib.wrapper.lastTime = datetime.now(timezone.utc)
            ib.wrapper.marketDataType(req_id, 1)
            ib.wrapper.priceSizeTick(req_id, 1, 1.25, 5)
            ib.wrapper.priceSizeTick(req_id, 2, 1.30, 7)
            ib.wrapper.tcpDataProcessed()
        asyncio.get_running_loop().call_soon(receive)

    monkeypatch.setattr(ib, "reqContractDetailsAsync", contract_details)
    monkeypatch.setattr(ib, "reqSecDefOptParamsAsync", parameters)
    monkeypatch.setattr(ib.client, "reqMktData", subscribe)
    client = IBAsyncClient(IBGatewaySettings())
    client._ib = ib
    client._connected.set()
    client._connected_since = datetime.now(timezone.utc)
    adapter = IBKRPaperAdapter({}, client=client)
    scope = OptionScope("options/1.0", "a" * 64, "fixture-profile", 1, "fixture-account", "paper")
    values = asdict(scope)
    adapter.bind_option_account(OptionVerifiedAccount(scope, "IBKR", "DU-OPTIONS-FIXTURE"))
    try:
        capabilities = await adapter.option_capabilities(scope)
        assert capabilities.discovery.status == capabilities.quotes.status == "IMPLEMENTED"
        assert capabilities.shapes == () and capabilities.greeks.status == "UNSUPPORTED"
        query = OptionContractQuery(**values, underlying="SPY", expiry_from="2026-10-16", expiry_to="2026-10-16",
            right="C", strike_min="590", strike_max="595", cursor=None, limit=1)
        first = await adapter.list_option_contracts(query)
        assert not first.complete and first.next_cursor
        second = await adapter.list_option_contracts(replace(query, cursor=first.next_cursor))
        assert second.complete and second.next_cursor is None
        contracts = first.contracts + second.contracts
        assert len(contracts) == 2
        assert [c.key.strike for c in contracts] == ["590", "595"]
        assert all(c.canonical_id == option_contract_id(c.key) for c in contracts)
        qualified = await adapter.qualify_option_contracts(QualificationRequest(**values, contracts=contracts))
        assert [r.status for r in qualified.results] == ["EXACT", "EXACT"]
        bindings = tuple(r.binding for r in qualified.results)
        assert [b.broker_contract_id for b in bindings] == ["1590", "1595"]
        # Another consumer's ticker for the same conId survives this finite read.
        native = adapter._option_catalog["1590"][2].contract
        existing = ib.wrapper.startTicker(900, native, "mktData")
        request = SnapshotRequest(**values, bindings=bindings, feed="IBKR_LIVE", purpose="RESEARCH",
                                  max_age_ms=5000, max_leg_skew_ms=1000)
        snapshot = await adapter.option_snapshot(request)
        assert snapshot.complete and not snapshot.missing_contract_ids
        assert all((q.bid, q.ask, q.bid_size, q.ask_size, q.quality) == ("1.25", "1.3", 5, 7, "EXECUTABLE")
                   for q in snapshot.quotes)
        assert all(q.provider == "IBKR" and q.greeks_as_of is None and q.delta is None for q in snapshot.quotes)
        assert option_from_payload(type(snapshot), dataclass_to_payload(snapshot)) == snapshot
        assert canceled == [1, 2] and ib.wrapper.reqId2Ticker == {900: existing}
        assert ib.wrapper.ticker2ReqId["mktData"][existing] == 900
        client._connected_since = datetime.now(timezone.utc)
        with pytest.raises(BrokerContractError, match="connection changed"):
            await adapter.option_snapshot(request)
    finally:
        client._connected.clear()
        client._ib = None
        await client._shutdown_sync_executor()
