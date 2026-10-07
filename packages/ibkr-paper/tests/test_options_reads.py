"""Original P0 read flow against native ib_async objects, without a gateway."""

import asyncio
from dataclasses import asdict, replace
from datetime import datetime, timezone
from itertools import count
from hashlib import sha256

import pytest
from ib_async import IB, ContractDetails, Option, OptionChain, Stock, Order, OrderState

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
    await read_flow(monkeypatch)


async def read_flow(monkeypatch, *, scope=None, consume_account=None):
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
        if contract.conId:
            strike = contract.conId - 1000
        else:
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
    scope = scope or OptionScope("options/1.0", "a" * 64, "fixture-profile", 1, "fixture-account", "paper")
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

        canceled_accounts, canceled_positions, retained = [], [], {}
        def account_values(req_id, account, model, ledger_only):
            assert (account, model, ledger_only) == ("DU-OPTIONS-FIXTURE", "", False)
            def receive():
                for tag, value, currency in (("Currency", "USD", "BASE"),
                        ("NetLiquidation", "10000.123456789012", "USD"), ("TotalCashValue", "9000", "USD"),
                        ("AvailableFunds", "8000", "USD"), ("BuyingPower", "32000", "USD"),
                        ("accountReady", "true", "")):
                    ib.client.decoder.interpret(["73", "1", str(req_id), account, "", tag, value, currency])
                ib.client.decoder.interpret(["74", "1", str(req_id)])
            asyncio.get_running_loop().call_soon(receive)

        def positions(req_id, account, model):
            assert (account, model) == ("DU-OPTIONS-FIXTURE", "")
            def receive():
                # Real decoder and wrapper callbacks, not a prebuilt Account DTO.
                for strike, qty, cost in ((590, "2", "240"), (595, "-2", "90")):
                    ib.client.decoder.interpret(["71", "1", str(req_id), account, str(1000 + strike),
                        "SPY", "OPT", "20261016", str(strike), "C", "100", "", "USD",
                        f"SPY   261016C{strike * 1000:08d}", "SPY", qty, cost, ""])
                ib.client.decoder.interpret(["72", "1", str(req_id)])
            asyncio.get_running_loop().call_soon(receive)

        def orders():
            def receive():
                ib.wrapper.openOrder(17, native, Order(orderId=17, clientId=40, permId=19001,
                    account="DU-OPTIONS-FIXTURE", totalQuantity=2, action="BUY", orderType="LMT", lmtPrice=1.25),
                    OrderState(status="Submitted"))
                ib.wrapper.openOrderEnd()
            asyncio.get_running_loop().call_soon(receive)

        async def retain(raw):
            digest = sha256(raw).hexdigest()
            retained[digest] = raw
            return digest

        monkeypatch.setattr(ib.client, "reqAccountUpdatesMulti", account_values)
        monkeypatch.setattr(ib.client, "cancelAccountUpdatesMulti", canceled_accounts.append)
        monkeypatch.setattr(ib.client, "reqPositionsMulti", positions)
        monkeypatch.setattr(ib.client, "cancelPositionsMulti", canceled_positions.append)
        monkeypatch.setattr(ib.client, "reqAllOpenOrders", orders)
        permissions = await adapter.option_account_permissions(scope)
        assert permissions.approval_status == "UNKNOWN" and permissions.permitted_structures == ()
        assert permissions.data_entitlement == "ALLOWED" and permissions.bp_semantics == "AVAILABLE_FUNDS"
        state = await adapter.option_account_state(scope, retain_lifecycle=retain)
        assert (state.equity_cash, state.cash_available, state.option_buying_power) == ("10000.123456789012", "9000", "8000")
        assert {v.name: v.value for v in state.raw_buying_power} == {"AvailableFunds": "8000", "BuyingPower": "32000"}
        assert [p.signed_contracts for p in state.positions] == [2, -2]
        assert [p.raw_cost_value for p in state.positions] == ["240", "90"]
        assert all(p.raw_cost_unit == "UNKNOWN" and p.raw_ref in retained for p in state.positions)
        assert state.positions_complete and not state.unresolved_positions
        assert not state.orders_complete and not state.executions_complete and not state.lifecycle_complete
        assert [(r.broker_order_session_key, r.broker_order_id) for r in state.open_order_refs] == [("IBKR_PERM_ID", "19001")]
        assert state.source_checkpoint in retained
        assert option_from_payload(type(state), dataclass_to_payload(state)) == state
        assert len(canceled_accounts) == 2 and len(canceled_positions) == 1
        assert not ib.wrapper._futures
        assert "positionMulti" not in ib.wrapper.__dict__ and "openOrder" not in ib.wrapper.__dict__
        if consume_account is not None:
            await consume_account(state)
        client._connected_since = datetime.now(timezone.utc)
        with pytest.raises(BrokerContractError, match="connection changed"):
            await adapter.option_snapshot(request)
    finally:
        client._connected.clear()
        client._ib = None
        await client._shutdown_sync_executor()
