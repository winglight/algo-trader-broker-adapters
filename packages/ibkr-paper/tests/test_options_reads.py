"""Original P0 read flow against native ib_async objects, without a gateway."""

import asyncio
from dataclasses import asdict, replace
from datetime import datetime, timezone, timedelta
from copy import deepcopy
import json
from itertools import count
from hashlib import sha256

import pytest
from ib_async import IB, ContractDetails, Option, OptionChain, Stock, Order, OrderState

from algo_trader_broker_adapter_ibkr_paper import IBKRPaperAdapter
from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient
from algo_trader_broker_adapter_ibkr_paper.settings import IBGatewaySettings
from algo_trader_broker_sdk import BrokerContractError, SubmissionGate
from algo_trader_broker_sdk.options import (
    OptionContractQuery, OptionScope, OptionVerifiedAccount, QualificationRequest,
    SnapshotRequest, option_contract_id, option_from_payload,
    OptionExecutionRequest, OptionLegIntent,
)
from algo_trader_broker_sdk import dataclass_to_payload


@pytest.mark.asyncio
async def test_standard_contract_discovery_qualification_and_live_snapshot(monkeypatch):
    await read_flow(monkeypatch, exercise_orders=True)


async def read_flow(monkeypatch, *, scope=None, consume_account=None, exercise_orders=False, gate_factory=None):
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
        if exercise_orders:
            await submission_flow(adapter, ib, scope, monkeypatch, gate_factory=gate_factory)
        client._connected_since = datetime.now(timezone.utc)
        with pytest.raises(BrokerContractError, match="connection changed"):
            await adapter.option_snapshot(request)
    finally:
        client._connected.clear()
        client._ib = None
        await client._shutdown_sync_executor()


async def submission_flow(adapter, ib, scope, monkeypatch, *, gate_factory=None):
    from algo_trader_broker_sdk.options_capabilities import OptionCapability, OptionShapeCapability
    from algo_trader_broker_sdk.submission_gate import order_fingerprint
    from algo_trader_broker_adapter_ibkr_paper.options_orders import build_order
    stored, sockets, evidence = [], [], []
    ib.client.connState = ib.client.CONNECTED
    ib.client._serverVersion = 178
    ib.client.clientId = ib.wrapper.clientId = 40

    async def sink(event):
        evidence.append(event)
    adapter.set_option_event_handler(OptionVerifiedAccount(scope, "IBKR", "DU-OPTIONS-FIXTURE"), sink)

    def socket_send(raw):
        assert len(stored) == len(sockets) + 1  # Durable callback preceded socket I/O.
        sockets.append(raw)

    def acknowledge(trade):
        native = deepcopy(trade.order)
        native.permId = 20000 + native.orderId
        asyncio.get_running_loop().call_soon(ib.wrapper.openOrder, native.orderId,
            deepcopy(trade.contract), native, OrderState(status="Submitted"))

    monkeypatch.setattr(ib.client.conn, "sendMsg", socket_send)
    ib.newOrderEvent += acknowledge
    try:
        bindings = tuple(adapter._option_catalog[key][0] for key in ("1590", "1595"))
        contracts = [adapter._option_catalog[key][2].contract for key in ("1590", "1595")]
        for combo in (False, True):
            shape = OptionShapeCapability("CALL_DEBIT_VERTICAL" if combo else "LONG_CALL", "NATIVE",
                OptionCapability("PAPER_CERTIFIED", ("EXPLICIT_FIXTURE_ONLY",), ("synthetic-ib-shape",)),
                2 if combo else 1, 1, "0.01", False, combo, False, False)
            request = OptionExecutionRequest(**asdict(scope), command_id="ib-bag" if combo else "ib-opt",
                client_order_id="ato-fixture-bag" if combo else "ato-fixture-opt", plan_hash="b" * 64,
                authorization_ref="fixture-close-permit" if combo else "fixture-reservation", quote_snapshot_ref="c" * 64,
                valid_until=(datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z"),
                capability_revision="fixture-ib", groups=2, order_type="LMT", signed_limit="-1.5" if combo else "1.25",
                tif="DAY", max_slippage="0", legs=(
                    (OptionLegIntent("L1", bindings[0], "SELL", "CLOSE", 1), OptionLegIntent("L2", bindings[1], "BUY", "CLOSE", 1))
                    if combo else (OptionLegIntent("L1", bindings[0], "BUY", "OPEN", 1),)))
            if gate_factory:
                request, extra_retain = await gate_factory(request, shape)
            else:
                extra_retain = None
            async def validate():
                pass  # Explicit authority fixture; no broker account is certified.
            async def retain(payload):
                if extra_retain is not None:
                    await extra_retain(payload)
                stored.append(payload)
            gate = SubmissionGate(validate, lambda: None, retain_native=retain, option_shape=shape)
            result = await adapter.submit_option_order_guarded(request, gate)
            assert gate.consumed and result.status == "ACKNOWLEDGED"
            assert result.parent_order_ref and all(leg.filled_contracts == 0 for leg in result.legs)
            payload = json.loads(stored[-1])
            assert payload["request_fingerprint"] == order_fingerprint(request)
            expected_contract, expected_order = build_order(request, contracts if combo else contracts[:1],
                "DU-OPTIONS-FIXTURE", 40, payload["order_id"], shape)
            from algo_trader_broker_adapter_ibkr_paper.options_account_native import raw_value
            assert payload["contract"] == raw_value(expected_contract) and payload["order"] == raw_value(expected_order)
            assert payload["order"]["lmtPrice"] == ("-1.5" if combo else "1.25")
            assert payload["order"]["action"] == "BUY" and payload["order"]["totalQuantity"] == 2
            assert payload["order"]["openClose"] == ("C" if combo else "O")
            if combo:
                assert [(leg["conId"], leg["ratio"], leg["action"], leg["openClose"])
                    for leg in payload["contract"]["comboLegs"]] == [(1590, 1, "SELL", 0), (1595, 1, "BUY", 0)]
            assert int.from_bytes(sockets[-1][:4], "big") == len(sockets[-1]) - 4
            assert sockets[-1][4:].split(b"\0")[0] == b"3"  # Actual TWS placeOrder wire message.
        assert len(sockets) == len(evidence) == 2
        assert all(event.source == "IB_OPTION_SUBMISSION" for event in evidence)
        async def resolve(native_id, symbol):
            from algo_trader_broker_sdk.options import QualificationResult
            binding, contract, _ = adapter._option_catalog[native_id]
            assert symbol == binding.local_symbol
            return QualificationResult(binding.canonical_id, "EXACT", binding, contract, ())
        for event in evidence:
            fact = await adapter.decode_option_event(event, resolve)
            assert fact.status == "ACKNOWLEDGED" and fact.session_key == "IBKR_PERM_ID"
            assert fact.executions == () and len(fact.legs) in (1, 2)
    finally:
        ib.newOrderEvent -= acknowledge
        ib.client.connState = ib.client.DISCONNECTED
