"""Original P0 read flow against native ib_async objects, without a gateway."""

import asyncio
from dataclasses import asdict, replace
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from copy import deepcopy
import json
from itertools import count
from hashlib import sha256

import pytest
from ib_async import IB, ContractDetails, Option, OptionChain, Stock, Order, OrderState

from algo_trader_broker_adapter_ibkr_paper import IBKRPaperAdapter
from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient
from algo_trader_broker_adapter_ibkr_paper.settings import IBGatewaySettings
from algo_trader_broker_sdk import BrokerCapabilityError, BrokerContractError, SubmissionGate
from algo_trader_broker_sdk.options_capabilities import require_options_extension
from algo_trader_broker_sdk.options import (
    OptionContractQuery, OptionScope, OptionVerifiedAccount, QualificationRequest,
    SnapshotRequest, option_contract_id, option_from_payload,
    OptionExecutionRequest, OptionLegIntent,
)
from algo_trader_broker_sdk import dataclass_to_payload


@pytest.mark.asyncio
async def test_standard_contract_discovery_qualification_and_live_snapshot(monkeypatch):
    await read_flow(monkeypatch, exercise_orders=True)


async def read_flow(monkeypatch, *, scope=None, consume_account=None, consume_snapshot=None, exercise_orders=False, gate_factory=None, consume_event=None, cancel_sender=None, reconcile_reader=None, preview_reader=None):
    ib = IB()
    monkeypatch.setattr(ib, "isConnected", lambda: True)
    monkeypatch.setattr(ib, "managedAccounts", lambda: ["DU-OPTIONS-FIXTURE"])
    request_ids, canceled, queries = count(1000), [], []
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
                                validExchanges="SMART,CBOE", minTick=0.01, marketRuleIds="26,26",
                                timeZoneId="US/Eastern", tradingHours="20261008:0930-20261008:1615",
                                liquidHours="20261008:0930-20261008:1615;20261010:CLOSED")]

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
            ib.wrapper.tickOptionComputation(req_id, 13, 0, .25, .55, 1.275, .01, .02, .12, -.03, 590.)
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
    assert require_options_extension(adapter) is adapter
    scope = scope or OptionScope("options/1.0", "a" * 64, "fixture-profile", 1, "fixture-account", "paper")
    values = asdict(scope)
    adapter.bind_option_account(OptionVerifiedAccount(scope, "IBKR", "DU-OPTIONS-FIXTURE"))
    try:
        capabilities = await adapter.option_capabilities(scope)
        assert capabilities.discovery.status == capabilities.quotes.status == "IMPLEMENTED"
        assert capabilities.shapes == () and capabilities.greeks.status == "IMPLEMENTED"
        with pytest.raises(BrokerCapabilityError, match="CAPABILITY_NOT_CERTIFIED"):
            capabilities.require_entry(scope=scope, structure="LONG_CALL", route="NATIVE",
                feed="IBKR_LIVE", now=datetime.now(timezone.utc))
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
        assert all(q.native_greeks.inputs_asof is None and q.native_greeks.units == "UNKNOWN"
                   and q.native_greeks.model_version is None for q in snapshot.quotes)
        assert [json.loads(q.native_greeks.raw_json)["values"]["delta"] for q in snapshot.quotes] == ["0.55", "0.55"]
        assert "tickOptionComputation" not in ib.wrapper.__dict__
        assert option_from_payload(type(snapshot), dataclass_to_payload(snapshot)) == snapshot
        if consume_snapshot is not None:
            await consume_snapshot(adapter, scope, contracts, bindings, snapshot)
        assert canceled == [1000, 1001] and ib.wrapper.reqId2Ticker == {900: existing}
        assert ib.wrapper.ticker2ReqId["mktData"][existing] == 900

        await market_data_flow(adapter, ib, request, monkeypatch)

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
                    contract = adapter._option_catalog[str(1000 + strike)][2].contract
                    # Same native account's existing portfolio callback gives
                    # an independent total-cost sample; it is not a new quote.
                    ib.wrapper.updatePortfolio(contract, Decimal(qty), 2.5 if strike == 590 else 1.0,
                        500.0 if strike == 590 else -200.0, float(cost),
                        20.0 if strike == 590 else -20.0, 0.0, account)
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
        # Native cash evidence shares the existing account walkthrough. Keep
        # both IB correction versions in the download; only .02 is current.
        cash_contract = Stock("SPY", "SMART", "USD", conId=100)
        cash_order = Order(permId=20980, orderRef="fixture-cash-entry", account="DU-OPTIONS-FIXTURE",
            action="BUY", orderType="LMT", totalQuantity=1, lmtPrice=20, tif="DAY", filledQuantity=1)
        def cash_completed(api_only):
            assert api_only
            ib.wrapper.completedOrder(cash_contract, cash_order, OrderState(status="Filled"))
            ib.wrapper.completedOrdersEnd()
        def cash_executions(req_id, query):
            from ib_async import Execution
            assert query.acctCode == cash_order.account
            for revision, price in (("01", 20.0), ("02", 19.5)):
                ib.wrapper.execDetails(req_id, cash_contract, Execution(execId="0001.fixture.980." + revision,
                    time=datetime.now(timezone.utc), acctNumber=cash_order.account, side="BOT", shares=1.0,
                    price=price, permId=cash_order.permId, orderRef=cash_order.orderRef))
            ib.wrapper.execDetailsEnd(req_id)
        monkeypatch.setattr(ib.client, "reqCompletedOrders", cash_completed)
        monkeypatch.setattr(ib.client, "reqExecutions", cash_executions)
        permissions = await adapter.option_account_permissions(scope)
        assert permissions.approval_status == "UNKNOWN" and permissions.permitted_structures == ()
        assert permissions.data_entitlement == "ALLOWED" and permissions.bp_semantics == "AVAILABLE_FUNDS"
        state = await adapter.option_account_state(scope, retain_lifecycle=retain)
        assert (state.equity_cash, state.cash_available, state.option_buying_power) == ("10000.123456789012", "9000", "8000")
        assert {v.name: v.value for v in state.raw_buying_power} == {"AvailableFunds": "8000", "BuyingPower": "32000"}
        assert [p.signed_contracts for p in state.positions] == [2, -2]
        assert [p.raw_cost_value for p in state.positions] == ["240", "90"]
        assert all(p.raw_cost_unit == "CASH_PER_CONTRACT" and p.raw_ref in retained for p in state.positions)
        proofs = [json.loads(retained[p.raw_ref]) for p in state.positions]
        assert [proof["cost_cash"] for proof in proofs] == ["480", "-180"]
        assert all(proof["source"] == "IB_POSITION_COST_SAMPLE" and proof["account"] == "DU-OPTIONS-FIXTURE" for proof in proofs)
        assert "IB_POSITION_COST_UNIT_UNVERIFIED" not in state.quality_reasons
        assert state.positions_complete and not state.unresolved_positions
        assert not state.orders_complete and not state.executions_complete and not state.lifecycle_complete
        assert [(r.broker_order_session_key, r.broker_order_id) for r in state.open_order_refs] == [("IBKR_PERM_ID", "19001")]
        assert state.source_checkpoint in retained
        assert len(state.cash_executions) == 1
        booked = state.cash_executions[0]
        assert (booked.execution_id, booked.price, booked.broker_order_id, booked.instrument_id) == (
            "0001.fixture.980.02", "19.5", "IBKR_PERM_ID:20980", "IBKR:100")
        assert booked.activity_ref in retained and booked.order_ref in retained
        assert booked.effective_at <= state.observed_at
        assert option_from_payload(type(state), dataclass_to_payload(state)) == state
        assert len(canceled_accounts) == 2 and len(canceled_positions) == 1
        assert not ib.wrapper._futures
        assert "positionMulti" not in ib.wrapper.__dict__ and "openOrder" not in ib.wrapper.__dict__
        if consume_account is not None:
            await consume_account(state)
        if exercise_orders:
            await submission_flow(adapter, ib, scope, monkeypatch, gate_factory=gate_factory,
                                  consume_event=consume_event, cancel_sender=cancel_sender,
                                  reconcile_reader=reconcile_reader, preview_reader=preview_reader)
        client._connected_since = datetime.now(timezone.utc)
        with pytest.raises(BrokerContractError, match="connection changed"):
            await adapter.option_snapshot(request)
    finally:
        client._connected.clear()
        client._ib = None
        await client._shutdown_sync_executor()


async def market_data_flow(adapter, ib, snapshot_request, monkeypatch):
    from ib_async import BarData, HistoricalTickLast, HistoricalTickBidAsk, TickAttribLast, TickAttribBidAsk
    from algo_trader_broker_sdk.options import QuoteSubscription, OptionHistoryRequest
    from algo_trader_broker_sdk.options_calendar import OptionCalendarQuery

    base = asdict(snapshot_request)
    base["bindings"] = snapshot_request.bindings[:1]
    first = adapter.stream_option_quotes(QuoteSubscription(**base, owner_id="owner-one", lease_seconds=30))
    second = adapter.stream_option_quotes(QuoteSubscription(**base, owner_id="owner-two", lease_seconds=30))
    try:
        q1, q2 = await asyncio.gather(anext(first), anext(second))
        assert q1.canonical_id == q2.canonical_id == base["bindings"][0].canonical_id
        assert q1.quality == q2.quality == "EXECUTABLE"
        assert q1.native_greeks is not None and q2.native_greeks is not None
        await first.aclose()
        assert len(ib.wrapper.reqId2Ticker) == 2  # Owner two plus pre-existing 900.
        remaining = next(key for key in ib.wrapper.reqId2Ticker if key != 900)
        ib.wrapper.tcpDataArrived()
        ib.wrapper.priceSizeTick(remaining, 1, 1.26, 6)
        ib.wrapper.tcpDataProcessed()
        update = await asyncio.wait_for(anext(second), 2)
        assert (update.bid, update.bid_size) == ("1.26", 6)
    finally:
        await first.aclose()
        await second.aclose()
    assert set(ib.wrapper.reqId2Ticker) == {900}

    start = datetime(2026, 10, 8, 13, 30, tzinfo=timezone.utc)
    calls = []
    def bars(req_id, contract, end, duration, bar_size, what, rth, fmt, keep, options):
        assert (contract.conId, duration, bar_size, what, rth, fmt, keep) == (1590, "1 D", "1 min", "TRADES", False, 2, False)
        calls.append("BAR")
        def receive():
            for offset in (0, 60):
                ib.wrapper.historicalData(req_id, BarData(date=str(int(start.timestamp()) + offset),
                    open=1.25, high=1.3, low=1.2, close=1.26, volume=3))
            ib.wrapper.historicalDataEnd(req_id, "", "")
        asyncio.get_running_loop().call_soon(receive)

    def ticks(req_id, contract, begin, end, count, what, rth, ignore, options):
        assert (contract.conId, begin, end, count, rth, ignore) == (1590, "20261008-13:30:00", "", 1000, False, False)
        calls.append(what)
        if what == "TRADES":
            rows = [HistoricalTickLast(start, TickAttribLast(), 1.25, qty, "CBOE", "") for qty in (1, 2, 3)]
            callback = ib.wrapper.historicalTicksLast
        else:
            rows = [HistoricalTickBidAsk(start, TickAttribBidAsk(), 1.25, 1.3, qty, 7) for qty in (5, 6)]
            callback = ib.wrapper.historicalTicksBidAsk
        asyncio.get_running_loop().call_soon(callback, req_id, rows, True)
    monkeypatch.setattr(ib.client, "reqHistoricalData", bars)
    monkeypatch.setattr(ib.client, "reqHistoricalTicks", ticks)
    for kind, count in (("BAR", 2), ("TRADE", 3), ("QUOTE", 2)):
        request = OptionHistoryRequest(**{**base, "feed": "provider_native"}, data_kind=kind,
            time_from=start.isoformat().replace("+00:00", "Z"), time_to=(start + timedelta(seconds=120 if kind == "BAR" else 1)).isoformat().replace("+00:00", "Z"),
            adjustment="raw", cursor=None, limit=1)
        output = []
        for _ in range(count):
            page = await adapter.option_history(request)
            assert page.coverage == "PARTIAL"
            assert option_from_payload(type(page), dataclass_to_payload(page)) == page
            output.extend(page.bars + page.trades + page.quotes)
            request = replace(request, cursor=page.next_cursor)
        assert page.complete and page.next_cursor is None and len(output) == count
        if kind == "TRADE":
            assert [row.contracts for row in output] == [1, 2, 3]  # Same second survives pagination.
    assert calls == ["BAR", "TRADES", "BID_ASK"] and not ib.wrapper._futures

    scope = {name: getattr(snapshot_request, name) for name in OptionScope.__dataclass_fields__}
    request = OptionCalendarQuery(**scope, bindings=snapshot_request.bindings, trade_date="2026-10-08")
    calendar = await adapter.option_calendar(request)
    assert all((row.session_open_at, row.session_close_at) == ("2026-10-08T13:30:00Z", "2026-10-08T20:15:00Z")
               for row in calendar.sessions)
    assert all(row.broker_entry_cutoff_at is None and row.reason_codes for row in calendar.sessions)
    closed = await adapter.option_calendar(replace(request, trade_date="2026-10-10"))
    assert all(not row.is_trading_day and row.session_close_at is None for row in closed.sessions)


async def submission_flow(adapter, ib, scope, monkeypatch, *, gate_factory=None, consume_event=None, cancel_sender=None, reconcile_reader=None, preview_reader=None):
    from algo_trader_broker_sdk.options_capabilities import OptionCapability, OptionShapeCapability
    from algo_trader_broker_sdk.submission_gate import order_fingerprint
    from algo_trader_broker_adapter_ibkr_paper.options_orders import build_order
    stored, sockets, evidence = [], [], []
    preview_sockets, previewing = [], False
    native_orders, native_fills, native_fees, originals = {}, [], {}, {}
    ib.client.connState = ib.client.CONNECTED
    ib.client._serverVersion = 178
    ib.client.clientId = ib.wrapper.clientId = 40

    async def sink(event):
        evidence.append(event)
        if consume_event is not None:
            await consume_event(adapter, event)
    adapter.set_option_event_handler(OptionVerifiedAccount(scope, "IBKR", "DU-OPTIONS-FIXTURE"), sink)

    def socket_send(raw):
        if previewing:
            preview_sockets.append(raw)
            return
        assert len(stored) == len(sockets) + 1  # Durable callback preceded socket I/O.
        sockets.append(raw)
        fields = raw[4:].split(b"\0")
        if fields[0] == b"4":
            native = native_orders[int(fields[2])]
            native[2].status = "Cancelled"
            order = native[1]
            asyncio.get_running_loop().call_soon(ib.wrapper.orderStatus, order.orderId, "Cancelled",
                1.0, 1.0, 999.0, order.permId, 0, 999.0, 40, "")

    def acknowledge(trade):
        native = deepcopy(trade.order)
        native.permId = 20000 + native.orderId
        native_orders[native.orderId] = [deepcopy(trade.contract), native, OrderState(status="Submitted")]
        asyncio.get_running_loop().call_soon(ib.wrapper.openOrder, native.orderId,
            deepcopy(trade.contract), native, OrderState(status="Submitted"))

    native_place = ib.client.placeOrder
    def place(order_id, contract, order):
        nonlocal previewing
        previewing = order.whatIf
        try:
            native_place(order_id, contract, order)
        finally:
            previewing = False
        if order.whatIf:
            state = OrderState(status="PreSubmitted", initMarginChange="310.25", commission=1.30, commissionCurrency="USD")
            asyncio.get_running_loop().call_soon(ib.wrapper.openOrder, order_id,
                deepcopy(contract), deepcopy(order), state)

    def open_orders():
        def receive():
            for contract, order, state in native_orders.values():
                if state.status not in {"Filled", "Cancelled"}:
                    ib.wrapper.openOrder(order.orderId, deepcopy(contract), deepcopy(order), deepcopy(state))
            ib.wrapper.openOrderEnd()
        asyncio.get_running_loop().call_soon(receive)

    def completed_orders(api_only):
        assert api_only is True
        def receive():
            for contract, order, state in native_orders.values():
                if state.status in {"Filled", "Cancelled"}:
                    # These fields are absent from the real completedOrder wire.
                    native = deepcopy(order)
                    native.clientId = native.orderId = 0
                    ib.wrapper.completedOrder(deepcopy(contract), native, deepcopy(state))
            ib.wrapper.completedOrdersEnd()
        asyncio.get_running_loop().call_soon(receive)

    def executions(req_id, query):
        assert query.acctCode == "DU-OPTIONS-FIXTURE" and query.time == ""
        def receive():
            for contract, execution in native_fills:
                ib.wrapper.execDetails(req_id, deepcopy(contract), deepcopy(execution))
                if execution.execId in native_fees:
                    ib.wrapper.commissionReport(deepcopy(native_fees[execution.execId]))
            ib.wrapper.execDetailsEnd(req_id)
        asyncio.get_running_loop().call_soon(receive)

    monkeypatch.setattr(ib.client.conn, "sendMsg", socket_send)
    monkeypatch.setattr(ib.client, "placeOrder", place)
    monkeypatch.setattr(ib.client, "reqAllOpenOrders", open_orders)
    monkeypatch.setattr(ib.client, "reqCompletedOrders", completed_orders)
    monkeypatch.setattr(ib.client, "reqExecutions", executions)
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
            from algo_trader_broker_sdk.options import OptionReplaceRequest
            replacement = option_from_payload(OptionReplaceRequest,
                {**dataclass_to_payload(request), "parent_order_ref": "fixture-existing-order"})
            with pytest.raises(BrokerCapabilityError, match="OPTION_REPLACE_UNSUPPORTED"):
                await adapter.replace_option_order(replacement)
            preview = (await adapter.preview_option_order(request) if preview_reader is None
                       else await preview_reader(adapter, request))
            assert (preview.source, preview.required_buying_power_cash, preview.estimated_fee_cash, preview.currency) == (
                "BROKER_WHAT_IF", "310.25", "1.3", "USD")
            assert "EMERGENCY_CLOSE_FEES_NOT_INCLUDED" in preview.warnings
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
            originals[request.client_order_id] = request
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
            # Continue the same native transaction through actual Wrapper
            # execution/commission/status callbacks after initial acknowledgement.
            from ib_async import Execution, CommissionReport
            for index, intent in enumerate(request.legs):
                execution = Execution(execId=f"0001.abcdef.{payload['order_id']:02d}{index}.01",
                    time=datetime.now(timezone.utc), acctNumber="DU-OPTIONS-FIXTURE", exchange="CBOE",
                    side="BOT" if intent.side == "BUY" else "SLD", shares=1.0 if combo else 2.0, price=1.25 + index,
                    permId=int(result.parent_order_ref), clientId=40, orderId=payload["order_id"],
                    cumQty=1.0 if combo else 2.0, avgPrice=1.25 + index, orderRef=request.client_order_id)
                native_fills.append((deepcopy(contracts[index]), deepcopy(execution)))
                ib.wrapper.execDetails(-1, contracts[index], execution)
                report = CommissionReport(execId=execution.execId, commission=0.65, currency="USD")
                native_fees[execution.execId] = deepcopy(report)
                ib.wrapper.commissionReport(report)
            if combo:
                aggregate = replace(execution, execId=f"0001.abcdef.{payload['order_id']:02d}P.01", side="BOT", price=-1.5)
                native_fills.append((deepcopy(expected_contract), deepcopy(aggregate)))
                ib.wrapper.execDetails(-1, expected_contract, aggregate)
            state = "Submitted" if combo else "Filled"
            native_orders[payload["order_id"]][2].status = state
            native_orders[payload["order_id"]][1].filledQuantity = 1.0 if combo else 2.0
            ib.wrapper.orderStatus(payload["order_id"], state, 1.0 if combo else 2.0, 1.0 if combo else 0.0,
                999.0, int(result.parent_order_ref), 0, 999.0, 40, "")
            await adapter._option_events.flush()
            if combo:
                from algo_trader_broker_sdk.options import OptionCancelRequest
                cancel_request = OptionCancelRequest(**asdict(scope), command_id="cancel-ib-bag",
                    parent_order_ref=result.parent_order_ref, authorization_ref="fixture-cancel-authority", valid_until=request.valid_until)
                async def retain_cancel(raw):
                    stored.append(raw)
                if cancel_sender is None:
                    cancel_gate = SubmissionGate(validate, lambda: None, retain_native=retain_cancel)
                    canceled = await adapter.cancel_option_order_guarded(cancel_request, cancel_gate, original=request)
                    assert cancel_gate.consumed
                else:
                    canceled = await cancel_sender(adapter, request, cancel_request, retain_cancel)
                assert canceled.status == "REQUESTED" and canceled.parent_order_ref == result.parent_order_ref
                native_cancel = json.loads(stored[-1])
                assert (native_cancel["client_id"], native_cancel["order_id"], native_cancel["parent_order_ref"]) == (
                    40, payload["order_id"], result.parent_order_ref)
                await asyncio.sleep(0)
                await adapter._option_events.flush()
        assert len(sockets) == 3 and [raw[4:].split(b"\0")[0] for raw in sockets] == [b"3", b"3", b"4"]
        assert len(preview_sockets) == 2 and all(raw[4:].split(b"\0")[0] == b"3" for raw in preview_sockets)
        # Continue the same execution through IB's native correction version.
        # The full .02 ID is retained; it must revise, not add to, the .01 fill.
        original_contract, original_execution = native_fills[0]
        corrected = replace(original_execution, execId=original_execution.execId.rsplit(".", 1)[0] + ".02",
                            price=1.24, avgPrice=1.24)
        native_fills.append((deepcopy(original_contract), deepcopy(corrected)))
        ib.wrapper.execDetails(-1, original_contract, corrected)
        corrected_fee = CommissionReport(execId=corrected.execId, commission=0.60, currency="USD")
        native_fees[corrected.execId] = deepcopy(corrected_fee)
        ib.wrapper.commissionReport(corrected_fee)
        await adapter._option_events.flush()
        # Recover from provider downloads, without relying on ib_async's cache.
        ib.wrapper.trades.clear()
        ib.wrapper.permId2Trade.clear()
        ib.wrapper.fills.clear()
        adapter._option_events.executions.clear()
        from algo_trader_broker_sdk.options import ReconcileOptionsRequest
        from algo_trader_broker_sdk.options_events import OptionNativeOrderQuery
        async def resolve_order(reference):
            return originals.get(reference)
        reconciliation_request = ReconcileOptionsRequest(**asdict(scope), since=None, cursor=None)
        reconciled = (await adapter.reconcile_options(reconciliation_request, resolve_order=resolve_order)
            if reconcile_reader is None else await reconcile_reader(adapter, reconciliation_request))
        assert [order.status for order in reconciled.orders] == ["FILLED", "CANCELED"]
        assert [order.filled_groups for order in reconciled.orders] == [2, 1]
        assert [order.remaining_groups for order in reconciled.orders] == [0, 1]
        assert reconciled.positions_complete and not reconciled.complete and not reconciled.executions_complete
        assert not reconciled.executions and not reconciled.activities
        detail = await adapter.read_option_order_evidence(OptionNativeOrderQuery(**asdict(scope), order_id=reconciled.orders[1].parent_order_ref))
        assert json.loads(detail.raw_payload)["order"]["orderId"] == 0
        assert sum(event.source == "IB_OPTION_SUBMISSION" for event in evidence) == 2
        async def resolve(native_id, symbol):
            from algo_trader_broker_sdk.options import QualificationResult
            binding, contract, _ = adapter._option_catalog[native_id]
            assert symbol == binding.local_symbol
            return QualificationResult(binding.canonical_id, "EXACT", binding, contract, ())
        fills, fees = [], []
        for event in evidence:
            fact = await adapter.decode_option_event(event, resolve)
            assert fact.session_key == "IBKR_PERM_ID"
            fills.extend(fact.executions)
            fees.extend(fact.fees)
        # Both reconciliation and the independent Account download replay the
        # same native versions; the unique financial identities remain four.
        assert len(fills) == len(fees) == 12
        unique = {fill.execution_id: fill for fill in fills}
        assert len(unique) == 4 and [fill.price for fill in unique.values()] == ["1.25", "1.25", "2.25", "1.24"]
        assert [fill.contracts for fill in unique.values()] == [2, 1, 1, 2]
        revised = unique[corrected.execId]
        assert revised.revision.original_execution_id == original_execution.execId and revised.revision.revision == 2
        assert all(fee.fee_cash == ("0.6" if fee.revision is not None else "0.65") for fee in fees)
        if gate_factory is None:
            # Continue the native read/send walkthrough with the admitted cash
            # order control port, preserving completedOrder's explicit zero fill.
            from algo_trader_broker_sdk.cash_control import CashOrderQuery
            cash_order = Order(orderId=990, clientId=40, permId=20990, orderRef="fixture-cash",
                account="DU-OPTIONS-FIXTURE", action="BUY", orderType="LMT", totalQuantity=1,
                lmtPrice=20, tif="DAY", filledQuantity=0)
            native_orders[990] = [Stock("SPY", "SMART", "USD", conId=100), cash_order, OrderState(status="Submitted")]
            cash = CashOrderQuery(**asdict(scope), client_order_id="fixture-cash", broker_order_id="990",
                instrument_id="IBKR:100", symbol="SPY", side="BUY", quantity="1", limit_price="20", tif="DAY")
            before = await adapter.read_cash_order(cash)
            assert before.state.status == "WORKING" and before.state.filled_quantity is None
            async def retain_cash(raw):
                stored.append(raw)
            cash_gate = SubmissionGate(validate, lambda: None, retain_native=retain_cash)
            assert await adapter.cancel_cash_order_guarded(cash, cash_gate) == "REQUESTED" and cash_gate.consumed
            terminal = await adapter.read_cash_order(cash)
            assert terminal.state.status == "CANCELLED" and terminal.state.filled_quantity == "0"
            assert json.loads(terminal.raw_payload)["completed"] is True
            assert json.loads(stored[-1])["source"] == "IB_CASH_CANCEL"
            assert terminal.state.broker_order_id == "IBKR_PERM_ID:20990"
            # Exercise the native SELL/CLOSE boundary with the same STK. The
            # original durable cash guard remains responsible for owned lots.
            from algo_trader_broker_sdk import StockOrderRequest
            position = [1]
            def cash_positions(req_id, account, model):
                ib.wrapper.positionMulti(req_id, account, model, native_orders[990][0], position[0], 20.0)
                ib.wrapper.positionMultiEnd(req_id)
            monkeypatch.setattr(ib.client, "reqPositionsMulti", cash_positions)
            async def cash_authority():
                stored.append(b"explicit-fixture-cash-authority")
            close_gate = SubmissionGate(cash_authority, lambda: None)
            close = StockOrderRequest(symbol="SPY", side="SELL", quantity=1, order_type="LMT", limit_price=20,
                account="DU-OPTIONS-FIXTURE", contract_id=100, client_order_id="fixture-cash-close", position_effect="CLOSE")
            sent = await adapter.place_stock_order_guarded(close, close_gate)
            assert close_gate.consumed
            closing = native_orders[int(sent.adapter_order_id)]
            assert closing[0].conId == 100 and closing[1].action == "SELL" and closing[1].openClose == "C"
            closing[1].filledQuantity, closing[2].status = 1, "Filled"
            native_fills.append((deepcopy(closing[0]), Execution(execId="0001.fixture.close.01",
                time=datetime.now(timezone.utc), acctNumber=close.account, side="SLD", shares=1., price=20.,
                permId=closing[1].permId, orderRef=close.client_order_id)))
            archive = {}
            async def archive_cash(raw):
                ref = sha256(raw).hexdigest()
                archive[ref] = raw
                return ref
            booked = await adapter.option_account_state(scope, retain_lifecycle=archive_cash)
            assert len(booked.cash_executions) == 1 and booked.cash_executions[0].side == "SELL"
            assert booked.cash_executions[0].broker_order_id == "IBKR_PERM_ID:" + str(closing[1].permId)
            position[0] = 0
            denied = SubmissionGate(cash_authority, lambda: None)
            with pytest.raises(BrokerContractError, match="exceeds the native long position"):
                await adapter.place_stock_order_guarded(close, denied)
            assert not denied.consumed
    finally:
        await adapter._option_events.flush()
        adapter.set_option_event_handler(OptionVerifiedAccount(scope, "IBKR", "DU-OPTIONS-FIXTURE"), None)
        ib.newOrderEvent -= acknowledge
        ib.client.connState = ib.client.DISCONNECTED
