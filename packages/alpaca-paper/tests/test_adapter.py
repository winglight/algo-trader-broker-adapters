from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest

from algo_trader_broker_adapter_alpaca_paper import AlpacaPaperAdapter
from algo_trader_broker_sdk import (
    BrokerCapabilityError,
    BrokerOrderError,
    FutureOrderRequest,
    OptionOrderRequest,
    StockOrderRequest,
)


SETTINGS = {
    "alpaca_api_key_id": "test-key",
    "alpaca_secret_key": "test-secret",
}


class FakeStream:
    def __init__(self, values: list[Any]) -> None:
        self.values = list(values)
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.values:
            raise StopAsyncIteration
        return self.values.pop(0)

    async def close(self) -> None:
        self.closed = True


class FakeBackend:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.trade_handler = None
        self.account = {
            "id": "paper-account",
            "currency": "USD",
            "equity": "100000",
            "last_equity": "99000",
            "cash": "50000",
            "buying_power": "200000",
        }
        self.positions = [
            {
                "symbol": "AAPL",
                "qty": "3",
                "avg_entry_price": "200",
                "exchange": "NASDAQ",
            }
        ]
        self.asset = {
            "id": "asset-uuid",
            "class": "us_equity",
            "exchange": "NASDAQ",
            "active": True,
            "tradable": True,
            "shortable": True,
            "fractionable": True,
        }
        self.order = {
            "id": "order-uuid",
            "client_order_id": "client-123",
            "symbol": "AAPL",
            "qty": "2",
            "filled_qty": "0",
            "status": "accepted",
            "created_at": "2026-07-15T14:30:00Z",
        }
        self.submit_timeout = False
        self.reconcile_order = None

    async def get_account(self):
        self.calls.append(("get_account", None))
        return self.account

    async def get_positions(self):
        self.calls.append(("get_positions", None))
        return self.positions

    async def get_asset(self, symbol):
        self.calls.append(("get_asset", symbol))
        return self.asset

    async def start_trade_updates(self, handler, failure_handler):
        self.trade_handler = handler
        self.failure_handler = failure_handler
        self.calls.append(("start_trade_updates", None))

    async def stop_trade_updates(self):
        self.calls.append(("stop_trade_updates", None))

    async def close(self):
        self.calls.append(("close", None))

    def make_order_request(self, request):
        return request

    def make_timeframe(self, bar_size):
        return bar_size if bar_size in {"1 min", "5 mins"} else None

    async def submit_order(self, request):
        self.calls.append(("submit_order", request))
        if self.submit_timeout:
            raise asyncio.TimeoutError
        return self.order

    async def get_order_by_client_id(self, client_order_id):
        self.calls.append(("get_order_by_client_id", client_order_id))
        return self.reconcile_order

    async def get_order_by_id(self, order_id):
        self.calls.append(("get_order_by_id", order_id))
        return self.order

    async def cancel_order(self, order_id):
        self.calls.append(("cancel_order", order_id))

    async def get_orders(self, *, status, after=None):
        self.calls.append(("get_orders", (status, after)))
        return [self.order]

    async def get_fill_activities(self, since):
        self.calls.append(("get_fill_activities", since))
        return [
            {
                "id": "fill-uuid",
                "order_id": "order-uuid",
                "symbol": "AAPL",
                "qty": "2",
                "price": "201",
                "transaction_time": "2026-07-15T14:31:00Z",
            }
        ]

    async def get_snapshot(self, symbol):
        self.calls.append(("get_snapshot", symbol))
        return {
            "latest_trade": {
                "price": "201.2",
                "size": "2",
                "timestamp": "2026-07-15T14:31:00Z",
            },
            "latest_quote": {
                "bid_price": "201.1",
                "ask_price": "201.3",
                "bid_size": "10",
                "ask_size": "12",
                "timestamp": "2026-07-15T14:31:00Z",
            },
            "daily_bar": {"close": "200.5"},
        }

    async def get_bars(self, symbol, *, timeframe, start, end):
        self.calls.append(("get_bars", (symbol, timeframe, start, end)))
        return [
            {
                "timestamp": "2026-07-15T14:30:00Z",
                "open": "200",
                "high": "202",
                "low": "199",
                "close": "201",
                "volume": "1000",
                "vwap": "200.5",
                "trade_count": 50,
            }
        ]

    async def stream_stock(self, symbol, kind):
        self.calls.append(("stream_stock", (symbol, kind)))
        if kind == "bars":
            return FakeStream(
                [
                    {
                        "timestamp": "2026-07-15T14:31:00Z",
                        "open": 201,
                        "high": 202,
                        "low": 200,
                        "close": 201.5,
                        "volume": 100,
                    }
                ]
            )
        if kind == "quotes":
            return FakeStream(
                [
                    {
                        "timestamp": "2026-07-15T14:31:00Z",
                        "bid_price": 201.1,
                        "ask_price": 201.3,
                        "bid_size": 10,
                        "ask_size": 12,
                    }
                ]
            )
        return FakeStream(
            [
                {
                    "timestamp": "2026-07-15T14:31:00Z",
                    "price": 201.2,
                    "size": 2,
                    "exchange": "V",
                }
            ]
        )


async def connected(backend: FakeBackend | None = None):
    backend = backend or FakeBackend()
    adapter = AlpacaPaperAdapter(SETTINGS, backend=backend)
    await adapter.start()
    return adapter, backend


@pytest.mark.asyncio
async def test_lifecycle_account_positions_and_pnl() -> None:
    adapter, backend = await connected()

    summary = await adapter.get_account_summary()
    positions = await adapter.get_positions()
    pnl = await adapter.get_account_pnl()
    await adapter.close()

    assert adapter.connection_state_snapshot().connected is False
    assert {item.tag for item in summary} >= {"NetLiquidation", "CashBalance", "BuyingPower"}
    assert positions[0].symbol == "AAPL"
    assert positions[0].position == 3
    assert pnl == {"daily": 1000.0, "realized": None, "unrealized": None}
    assert ("start_trade_updates", None) in backend.calls


@pytest.mark.asyncio
async def test_submit_rejects_client_order_id_over_vendor_limit_before_api_call() -> None:
    adapter, backend = await connected()
    request = StockOrderRequest(
        symbol="AAPL",
        side="BUY",
        quantity=1,
        order_type="MKT",
        tif="DAY",
        client_order_id="x" * 49,
    )

    with pytest.raises(BrokerOrderError) as exc_info:
        await adapter.place_stock_order(request)

    await adapter.close()
    assert exc_info.value.code == "client_order_id_invalid"
    assert not any(name == "submit_order" for name, _value in backend.calls)


@pytest.mark.asyncio
async def test_whole_share_order_is_submitted_once_with_client_identity() -> None:
    adapter, backend = await connected()

    result = await adapter.place_stock_order(
        StockOrderRequest(
            symbol="aapl",
            side="BUY",
            quantity=2,
            order_type="MKT",
            client_order_id="client-123",
        )
    )

    assert result.adapter_order_id == "order-uuid"
    assert result.adapter_order_ref == "client-123"
    assert result.status == "PendingSubmit"
    assert len([call for call in backend.calls if call[0] == "submit_order"]) == 1


@pytest.mark.asyncio
async def test_submit_timeout_only_reconciles_and_never_resubmits() -> None:
    backend = FakeBackend()
    backend.submit_timeout = True
    backend.reconcile_order = backend.order
    adapter, _ = await connected(backend)

    result = await adapter.place_stock_order(
        StockOrderRequest(
            symbol="AAPL",
            side="BUY",
            quantity=2,
            order_type="MKT",
            client_order_id="client-123",
        )
    )

    assert result.adapter_order_id == "order-uuid"
    assert len([call for call in backend.calls if call[0] == "submit_order"]) == 1
    assert ("get_order_by_client_id", "client-123") in backend.calls


@pytest.mark.asyncio
async def test_unresolved_submit_timeout_has_stable_unknown_outcome() -> None:
    backend = FakeBackend()
    backend.submit_timeout = True
    adapter, _ = await connected(backend)

    with pytest.raises(BrokerOrderError) as exc:
        await adapter.place_stock_order(
            StockOrderRequest(
                symbol="AAPL",
                side="BUY",
                quantity=2,
                order_type="MKT",
                client_order_id="client-123",
            )
        )

    assert exc.value.code == "broker_order_outcome_unknown"
    assert len([call for call in backend.calls if call[0] == "submit_order"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "order_request",
    [
        StockOrderRequest(symbol="AAPL", side="BUY", quantity=0.5, order_type="MKT", client_order_id="c"),
        StockOrderRequest(symbol="AAPL", side="BUY", quantity=1, order_type="MKT", client_order_id=None),
        StockOrderRequest(symbol="AAPL", side="BUY", quantity=1, order_type="MKT", client_order_id="c", outside_rth=True),
    ],
)
async def test_invalid_orders_are_rejected_before_submit(
    order_request: StockOrderRequest,
) -> None:
    adapter, backend = await connected()

    with pytest.raises((BrokerOrderError, BrokerCapabilityError)):
        await adapter.place_stock_order(order_request)

    assert not [call for call in backend.calls if call[0] == "submit_order"]


@pytest.mark.asyncio
async def test_future_options_dom_and_scanner_are_explicitly_rejected() -> None:
    adapter, _ = await connected()

    with pytest.raises(BrokerCapabilityError):
        await adapter.place_future_order(
            FutureOrderRequest(symbol="MNQ", side="BUY", quantity=1, order_type="MKT")
        )
    with pytest.raises(BrokerCapabilityError):
        await adapter.place_option_order(
            OptionOrderRequest(symbol="AAPL", side="BUY", quantity=1, order_type="MKT")
        )
    with pytest.raises(BrokerCapabilityError):
        await adapter.stream_market_depth({"symbol": "AAPL"})
    with pytest.raises(BrokerCapabilityError):
        await adapter.request_scanner_parameters()


@pytest.mark.asyncio
async def test_reconciliation_uses_stable_fill_activity_id() -> None:
    adapter, _ = await connected()

    updates = await adapter.request_executions(datetime(2026, 7, 15, tzinfo=UTC))

    assert updates[0].adapter_execution_id == "fill-uuid"
    assert updates[0].adapter_order_id == "order-uuid"


@pytest.mark.asyncio
async def test_market_snapshot_history_and_live_bar_preserve_utc() -> None:
    adapter, _ = await connected()
    contract = {"symbol": "AAPL", "secType": "STK", "currency": "USD"}

    snapshot = await adapter.request_market_snapshot(contract)
    history = await adapter.get_historical_data(contract, duration="1 D", bar_size="5 mins")
    live = await adapter.stream_historical_bars(contract, bar_size="1 min")
    live_items = [item async for item in live]

    assert snapshot["last"] == 201.2
    assert history[0].time.tzinfo is UTC
    assert live_items[0].close == 201.5


@pytest.mark.asyncio
async def test_trade_update_handler_uses_execution_id_and_refreshes_snapshots() -> None:
    adapter, backend = await connected()
    updates = []

    async def handler(update):
        updates.append(update)

    adapter.set_trade_update_handler(handler)
    await backend.trade_handler(
        {
            "event": "fill",
            "execution_id": "execution-uuid",
            "timestamp": "2026-07-15T14:31:00Z",
            "price": "201",
            "qty": "2",
            "order": {**backend.order, "status": "filled", "filled_qty": "2"},
        }
    )

    assert updates[0].adapter_execution_id == "execution-uuid"
    assert updates[0].status == "Filled"
    assert len([call for call in backend.calls if call[0] == "get_account"]) >= 2
