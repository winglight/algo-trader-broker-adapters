from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import UTC, datetime

import pytest
from algo_trader_broker_adapter_ccxt_crypto import CCXTCryptoAdapter
from algo_trader_broker_sdk import BrokerConnectionError, BrokerOrderError

from .fakes import BALANCE, ORDER, FakeClient, settings


def order_payload(**overrides):
    payload = {
        "schemaVersion": "broker-order-request.v2",
        "commandId": "command-1",
        "clientOrderId": "client123",
        "executionTargetId": "okx-spot-demo-paper-1",
        "instrumentId": "crypto-spot:BTC-USDT:OKX",
        "side": "BUY",
        "orderType": "MARKET",
        "quantityDecimal": "0.001",
        "limitPriceDecimal": None,
        "stopPriceDecimal": None,
        "timeInForce": "GTC",
        "reduceOnly": False,
        "positionEffect": "AUTO",
        "positionGroupId": "crypto-canary",
        "legId": None,
    }
    payload.update(overrides)
    return payload


@pytest.mark.asyncio
async def test_inactive_install_performs_no_exchange_io() -> None:
    backend = FakeClient()
    adapter = CCXTCryptoAdapter(settings(public_data_enabled=False), backend=backend)
    await adapter.start()
    assert adapter.connection_state_snapshot().state == "installed"
    assert backend.calls == []
    await adapter.close()


@pytest.mark.asyncio
async def test_inactive_profile_endpoints_cannot_bypass_network_gates() -> None:
    backend = FakeClient()
    adapter = CCXTCryptoAdapter(settings(public_data_enabled=False), backend=backend)
    await adapter.start()

    with pytest.raises(BrokerConnectionError):
        await adapter.market_metadata_v2()
    with pytest.raises(BrokerConnectionError):
        await adapter.request_market_snapshot({"symbol": "BTC/USDT"})
    with pytest.raises(BrokerConnectionError):
        await adapter.request_open_orders()

    assert backend.calls == []
    await adapter.close()


@pytest.mark.asyncio
async def test_public_stream_methods_follow_runner_awaitable_contract(caplog) -> None:
    caplog.set_level("INFO")
    backend = FakeClient()
    adapter = CCXTCryptoAdapter(settings(), backend=backend)
    await adapter.start()

    ticker_stream = await adapter.stream_real_time_price(
        {"symbol": "BTC/USDT"}, snapshot=True
    )
    ticker = await anext(ticker_stream)
    assert ticker.symbol == "BTC/USDT"
    assert ticker.last == 10000.0

    trade_stream = await adapter.stream_tick_by_tick_data(
        {"symbol": "BTC/USDT"}, tick_type="Last", number_of_ticks=1
    )
    trade = await anext(trade_stream)
    assert trade.price == 10000.0
    assert trade.size == 0.001

    bar_stream = await adapter.stream_historical_bars(
        {"symbol": "BTC/USDT"}, bar_size="1 min"
    )
    bar = await anext(bar_stream)
    assert bar.close == 1.5

    events = [
        getattr(record, "event", None)
        for record in caplog.records
    ]
    assert "broker.crypto.metadata_accepted" in events
    assert events.count("broker.crypto.public_stream_ready") == 3
    assert "broker.crypto.readiness_changed" in events

    await adapter.close()


@pytest.mark.asyncio
async def test_trade_stream_discards_cached_stale_and_duplicate_rows() -> None:
    class CachedTradeClient(FakeClient):
        async def watch_trades(self, symbol):
            self.calls.append(("watch_trades", symbol))
            now_ms = int(datetime.now(UTC).timestamp() * 1000)
            fresh = {
                "id": "fresh-1",
                "price": "10001",
                "amount": "0.002",
                "timestamp": now_ms,
            }
            return [
                {
                    "id": "stale-1",
                    "price": "9999",
                    "amount": "0.001",
                    "timestamp": now_ms - 121_000,
                },
                fresh,
                dict(fresh),
                {
                    "id": "fresh-2",
                    "price": "10002",
                    "amount": "0.003",
                    "timestamp": now_ms + 1,
                },
            ]

    adapter = CCXTCryptoAdapter(settings(), backend=CachedTradeClient())
    await adapter.start()
    stream = await adapter.stream_tick_by_tick_data(
        {"symbol": "BTC/USDT"}, number_of_ticks=2
    )

    first = await anext(stream)
    second = await anext(stream)

    assert (first.price, first.size) == (10001.0, 0.002)
    assert (second.price, second.size) == (10002.0, 0.003)
    await adapter.close()


@pytest.mark.asyncio
async def test_account_summary_aggregates_assets_into_stable_valuation_units() -> None:
    class ValuedBalanceClient(FakeClient):
        async def fetch_balance(self):
            balance = deepcopy(BALANCE)
            for item in balance["info"]["data"][0]["details"]:
                if item["ccy"] == "BTC":
                    item.update({"cashBal": "1", "availBal": "0.5", "eqUsd": "9900"})
                if item["ccy"] == "ETH":
                    item.update({"cashBal": "2", "availBal": "1", "eqUsd": "19800"})
                if item["ccy"] == "USDT":
                    item.update({"eqUsd": "999"})
            return balance

    backend = ValuedBalanceClient()
    adapter = CCXTCryptoAdapter(
        settings(
            private_read_enabled=True,
            api_key="key",
            secret="secret",
            passphrase="passphrase",
        ),
        backend=backend,
    )
    await adapter.start()

    summary = await adapter.get_account_summary()
    by_tag = {item.tag: item for item in summary}

    assert len(by_tag) == len(summary)
    assert by_tag["NetLiquidation"].currency == "USD"
    assert by_tag["NetLiquidation"].value == "30699"
    assert by_tag["AvailableFunds"].value == "15849"
    assert by_tag["NetLiquidationUSDT"].currency == "USDT"
    assert by_tag["NetLiquidationUSDT"].value == "31000"
    assert by_tag["NetLiquidationUSDC"].currency == "USDC"
    assert by_tag["NetLiquidationUSDC"].value == "30699"
    assert {value for name, value in backend.calls if name == "fetch_ticker"} == {
        "BTC/USDT",
        "ETH/USDT",
    }

    await adapter.close()


@pytest.mark.asyncio
async def test_market_order_uses_base_quantity_cash_and_client_identity() -> None:
    backend = FakeClient()
    adapter = CCXTCryptoAdapter(
        settings(
            private_read_enabled=True,
            trading_enabled=True,
            market_order_enabled=True,
            api_key="key",
            secret="secret",
            passphrase="passphrase",
        ),
        backend=backend,
    )
    await adapter.start()
    result = await adapter.place_order_v2(order_payload())
    create = next(value for name, value in backend.calls if name == "create_order")
    assert create[:5] == ("BTC/USDT", "market", "buy", "0.001", None)
    assert create[5] == {"tdMode": "cash", "clOrdId": "client123", "tgtCcy": "base_ccy"}
    assert result["identity"]["brokerOrderId"] == "10001"
    assert result["clientOrderId"] == "client123"
    await adapter.close()


@pytest.mark.asyncio
async def test_limit_order_quantizes_and_rejects_native_parameters() -> None:
    backend = FakeClient()
    adapter = CCXTCryptoAdapter(
        settings(
            private_read_enabled=True,
            trading_enabled=True,
            api_key="key",
            secret="secret",
            passphrase="passphrase",
        ),
        backend=backend,
    )
    await adapter.start()
    await adapter.place_order_v2(
        order_payload(orderType="LIMIT", limitPriceDecimal="10000.09")
    )
    create = next(value for name, value in backend.calls if name == "create_order")
    assert create[4] == "10000"
    assert create[5] == {"tdMode": "cash", "clOrdId": "client123"}
    await adapter.close()


@pytest.mark.asyncio
async def test_timeout_reconciles_once_and_never_resubmits() -> None:
    backend = FakeClient()
    backend.create_timeout = True
    backend.reconciled_order = ORDER
    adapter = CCXTCryptoAdapter(
        settings(
            private_read_enabled=True,
            trading_enabled=True,
            market_order_enabled=True,
            api_key="key",
            secret="secret",
            passphrase="passphrase",
        ),
        backend=backend,
    )
    await adapter.start()
    result = await adapter.place_order_v2(order_payload())
    assert result["identity"]["brokerOrderId"] == "10001"
    assert len([name for name, _ in backend.calls if name == "create_order"]) == 1
    await adapter.close()


@pytest.mark.asyncio
async def test_unresolved_timeout_is_unknown_and_suppresses_retry(caplog) -> None:
    backend = FakeClient()
    backend.create_timeout = True
    adapter = CCXTCryptoAdapter(
        settings(
            private_read_enabled=True,
            trading_enabled=True,
            market_order_enabled=True,
            api_key="key",
            secret="secret",
            passphrase="passphrase",
        ),
        backend=backend,
    )
    await adapter.start()
    with pytest.raises(BrokerOrderError) as exc:
        await adapter.place_order_v2(order_payload())
    assert exc.value.code == "order_outcome_unknown"
    assert exc.value.details["retry_allowed"] is False
    assert len([name for name, _ in backend.calls if name == "create_order"]) == 1
    assert any(
        getattr(record, "event", None) == "broker.crypto.order_unknown"
        for record in caplog.records
    )
    await adapter.close()


@pytest.mark.asyncio
async def test_unresolved_cancel_timeout_is_unknown_and_never_retried() -> None:
    backend = FakeClient()
    backend.cancel_timeout = True
    backend.fetch_order_fails = True
    adapter = CCXTCryptoAdapter(
        settings(
            private_read_enabled=True,
            trading_enabled=True,
            api_key="key",
            secret="secret",
            passphrase="passphrase",
        ),
        backend=backend,
    )
    await adapter.start()
    with pytest.raises(BrokerOrderError) as exc:
        await adapter.cancel_order_v2(
            {
                "commandId": "cancel-command-1",
                "executionTargetId": "okx-spot-demo-paper-1",
                "brokerOrderId": "10001",
                "instrumentId": "crypto-spot:BTC-USDT:OKX",
            }
        )
    assert exc.value.code == "order_outcome_unknown"
    assert len([name for name, _ in backend.calls if name == "cancel_order"]) == 1
    await adapter.close()


@pytest.mark.asyncio
async def test_reconciliation_has_target_scoped_decimal_payloads() -> None:
    backend = FakeClient()
    adapter = CCXTCryptoAdapter(
        settings(
            private_read_enabled=True,
            api_key="key",
            secret="secret",
            passphrase="passphrase",
        ),
        backend=backend,
    )
    await adapter.start()
    result = await adapter.reconcile_v2()
    assert result["executionTargetId"] == "okx-spot-demo-paper-1"
    assert result["orderUpdates"][0]["instrumentId"] == "crypto-spot:BTC-USDT:OKX"
    assert {item["currency"] for item in result["balances"]} == {"BTC", "ETH", "USDT"}
    assert all(item["quantityDecimal"] == "0" for item in result["positions"])
    await adapter.close()


@pytest.mark.asyncio
async def test_reconciliation_handler_receives_snapshot_and_generation(caplog) -> None:
    caplog.set_level("INFO")
    backend = FakeClient()
    adapter = CCXTCryptoAdapter(
        settings(
            private_read_enabled=True,
            api_key="key",
            secret="secret",
            passphrase="passphrase",
        ),
        backend=backend,
    )
    evidence = []

    async def handler(snapshot, generation):
        evidence.append((snapshot["executionTargetId"], generation))

    adapter.set_reconciliation_handler(handler)
    await adapter.start()
    await adapter.reconcile_v2()

    assert evidence == [
        ("okx-spot-demo-paper-1", 1),
        ("okx-spot-demo-paper-1", 1),
    ]
    assert sum(
        getattr(record, "event", None)
        == "broker.crypto.reconciliation_completed"
        for record in caplog.records
    ) == 2
    await adapter.close()


@pytest.mark.asyncio
async def test_private_stream_first_events_are_logged(caplog) -> None:
    caplog.set_level("INFO")

    class FirstEventClient(FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.seen: set[str] = set()
            self.wait_forever = asyncio.Event()

        async def _first(self, stream, value):
            if stream not in self.seen:
                self.seen.add(stream)
                return value
            await self.wait_forever.wait()
            raise AssertionError("unreachable")

        async def watch_orders(self):
            return await self._first("orders", [])

        async def watch_balance(self):
            return await self._first("balance", await self.fetch_balance())

        async def watch_my_trades(self):
            return await self._first("trades", [])

    backend = FirstEventClient()
    adapter = CCXTCryptoAdapter(
        settings(
            private_read_enabled=True,
            api_key="key",
            secret="secret",
            passphrase="passphrase",
        ),
        backend=backend,
    )
    await adapter.start()
    await asyncio.sleep(0.01)
    await adapter.close()

    ready_streams = {
        getattr(record, "broker.stream", None)
        for record in caplog.records
        if getattr(record, "event", None) == "broker.crypto.private_stream_ready"
    }
    assert ready_streams == {"orders", "balance", "trades"}
