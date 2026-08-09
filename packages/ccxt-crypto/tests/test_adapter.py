from __future__ import annotations

import pytest
from algo_trader_broker_adapter_ccxt_crypto import CCXTCryptoAdapter
from algo_trader_broker_sdk import BrokerOrderError

from .fakes import ORDER, FakeClient, settings


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
async def test_unresolved_timeout_is_unknown_and_suppresses_retry() -> None:
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
