from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from ati_shared_sdk.common.schemas.crypto_perpetual import (
    FundingLedgerEntryV1,
    PerpetualPositionRiskV1,
)
from ati_shared_sdk.common.schemas.multi_asset_market_data import (
    MarketDataObjectEnvelopeV1,
)

from algo_trader_broker_sdk import BrokerConnectionError, BrokerContractError, BrokerOrderError
from algo_trader_broker_adapter_ccxt_crypto_perpetual import PerpetualContext
from algo_trader_broker_adapter_ccxt_crypto_perpetual.client import (
    OKXDemoPerpetualClient,
)
from algo_trader_broker_adapter_ccxt_crypto_perpetual.quantizer import PerpetualMarketRules
from algo_trader_broker_adapter_ccxt_crypto_perpetual.mapping import (
    decimal,
    position_payload,
    timestamp,
)
from algo_trader_broker_adapter_ccxt_crypto_perpetual.settings import (
    CCXTCryptoPerpetualSettings,
)

from .fakes import FakeBackend, market


def _settings(**overrides: object) -> dict[str, object]:
    result: dict[str, object] = {
        "exchange_id": "okx",
        "sandbox": True,
        "live": False,
        "api_key": "demo-key",
        "secret": "demo-secret",
        "passphrase": "demo-passphrase",
        "allowed_symbols": "BTC/USDT:USDT,ETH/USDT:USDT",
        "execution_target_id": "okx-perpetual-demo-paper-1",
        "market_data_target_id": "okx-perpetual-demo-market-1",
        "position_mode": "ONE_WAY",
        "margin_mode": "ISOLATED",
        "fixed_leverage": 2,
    }
    result.update(overrides)
    return result


def _order(**overrides: object) -> dict[str, object]:
    result: dict[str, object] = {
        "schemaVersion": "broker-order-request.v2",
        "commandId": "phase5-command-1",
        "clientOrderId": "phase5-client-order-1",
        "executionTargetId": "okx-perpetual-demo-paper-1",
        "instrumentId": "crypto-perpetual:BTC-USDT:USDT:OKX",
        "side": "BUY",
        "orderType": "LIMIT",
        "quantityDecimal": "10",
        "limitPriceDecimal": "60000.15",
        "stopPriceDecimal": None,
        "timeInForce": "GTC",
        "reduceOnly": False,
        "positionEffect": "OPEN",
        "positionGroupId": "phase5-paper",
        "legId": None,
    }
    result.update(overrides)
    return result


def test_settings_are_demo_only_one_way_isolated_2x() -> None:
    settings = CCXTCryptoPerpetualSettings.from_mapping(_settings())
    assert settings.fixed_leverage == 2
    assert settings.execution_target_id == "okx-perpetual-demo-paper-1"
    assert settings.redacted()["credential_fingerprint"]
    assert "demo-secret" not in str(settings.redacted())

    for overrides in (
        {"live": True},
        {"sandbox": False},
        {"position_mode": "HEDGE"},
        {"margin_mode": "CROSS"},
        {"fixed_leverage": 1},
    ):
        with pytest.raises(BrokerContractError):
            CCXTCryptoPerpetualSettings.from_mapping(_settings(**overrides))


def test_market_metadata_requires_linear_usdt_swap_and_broker_contract_step() -> None:
    rules = PerpetualMarketRules.from_ccxt(
        "BTC/USDT:USDT", market("BTC/USDT:USDT")
    )
    assert str(rules.contract_multiplier) == "0.01"
    assert str(rules.quantity_step) == "0.01"
    assert str(rules.quantize_contracts("0.01")) == "0.01"
    with pytest.raises(BrokerOrderError, match="below minimum"):
        rules.quantize_contracts("0.001")
    with pytest.raises(BrokerOrderError, match="broker contract step"):
        rules.quantize_contracts("0.015")

    inverse = market("BTC/USDT:USDT")
    inverse["linear"] = False
    inverse["inverse"] = True
    with pytest.raises(BrokerContractError, match="linear"):
        PerpetualMarketRules.from_ccxt("BTC/USDT:USDT", inverse)


def test_broker_float_is_converted_only_at_external_mapping_boundary() -> None:
    assert decimal(0.01) == Decimal("0.01")
    assert timestamp("1786867200000") == "2026-08-16T08:00:00.000Z"
    with pytest.raises(ValueError, match="non-finite"):
        decimal(float("nan"))


@pytest.mark.asyncio
async def test_index_stream_subscribes_to_okx_index_instrument() -> None:
    client = object.__new__(OKXDemoPerpetualClient)
    exchange = AsyncMock()
    exchange.watch_mark_price.return_value = {"last": "63000"}
    client._exchange = exchange

    assert await client.watch_index_price("BTC/USDT:USDT") == {"last": "63000"}
    exchange.watch_mark_price.assert_awaited_once_with(
        "BTC/USDT",
        {"channel": "index-tickers"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    [
        "indexPrice",
        "initialMargin",
        "maintenanceMargin",
        "marginRatio",
        "maintenanceTierId",
    ],
)
async def test_non_flat_position_requires_complete_risk_evidence(field: str) -> None:
    backend = FakeBackend()
    position = (await backend.fetch_positions(list(_settings()["allowed_symbols"].split(","))))[0]
    position.pop(field)
    rules = PerpetualMarketRules.from_ccxt(
        "BTC/USDT:USDT", market("BTC/USDT:USDT")
    )

    with pytest.raises(ValueError, match="required risk fields"):
        position_payload(
            position,
            rules=rules,
            account_id="okx-demo-perpetual",
            execution_target_id="okx-perpetual-demo-paper-1",
        )


@pytest.mark.asyncio
async def test_connect_reads_back_mode_leverage_and_reconciles() -> None:
    backend = FakeBackend()
    adapter = PerpetualContext(_settings(), backend=backend)

    await adapter.connect()

    assert adapter.connection_state_snapshot().connected is True
    assert adapter.connection_state_snapshot().state == "trading_ready"
    assert adapter.adapter_id == "ccxt_crypto"
    assert not hasattr(adapter, "manifest")
    assert not hasattr(adapter, "capabilities")
    reconciliation = await adapter.reconcile_v2()
    assert reconciliation["executionTargetId"] == "okx-perpetual-demo-paper-1"
    assert reconciliation["positions"][0]["quantityDecimal"] == "10"
    risks = await adapter.position_risk_v1()
    assert risks[0]["baseExposureDecimal"] == "0.1"
    assert risks[0]["markNotionalDecimal"] == "6000"
    assert risks[0]["liquidationDistanceDecimal"] == "0.5"
    funding = await adapter.funding_ledger_v1()
    assert funding[0]["brokerLedgerId"] == "funding-bill-1"
    assert funding[0]["amountDecimal"] == "-0.06"
    PerpetualPositionRiskV1.model_validate(risks[0])
    FundingLedgerEntryV1.model_validate(funding[0])


@pytest.mark.asyncio
async def test_connect_blocks_hedged_cross_or_non_2x_state() -> None:
    for field, value in (
        ("hedged", True),
        ("margin_mode", "cross"),
        ("leverage", "3"),
    ):
        backend = FakeBackend()
        setattr(backend, field, value)
        adapter = PerpetualContext(_settings(), backend=backend)
        with pytest.raises(BrokerConnectionError):
            await adapter.connect()
        assert adapter.connection_state_snapshot().state == "blocked"


@pytest.mark.asyncio
async def test_reconciliation_blocks_runtime_policy_or_metadata_drift() -> None:
    backend = FakeBackend()
    adapter = PerpetualContext(_settings(), backend=backend)
    await adapter.connect()

    backend.leverage = "3"
    with pytest.raises(BrokerConnectionError, match="drifted"):
        await adapter.reconcile_v2()
    diagnostics = adapter.connection_diagnostics()
    assert diagnostics["state"] == "blocked"
    assert diagnostics["policyReadback"]["matches"] is False

    backend.leverage = "2"
    backend.tick_size = "0.2"
    with pytest.raises(BrokerConnectionError, match="metadata changed"):
        await adapter.reconcile_v2()


@pytest.mark.asyncio
async def test_reconciliation_generation_is_monotonic_and_stream_failure_recovers(
    monkeypatch,
) -> None:
    backend = FakeBackend()
    adapter = PerpetualContext(_settings(), backend=backend)
    generations: list[int] = []
    lifecycle: list[tuple[str, int]] = []
    connection_states: list[tuple[str, dict[str, object]]] = []

    adapter.set_reconciliation_handler(
        lambda _snapshot, generation: (
            generations.append(generation),
            lifecycle.append(("reconcile", generation)),
        )
    )
    adapter.add_connection_listener(
        lambda state, payload: connection_states.append((state, dict(payload)))
    )
    await adapter.connect()
    await adapter.reconcile_v2()

    assert generations == [1, 2]
    assert adapter.connection_diagnostics()["reconciliationGeneration"] == 2

    async def immediate_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(
        "algo_trader_broker_adapter_ccxt_crypto_perpetual.adapter.asyncio.sleep",
        immediate_sleep,
    )
    adapter._start_streams = lambda: lifecycle.append(
        ("streams", adapter.connection_diagnostics()["reconciliationGeneration"])
    )
    await adapter._stream_failed("market_data", TimeoutError("test"))
    assert adapter.connection_state_snapshot().state == "blocked"
    assert adapter._recovery_task is not None
    await adapter._recovery_task

    assert generations == [1, 2, 3]
    assert lifecycle[-2:] == [("reconcile", 3), ("streams", 3)]
    assert adapter.connection_state_snapshot().state == "trading_ready"
    assert connection_states[-1][0] == "connected"
    assert connection_states[-1][1]["executionTargetId"] == (
        "okx-perpetual-demo-paper-1"
    )
    await adapter.close()
    assert adapter._recovery_task is None


@pytest.mark.asyncio
async def test_v2_order_compiles_only_reviewed_native_params() -> None:
    backend = FakeBackend()
    adapter = PerpetualContext(_settings(), backend=backend)
    await adapter.connect()

    result = await adapter.place_order_v2(_order())

    assert result["status"] == "SUBMITTED"
    assert result["instrumentId"] == "crypto-perpetual:BTC-USDT:USDT:OKX"
    submitted = backend.created[0]
    assert submitted["amount"] == "10"
    assert submitted["price"] == "60000.1"
    assert submitted["params"] == {
        "tdMode": "isolated",
        "posSide": "net",
        "reduceOnly": False,
        "timeInForce": "GTC",
        "clOrdId": "phase5-client-order-1",
        "tag": "phase5-command-1",
    }


@pytest.mark.asyncio
async def test_ioc_is_compiled_into_the_native_order_request() -> None:
    backend = FakeBackend()
    adapter = PerpetualContext(_settings(), backend=backend)
    await adapter.connect()

    await adapter.place_order_v2(_order(timeInForce="IOC"))

    assert backend.created[0]["params"]["timeInForce"] == "IOC"


@pytest.mark.asyncio
async def test_reduce_only_requires_close_and_integer_contract_step() -> None:
    backend = FakeBackend()
    adapter = PerpetualContext(_settings(), backend=backend)
    await adapter.connect()

    with pytest.raises(BrokerOrderError, match="declared together"):
        await adapter.place_order_v2(_order(reduceOnly=True, positionEffect="OPEN"))
    with pytest.raises(BrokerOrderError, match="broker contract step"):
        await adapter.place_order_v2(_order(quantityDecimal="1.005"))

    result = await adapter.place_order_v2(
        _order(
            side="SELL",
            quantityDecimal="10",
            reduceOnly=True,
            positionEffect="CLOSE",
        )
    )
    assert result["status"] == "SUBMITTED"
    assert backend.created[-1]["params"]["reduceOnly"] is True


@pytest.mark.asyncio
async def test_market_objects_have_target_sequence_and_distinct_types() -> None:
    adapter = PerpetualContext(_settings(), backend=FakeBackend())
    await adapter.connect()

    objects = await adapter.market_data_objects_v1()

    assert len(objects) == 6
    assert {item["objectType"] for item in objects} == {"mark", "index", "funding"}
    assert [item["sequence"] for item in objects] == [1, 2, 3, 4, 5, 6]
    assert {
        item["marketDataTargetId"] for item in objects
    } == {"okx-perpetual-demo-market-1"}
    assert {item["source"] for item in objects} == {"OKX"}
    assert all(len(item["metadataHash"]) == 64 for item in objects)
    for item in objects:
        MarketDataObjectEnvelopeV1.model_validate(item)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("object_type", "method_name", "payload", "payload_key"),
    [
        ("mark", "watch_mark_price", {"last": "60001"}, "markPriceDecimal"),
        ("index", "watch_index_price", {"last": "59991"}, "indexPriceDecimal"),
        (
            "funding",
            "watch_funding_rate",
            {
                "fundingRate": "0.0001",
                "fundingTimestamp": int(
                    (datetime.now(UTC) + timedelta(hours=8)).timestamp() * 1000
                ),
            },
            "fundingRateDecimal",
        ),
    ],
)
async def test_dedicated_market_stream_emits_one_target_scoped_object(
    object_type: str,
    method_name: str,
    payload: dict[str, object],
    payload_key: str,
) -> None:
    backend = FakeBackend()
    stream = AsyncMock(return_value=payload)
    setattr(backend, method_name, stream)
    adapter = PerpetualContext(_settings(), backend=backend)
    await adapter.connect()
    captured: list[dict[str, object]] = []

    async def handler(rows: list[dict[str, object]]) -> None:
        captured.extend(rows)
        raise asyncio.CancelledError

    adapter.set_market_data_update_handler(handler)
    with pytest.raises(asyncio.CancelledError):
        await adapter._watch_market_data("BTC/USDT:USDT", object_type)

    stream.assert_awaited_once_with("BTC/USDT:USDT")
    assert len(captured) == 1
    assert captured[0]["objectType"] == object_type
    assert captured[0]["marketDataTargetId"] == "okx-perpetual-demo-market-1"
    assert payload_key in captured[0]["payload"]
    MarketDataObjectEnvelopeV1.model_validate(captured[0])


@pytest.mark.asyncio
async def test_reduce_only_never_crosses_zero_or_closes_wrong_side() -> None:
    backend = FakeBackend()
    adapter = PerpetualContext(_settings(), backend=backend)
    await adapter.connect()

    with pytest.raises(BrokerOrderError, match="cross zero"):
        await adapter.place_order_v2(
            _order(
                side="SELL",
                quantityDecimal="11",
                reduceOnly=True,
                positionEffect="CLOSE",
            )
        )
    with pytest.raises(BrokerOrderError, match="cross zero"):
        await adapter.place_order_v2(
            _order(
                side="BUY",
                quantityDecimal="1",
                reduceOnly=True,
                positionEffect="CLOSE",
            )
        )


@pytest.mark.asyncio
async def test_cancel_is_target_and_instrument_scoped() -> None:
    backend = FakeBackend()
    adapter = PerpetualContext(_settings(), backend=backend)
    await adapter.connect()

    result = await adapter.cancel_order_v2(
        {
            "commandId": "cancel-1",
            "executionTargetId": "okx-perpetual-demo-paper-1",
            "brokerOrderId": "order-1",
            "instrumentId": "crypto-perpetual:BTC-USDT:USDT:OKX",
        }
    )
    assert result["status"] == "CANCELLED"
    assert backend.cancelled == [("order-1", "BTC/USDT:USDT")]
