from __future__ import annotations

import asyncio
from copy import deepcopy
from decimal import Decimal

import pytest
from algo_trader_broker_adapter_ccxt_crypto.reconciliation import Reconciler
from algo_trader_broker_adapter_ccxt_crypto.settings import CCXTCryptoSettings
from algo_trader_broker_sdk import BrokerConnectionError

from .fakes import BALANCE, FakeClient, settings


class _OKXDefaultAssetClient(FakeClient):
    async def fetch_balance(self):
        balance = deepcopy(BALANCE)
        balance["info"]["data"][0]["details"].append(
            {
                "ccy": "OKB",
                "cashBal": "1",
                "availBal": "1",
                "frozenBal": "0",
                "liab": "0",
            }
        )
        return balance


class _UnexpectedAssetClient(_OKXDefaultAssetClient):
    async def fetch_balance(self):
        balance = await super().fetch_balance()
        balance["info"]["data"][0]["details"].append(
            {
                "ccy": "SOL",
                "cashBal": "1",
                "availBal": "1",
                "frozenBal": "0",
                "liab": "0",
            }
        )
        return balance


class _ExistingSpotAssetsClient(FakeClient):
    async def fetch_balance(self):
        balance = deepcopy(BALANCE)
        for item in balance["info"]["data"][0]["details"]:
            if item["ccy"] in {"BTC", "ETH"}:
                item["cashBal"] = "1"
                item["availBal"] = "1"
        balance["total"].update({"BTC": "1", "ETH": "1"})
        balance["free"].update({"BTC": "1", "ETH": "1"})
        return balance


class _LiabilityClient(FakeClient):
    async def fetch_balance(self):
        balance = deepcopy(BALANCE)
        balance["info"]["data"][0]["details"][0]["liab"] = "0.1"
        return balance


class _ExternalBaselineRoundTripClient(_ExistingSpotAssetsClient):
    def __init__(self) -> None:
        super().__init__()
        self.eth_balance = "1"
        self.trades = []

    async def fetch_balance(self):
        balance = await super().fetch_balance()
        for item in balance["info"]["data"][0]["details"]:
            if item["ccy"] == "ETH":
                item["cashBal"] = self.eth_balance
                item["availBal"] = self.eth_balance
        balance["total"]["ETH"] = self.eth_balance
        balance["free"]["ETH"] = self.eth_balance
        return balance

    async def fetch_my_trades(self, symbol):
        return self.trades if symbol == "ETH/USDT" else []


def _private_read_settings() -> CCXTCryptoSettings:
    return CCXTCryptoSettings.from_mapping(
        settings(
            api_key="key",
            secret="secret",
            passphrase="passphrase",
        )
    )


def test_reconciliation_ignores_okx_demo_default_okb_balance() -> None:
    result = asyncio.run(Reconciler(_OKXDefaultAssetClient(), _private_read_settings()).snapshot())

    assert {item["currency"] for item in result["balances"]} == {"BTC", "ETH", "USDT"}


def test_reconciliation_accepts_existing_spot_assets_with_a_valuation_baseline() -> None:
    client = _ExistingSpotAssetsClient()
    reconciler = Reconciler(client, _private_read_settings())

    async def snapshots():
        return await reconciler.snapshot(), await reconciler.snapshot()

    result, repeated = asyncio.run(snapshots())

    nonzero = [item for item in result["positions"] if item["quantityDecimal"] != "0"]
    assert len(nonzero) == 2
    assert all(item["averagePriceDecimal"] == "10000" for item in nonzero)
    assert all(item["markPriceDecimal"] == "10000" for item in nonzero)
    assert all(item["positionGroupId"] == "external-asset-baseline" for item in nonzero)
    assert [item["averagePriceDecimal"] for item in repeated["positions"]] == [
        item["averagePriceDecimal"] for item in result["positions"]
    ]
    assert [call for call in client.calls if call[0] == "fetch_ticker"] == [
        ("fetch_ticker", "BTC/USDT"),
        ("fetch_ticker", "ETH/USDT"),
    ]


def test_reconciliation_restores_external_cost_basis_after_increment_round_trip() -> None:
    client = _ExternalBaselineRoundTripClient()
    reconciler = Reconciler(client, _private_read_settings())

    async def snapshots():
        baseline = await reconciler.snapshot()
        client.eth_balance = "1.002997"
        client.trades = [
            {"id": "buy", "symbol": "ETH/USDT", "side": "buy", "amount": "0.003", "price": "1879.06", "fee": {"currency": "ETH", "cost": "0.000003"}},
        ]
        increased = await reconciler.snapshot()
        client.eth_balance = "1"
        client.trades.append(
            {"id": "sell", "symbol": "ETH/USDT", "side": "sell", "amount": "0.002997", "price": "1878.73", "fee": {"currency": "USDT", "cost": "0.00563055381"}},
        )
        restored = await reconciler.snapshot()
        return baseline, increased, restored

    baseline, increased, restored = asyncio.run(snapshots())
    baseline_eth = next(item for item in baseline["positions"] if item["instrumentId"].startswith("crypto-spot:ETH"))
    increased_eth = next(item for item in increased["positions"] if item["instrumentId"].startswith("crypto-spot:ETH"))
    restored_eth = next(item for item in restored["positions"] if item["instrumentId"].startswith("crypto-spot:ETH"))

    assert baseline_eth["averagePriceDecimal"] == "10000"
    assert Decimal(increased_eth["averagePriceDecimal"]) > Decimal("10000") - Decimal("25")
    assert restored_eth["quantityDecimal"] == "1"
    assert restored_eth["averagePriceDecimal"] == "10000"
    assert restored_eth["positionGroupId"] == "external-asset-baseline"


def test_reconciliation_ignores_holdings_outside_the_supported_asset_universe() -> None:
    result = asyncio.run(Reconciler(_UnexpectedAssetClient(), _private_read_settings()).snapshot())

    assert {item["currency"] for item in result["balances"]} == {"BTC", "ETH", "USDT"}


def test_reconciliation_still_rejects_spot_liabilities(caplog) -> None:
    with pytest.raises(
        BrokerConnectionError,
        match="non-zero liability",
    ) as raised:
        asyncio.run(Reconciler(_LiabilityClient(), _private_read_settings()).snapshot())

    assert raised.value.details == {"currency": "BTC"}
    assert any(
        getattr(record, "event", None) == "broker.crypto.balance_drift"
        for record in caplog.records
    )
