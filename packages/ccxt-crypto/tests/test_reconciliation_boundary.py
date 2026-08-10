from __future__ import annotations

import asyncio
from copy import deepcopy

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


def _private_read_settings() -> CCXTCryptoSettings:
    return CCXTCryptoSettings.from_mapping(
        settings(
            private_read_enabled=True,
            api_key="key",
            secret="secret",
            passphrase="passphrase",
        )
    )


def test_reconciliation_ignores_okx_demo_default_okb_balance() -> None:
    result = asyncio.run(Reconciler(_OKXDefaultAssetClient(), _private_read_settings()).snapshot())

    assert {item["currency"] for item in result["balances"]} == {"BTC", "ETH", "USDT"}


def test_reconciliation_reports_other_unexpected_non_zero_asset_as_safe_probe_failure() -> None:
    with pytest.raises(
        BrokerConnectionError,
        match="non-zero asset outside BTC/ETH/USDT",
    ) as raised:
        asyncio.run(Reconciler(_UnexpectedAssetClient(), _private_read_settings()).snapshot())

    assert raised.value.details == {"currency": "SOL"}
