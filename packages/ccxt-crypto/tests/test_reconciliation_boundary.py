from __future__ import annotations

import asyncio
from copy import deepcopy

import pytest
from algo_trader_broker_adapter_ccxt_crypto.reconciliation import Reconciler
from algo_trader_broker_adapter_ccxt_crypto.settings import CCXTCryptoSettings
from algo_trader_broker_sdk import BrokerConnectionError

from .fakes import BALANCE, FakeClient, settings


class _UnexpectedAssetClient(FakeClient):
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


def test_reconciliation_reports_unexpected_non_zero_asset_as_safe_probe_failure() -> None:
    parsed = CCXTCryptoSettings.from_mapping(
        settings(
            private_read_enabled=True,
            api_key="key",
            secret="secret",
            passphrase="passphrase",
        )
    )

    with pytest.raises(
        BrokerConnectionError,
        match="non-zero asset outside BTC/ETH/USDT",
    ) as raised:
        asyncio.run(Reconciler(_UnexpectedAssetClient(), parsed).snapshot())

    assert raised.value.details == {"currency": "OKB"}
