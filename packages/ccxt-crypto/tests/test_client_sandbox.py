from __future__ import annotations

import pytest
from algo_trader_broker_adapter_ccxt_crypto.client import OKXDemoClient
from algo_trader_broker_adapter_ccxt_crypto.settings import CCXTCryptoSettings
from algo_trader_broker_sdk import BrokerContractError

from .fakes import settings


class FakeExchange:
    def __init__(self, *, test_ws="wss://wspap.okx.com:8443/ws/v5/private") -> None:
        self.options = {}
        self.headers = {}
        self.urls = {
            "api": {"rest": "https://www.okx.com", "ws": "wss://ws.okx.com:8443/ws/v5/public"},
            "test": {"ws": test_ws},
        }
        self.sandbox_calls = 0

    def set_sandbox_mode(self, enabled):
        self.sandbox_calls += 1
        self.options["sandboxMode"] = enabled
        self.headers["x-simulated-trading"] = "1"


def parsed_settings():
    return CCXTCryptoSettings.from_mapping(settings(public_data_enabled=False))


def test_client_enables_and_verifies_okx_demo_before_io() -> None:
    exchange = FakeExchange()
    client = OKXDemoClient(parsed_settings(), exchange=exchange)
    assert exchange.sandbox_calls == 1
    assert client.sandbox_evidence()["simulatedTradingHeader"] is True


def test_client_rejects_production_websocket_host() -> None:
    with pytest.raises(BrokerContractError, match="must be wspap"):
        OKXDemoClient(
            parsed_settings(),
            exchange=FakeExchange(test_ws="wss://ws.okx.com:8443/ws/v5/private"),
        )
