from __future__ import annotations

import asyncio

import pytest
from algo_trader_broker_adapter_ccxt_crypto.client import OKXDemoClient, _exchange_config
from algo_trader_broker_adapter_ccxt_crypto.settings import CCXTCryptoSettings
from algo_trader_broker_sdk import BrokerContractError

from .fakes import settings


class FakeExchange:
    def __init__(
        self,
        *,
        hostname="www.okx.com",
        rest_url="https://www.okx.com",
        test_ws="wss://wspap.okx.com:8443/ws/v5/private",
    ) -> None:
        self.hostname = hostname
        self.options = {}
        self.headers = {}
        self.urls = {
            "api": {"rest": rest_url, "ws": "wss://ws.okx.com:8443/ws/v5/public"},
            "test": {"ws": test_ws},
        }
        self.sandbox_calls = 0

    def set_sandbox_mode(self, enabled):
        self.sandbox_calls += 1
        self.options["sandboxMode"] = enabled
        self.headers["x-simulated-trading"] = "1"


class NarrowMarketsExchange(FakeExchange):
    def __init__(self) -> None:
        super().__init__()
        self.market_requests = []

    async def fetch_markets(self, params):
        self.market_requests.append(dict(params))
        symbol = params["instId"].replace("-", "/")
        return [{"symbol": symbol}]

    def set_markets(self, markets):
        return {market["symbol"]: market for market in markets}


def parsed_settings():
    return CCXTCryptoSettings.from_mapping(settings(public_data_enabled=False))


def test_client_enables_and_verifies_okx_demo_before_io() -> None:
    exchange = FakeExchange()
    client = OKXDemoClient(parsed_settings(), exchange=exchange)
    assert exchange.sandbox_calls == 1
    assert client.sandbox_evidence()["simulatedTradingHeader"] is True


def test_client_resolves_ccxt_hostname_template_before_allowlist_check() -> None:
    client = OKXDemoClient(
        parsed_settings(),
        exchange=FakeExchange(rest_url="https://{hostname}"),
    )

    assert client.sandbox_evidence()["restHostsApproved"] is True


def test_ccxt_market_discovery_is_restricted_to_spot() -> None:
    config = _exchange_config(parsed_settings())

    assert config["options"] == {
        "defaultType": "spot",
        "fetchMarkets": {"types": ["spot"]},
    }
    assert config["timeout"] == 30000


def test_market_discovery_requests_only_allowlisted_instrument_ids() -> None:
    exchange = NarrowMarketsExchange()
    client = OKXDemoClient(parsed_settings(), exchange=exchange)

    markets = asyncio.run(client.load_markets(("BTC/USDT", "ETH/USDT")))

    assert exchange.market_requests == [
        {"instId": "BTC-USDT"},
        {"instId": "ETH-USDT"},
    ]
    assert set(markets) == {"BTC/USDT", "ETH/USDT"}


def test_client_rejects_unapproved_ccxt_hostname_template() -> None:
    with pytest.raises(BrokerContractError, match="outside the approved allowlist"):
        OKXDemoClient(
            parsed_settings(),
            exchange=FakeExchange(
                hostname="attacker.example",
                rest_url="https://{hostname}",
            ),
        )


def test_client_rejects_production_websocket_host() -> None:
    with pytest.raises(BrokerContractError, match="must be wspap"):
        OKXDemoClient(
            parsed_settings(),
            exchange=FakeExchange(test_ws="wss://ws.okx.com:8443/ws/v5/private"),
        )
