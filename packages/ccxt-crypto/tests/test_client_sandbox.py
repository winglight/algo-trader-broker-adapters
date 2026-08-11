from __future__ import annotations

import asyncio

import pytest
from algo_trader_broker_adapter_ccxt_crypto.client import OKXDemoClient, _exchange_config
from algo_trader_broker_adapter_ccxt_crypto.settings import CCXTCryptoSettings
from algo_trader_broker_sdk import BrokerConnectionError, BrokerContractError

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
        self.closed = False

    def set_sandbox_mode(self, enabled):
        self.sandbox_calls += 1
        self.options["sandboxMode"] = enabled
        self.headers["x-simulated-trading"] = "1"

    async def close(self):
        self.closed = True


class NarrowMarketsExchange(FakeExchange):
    def __init__(self) -> None:
        super().__init__()
        self.market_requests = []

    async def fetch_markets(self, params):
        self.market_requests.append(dict(params))
        return [{"id": "BTC-USDT", "symbol": "BTC/USDT"}, {"id": "ETH-USDT", "symbol": "ETH/USDT"}]

    def set_markets(self, markets):
        return {market["symbol"]: market for market in markets}


class RateLimitedExchange(FakeExchange):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0

    async def fetch_time(self):
        self.attempts += 1
        if self.attempts < 3:
            raise RuntimeError("OKX 50011 rate limit reached")
        return 1786320000000


class TransientTimeoutExchange(FakeExchange):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0

    async def fetch_time(self):
        self.attempts += 1
        if self.attempts < 3:
            raise TimeoutError
        return 1786320000000


class MaintenanceExchange(FakeExchange):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0

    async def fetch_time(self):
        self.attempts += 1
        if self.attempts < 3:
            raise RuntimeError(
                'okx {"msg":"Service temporarily unavailable. Please try again later.","code":"50001"}'
            )
        return 1786320000000


class RecoveringWebsocketExchange(FakeExchange):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0
        self.close_calls = 0

    async def watch_ohlcv(self, _symbol, _timeframe):
        self.attempts += 1
        if self.attempts == 1:
            raise TimeoutError("ping-pong keepalive missing on time")
        return [[1786262400000, "1", "2", "0.5", "1.5", "10"]]

    async def close(self):
        self.close_calls += 1


class ConcurrentTimeoutWebsocketExchange(RecoveringWebsocketExchange):
    def __init__(self) -> None:
        super().__init__()
        self.arrived = 0
        self.release = asyncio.Event()

    async def watch_ohlcv(self, _symbol, _timeframe):
        self.attempts += 1
        if self.attempts <= 2:
            self.arrived += 1
            if self.arrived == 2:
                self.release.set()
            await self.release.wait()
            raise TimeoutError("connection timeout")
        return [[1786262400000, "1", "2", "0.5", "1.5", "10"]]


def parsed_settings():
    return CCXTCryptoSettings.from_mapping(settings())


def test_client_enables_and_verifies_okx_demo_before_io(caplog) -> None:
    caplog.set_level("INFO")
    exchange = FakeExchange()
    client = OKXDemoClient(parsed_settings(), exchange=exchange)
    assert exchange.sandbox_calls == 1
    assert client.sandbox_evidence()["simulatedTradingHeader"] is True
    assert any(
        getattr(record, "event", None) == "broker.crypto.sandbox_host_verified"
        for record in caplog.records
    )


def test_client_isolates_rest_and_websocket_boundaries() -> None:
    rest_exchange = FakeExchange()
    websocket_exchange = FakeExchange()
    client = OKXDemoClient(
        parsed_settings(),
        exchange=rest_exchange,
        ws_exchange=websocket_exchange,
    )

    assert client.exchange is rest_exchange
    assert client.ws_exchange is websocket_exchange
    assert rest_exchange.sandbox_calls == 1
    assert websocket_exchange.sandbox_calls == 1

    asyncio.run(client.close())

    assert rest_exchange.closed is True
    assert websocket_exchange.closed is True


def test_read_rate_limit_is_logged_and_retried(caplog) -> None:
    exchange = RateLimitedExchange()
    client = OKXDemoClient(parsed_settings(), exchange=exchange)

    assert asyncio.run(client.fetch_time()) == 1786320000000
    events = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "broker.crypto.rate_limited"
    ]
    assert len(events) == 2
    assert exchange.attempts == 3


def test_transient_read_timeout_is_logged_and_retried(caplog) -> None:
    exchange = TransientTimeoutExchange()
    client = OKXDemoClient(parsed_settings(), exchange=exchange)

    assert asyncio.run(client.fetch_time()) == 1786320000000
    events = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "broker.crypto.read_retry"
    ]
    assert len(events) == 2
    assert exchange.attempts == 3


def test_okx_50001_maintenance_read_is_logged_and_retried(caplog) -> None:
    exchange = MaintenanceExchange()
    client = OKXDemoClient(parsed_settings(), exchange=exchange)

    assert asyncio.run(client.fetch_time()) == 1786320000000
    events = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "broker.crypto.read_retry"
    ]
    assert len(events) == 2
    assert exchange.attempts == 3


@pytest.mark.asyncio
async def test_transient_websocket_timeout_resets_connection_before_retry(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest_exchange = FakeExchange()
    websocket_exchange = RecoveringWebsocketExchange()
    client = OKXDemoClient(
        parsed_settings(),
        exchange=rest_exchange,
        ws_exchange=websocket_exchange,
    )

    with pytest.raises(BrokerConnectionError) as error:
        await client.watch_ohlcv("BTC/USDT", "1m")

    assert error.value.details == {
        "operation": "watch_ohlcv",
        "error_type": "TimeoutError",
        "websocketGeneration": 0,
        "resetPerformed": True,
        "nextWebsocketGeneration": 1,
    }
    assert websocket_exchange.close_calls == 1
    assert await client.watch_ohlcv("BTC/USDT", "1m") == [
        [1786262400000, "1", "2", "0.5", "1.5", "10"]
    ]
    reset_records = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "broker.crypto.websocket_reset"
    ]
    assert len(reset_records) == 1


@pytest.mark.asyncio
async def test_concurrent_websocket_timeouts_reset_failed_generation_once() -> None:
    websocket_exchange = ConcurrentTimeoutWebsocketExchange()
    client = OKXDemoClient(
        parsed_settings(),
        exchange=FakeExchange(),
        ws_exchange=websocket_exchange,
    )

    failures = await asyncio.gather(
        client.watch_ohlcv("BTC/USDT", "1m"),
        client.watch_ohlcv("ETH/USDT", "1m"),
        return_exceptions=True,
    )

    assert all(isinstance(item, BrokerConnectionError) for item in failures)
    assert sorted(
        bool(item.details["resetPerformed"])  # type: ignore[union-attr]
        for item in failures
    ) == [False, True]
    assert websocket_exchange.close_calls == 1
    assert await client.watch_ohlcv("BTC/USDT", "1m")


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
        "adjustForTimeDifference": True,
    }
    assert config["timeout"] == 30000


def test_market_discovery_loads_spot_once_and_keeps_only_allowlisted_instruments() -> None:
    exchange = NarrowMarketsExchange()
    client = OKXDemoClient(parsed_settings(), exchange=exchange)

    markets = asyncio.run(client.load_markets(("BTC/USDT", "ETH/USDT")))

    assert exchange.market_requests == [{"instType": "SPOT"}]
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
