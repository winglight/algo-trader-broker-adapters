"""Narrow CCXT Pro client boundary fixed to OKX Demo Trading."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlparse

from algo_trader_broker_sdk import BrokerConnectionError, BrokerContractError

from .settings import CCXTCryptoSettings

_REST_HOSTS = {"www.okx.com", "openapi.okx.com"}
_DEMO_WS_HOST = "wspap.okx.com"
_PRODUCTION_WS_HOST = "ws.okx.com"


def _hosts(value: Any) -> set[str]:
    result: set[str] = set()
    if isinstance(value, Mapping):
        for item in value.values():
            result.update(_hosts(item))
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            result.update(_hosts(item))
    elif isinstance(value, str) and "://" in value:
        host = (urlparse(value).hostname or "").lower()
        if host:
            result.add(host)
    return result


class OKXDemoClient:
    """Owns one exchange instance and applies all sandbox guards before I/O."""

    def __init__(self, settings: CCXTCryptoSettings, *, exchange: Any | None = None) -> None:
        self.settings = settings
        self._semaphore = asyncio.Semaphore(settings.rest_max_concurrency)
        if exchange is None:
            try:
                import ccxt.pro as ccxtpro
            except ImportError as exc:  # pragma: no cover - installation failure path
                raise BrokerConnectionError("ccxt.pro is not installed") from exc
            config: dict[str, Any] = {
                "enableRateLimit": True,
                "options": {"defaultType": "spot"},
            }
            if settings.api_key:
                config.update(
                    {
                        "apiKey": settings.api_key,
                        "secret": settings.secret,
                        "password": settings.passphrase,
                    }
                )
            exchange = ccxtpro.okx(config)
        self.exchange = exchange
        self.exchange.set_sandbox_mode(True)
        self._verify_sandbox()

    def _verify_sandbox(self) -> None:
        options = getattr(self.exchange, "options", {}) or {}
        headers = getattr(self.exchange, "headers", {}) or {}
        if options.get("sandboxMode") is not True:
            raise BrokerContractError("CCXT OKX sandboxMode was not enabled")
        simulated = next(
            (
                str(value)
                for key, value in headers.items()
                if str(key).strip().lower() == "x-simulated-trading"
            ),
            "",
        )
        if simulated != "1":
            raise BrokerContractError("OKX Demo REST header x-simulated-trading=1 is required")
        urls = getattr(self.exchange, "urls", {}) or {}
        api_hosts = _hosts(urls.get("api") if isinstance(urls, Mapping) else {})
        rest_hosts = api_hosts & _REST_HOSTS
        if not rest_hosts:
            raise BrokerContractError(
                "CCXT OKX REST host is outside the approved allowlist",
                details={"approved": sorted(_REST_HOSTS)},
            )
        test_hosts = _hosts(urls.get("test") if isinstance(urls, Mapping) else {})
        if _DEMO_WS_HOST not in test_hosts or _PRODUCTION_WS_HOST in test_hosts:
            raise BrokerContractError("OKX Demo WebSocket host must be wspap.okx.com")

    async def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        async with self._semaphore:
            return await getattr(self.exchange, method)(*args, **kwargs)

    async def _read(self, method: str, *args: Any, **kwargs: Any) -> Any:
        """Bounded retry for OKX rate limiting; never used by mutations."""

        for attempt in range(3):
            try:
                return await self._call(method, *args, **kwargs)
            except Exception as exc:
                if "50011" not in str(exc) or attempt == 2:
                    raise
                await asyncio.sleep(0.25 * (2**attempt))
        raise AssertionError("unreachable")

    async def load_markets(self) -> Mapping[str, Any]:
        return await self._read("load_markets")

    async def fetch_time(self) -> int:
        return int(await self._read("fetch_time"))

    async def fetch_balance(self) -> Mapping[str, Any]:
        return await self._read("fetch_balance", {"type": "spot"})

    async def fetch_ticker(self, symbol: str) -> Mapping[str, Any]:
        return await self._read("fetch_ticker", symbol)

    async def fetch_trading_fee(self, symbol: str) -> Mapping[str, Any]:
        return await self._read("fetch_trading_fee", symbol)

    async def fetch_ohlcv(
        self, symbol: str, timeframe: str, since: int | None = None, limit: int | None = None
    ) -> list[list[Any]]:
        return await self._read("fetch_ohlcv", symbol, timeframe, since, limit)

    async def create_order(
        self,
        symbol: str,
        order_type: str,
        side: str,
        amount: str,
        price: str | None,
        params: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        return await self._call(
            "create_order", symbol, order_type, side, amount, price, dict(params)
        )

    async def cancel_order(self, order_id: str, symbol: str) -> Mapping[str, Any]:
        return await self._call("cancel_order", order_id, symbol, {"tdMode": "cash"})

    async def fetch_order(self, order_id: str, symbol: str) -> Mapping[str, Any]:
        return await self._read("fetch_order", order_id, symbol)

    async def fetch_order_by_client_id(
        self, client_order_id: str, symbol: str
    ) -> Mapping[str, Any] | None:
        open_orders = await self.fetch_open_orders(symbol)
        closed_orders = await self.fetch_closed_orders(symbol)
        for order in [*open_orders, *closed_orders]:
            value = str(
                order.get("clientOrderId")
                or (order.get("info") or {}).get("clOrdId")
                or ""
            )
            if value == client_order_id:
                return order
        return None

    async def fetch_open_orders(self, symbol: str) -> list[Mapping[str, Any]]:
        return list(await self._read("fetch_open_orders", symbol))

    async def fetch_closed_orders(self, symbol: str) -> list[Mapping[str, Any]]:
        return list(await self._read("fetch_closed_orders", symbol, None, 100))

    async def fetch_my_trades(self, symbol: str) -> list[Mapping[str, Any]]:
        return list(await self._read("fetch_my_trades", symbol, None, 100))

    async def watch_ticker(self, symbol: str) -> Mapping[str, Any]:
        return await self.exchange.watch_ticker(symbol)

    async def watch_trades(self, symbol: str) -> list[Mapping[str, Any]]:
        return list(await self.exchange.watch_trades(symbol))

    async def watch_ohlcv(self, symbol: str, timeframe: str) -> list[list[Any]]:
        return list(await self.exchange.watch_ohlcv(symbol, timeframe))

    async def watch_orders(self) -> list[Mapping[str, Any]]:
        return list(await self.exchange.watch_orders())

    async def watch_my_trades(self) -> list[Mapping[str, Any]]:
        return list(await self.exchange.watch_my_trades())

    async def watch_balance(self) -> Mapping[str, Any]:
        return await self.exchange.watch_balance({"type": "spot"})

    async def close(self) -> None:
        await self.exchange.close()

    def sandbox_evidence(self) -> dict[str, Any]:
        urls = getattr(self.exchange, "urls", {}) or {}
        api_hosts = _hosts(urls.get("api") if isinstance(urls, Mapping) else {})
        test_hosts = _hosts(urls.get("test") if isinstance(urls, Mapping) else {})
        return {
            "sandboxMode": True,
            "simulatedTradingHeader": True,
            "restHostsApproved": bool(api_hosts & _REST_HOSTS),
            "demoWebsocket": _DEMO_WS_HOST in test_hosts and _PRODUCTION_WS_HOST not in test_hosts,
        }
