"""Narrow CCXT Pro client boundary fixed to OKX Demo Trading."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlparse

from algo_trader_broker_sdk import BrokerConnectionError, BrokerContractError, BrokerError

from .settings import CCXTCryptoSettings

_REST_HOSTS = {"www.okx.com", "openapi.okx.com"}
_DEMO_WS_HOST = "wspap.okx.com"
_PRODUCTION_WS_HOST = "ws.okx.com"
LOGGER = logging.getLogger(__name__)


def _exchange_config(settings: CCXTCryptoSettings) -> dict[str, Any]:
    return {
        "enableRateLimit": True,
        "timeout": settings.request_timeout_ms,
        "options": {
            "defaultType": "spot",
            "fetchMarkets": {"types": ["spot"]},
            "adjustForTimeDifference": True,
        },
        **(
            {
                "apiKey": settings.api_key,
                "secret": settings.secret,
                "password": settings.passphrase,
            }
            if settings.api_key
            else {}
        ),
    }


def _hosts(value: Any, *, hostname: str = "") -> set[str]:
    result: set[str] = set()
    if isinstance(value, Mapping):
        for item in value.values():
            result.update(_hosts(item, hostname=hostname))
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            result.update(_hosts(item, hostname=hostname))
    elif isinstance(value, str) and "://" in value:
        resolved = value.replace("{hostname}", hostname) if hostname else value
        host = (urlparse(resolved).hostname or "").lower()
        if host:
            result.add(host)
    return result


class OKXDemoClient:
    """Own isolated REST/WS exchanges and apply sandbox guards before I/O."""

    def __init__(
        self,
        settings: CCXTCryptoSettings,
        *,
        exchange: Any | None = None,
        ws_exchange: Any | None = None,
    ) -> None:
        self.settings = settings
        self._semaphore = asyncio.Semaphore(settings.rest_max_concurrency)
        self._websocket_reset_lock = asyncio.Lock()
        self._websocket_generation = 0
        if exchange is None:
            try:
                import ccxt.async_support as ccxtasync
                import ccxt.pro as ccxtpro
            except ImportError as exc:  # pragma: no cover - installation failure path
                raise BrokerConnectionError("ccxt async/pro is not installed") from exc
            exchange = ccxtasync.okx(_exchange_config(settings))
            ws_exchange = ccxtpro.okx(_exchange_config(settings))
        elif ws_exchange is None:
            # Preserve the narrow fake-injection contract used by unit tests and
            # downstream adapters. Production always constructs isolated clients.
            ws_exchange = exchange
        self.exchange = exchange
        self.ws_exchange = ws_exchange
        self.exchange.set_sandbox_mode(True)
        if self.ws_exchange is not self.exchange:
            self.ws_exchange.set_sandbox_mode(True)
        self._verify_sandbox()
        LOGGER.warning(
            "OKX Demo sandbox host and simulated-trading boundary verified",
            extra={
                "event": "broker.crypto.sandbox_host_verified",
                "broker.adapter_id": "ccxt_crypto",
                "broker.exchange_id": "okx",
                "broker.environment": "PAPER",
            },
        )

    def _verify_sandbox(self) -> None:
        for boundary, exchange in (("REST", self.exchange), ("WebSocket", self.ws_exchange)):
            options = getattr(exchange, "options", {}) or {}
            if options.get("sandboxMode") is not True:
                raise BrokerContractError(f"CCXT OKX {boundary} sandboxMode was not enabled")
        headers = getattr(self.exchange, "headers", {}) or {}
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
        hostname = str(getattr(self.exchange, "hostname", "") or "").strip().lower()
        api_hosts = _hosts(
            urls.get("api") if isinstance(urls, Mapping) else {},
            hostname=hostname,
        )
        rest_hosts = api_hosts & _REST_HOSTS
        if not rest_hosts:
            raise BrokerContractError(
                "CCXT OKX REST host is outside the approved allowlist",
                details={"approved": sorted(_REST_HOSTS)},
            )
        ws_urls = getattr(self.ws_exchange, "urls", {}) or {}
        ws_hostname = str(getattr(self.ws_exchange, "hostname", "") or "").strip().lower()
        test_hosts = _hosts(
            ws_urls.get("test") if isinstance(ws_urls, Mapping) else {},
            hostname=ws_hostname,
        )
        if _DEMO_WS_HOST not in test_hosts or _PRODUCTION_WS_HOST in test_hosts:
            raise BrokerContractError("OKX Demo WebSocket host must be wspap.okx.com")

    async def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        async with self._semaphore:
            try:
                return await getattr(self.exchange, method)(*args, **kwargs)
            except BrokerError:
                raise
            except Exception as exc:
                raise BrokerConnectionError(
                    f"OKX Demo {method} request failed",
                    details={"operation": method, "error_type": type(exc).__name__},
                ) from exc

    async def _read(self, method: str, *args: Any, **kwargs: Any) -> Any:
        """Bounded retry for transient OKX read failures; never used by mutations."""

        for attempt in range(3):
            try:
                return await self._call(method, *args, **kwargs)
            except Exception as exc:
                rate_limited = self._is_rate_limited(exc)
                transient = self._is_transient_read_failure(exc)
                if not rate_limited and not transient:
                    raise
                LOGGER.warning(
                    "OKX Demo read request will be retried",
                    extra={
                        "event": (
                            "broker.crypto.rate_limited"
                            if rate_limited
                            else "broker.crypto.read_retry"
                        ),
                        "broker.adapter_id": "ccxt_crypto",
                        "broker.operation": method,
                        "broker.retry_attempt": attempt + 1,
                        "broker.error_type": self._error_type(exc),
                    },
                )
                if attempt == 2:
                    raise
                await asyncio.sleep((0.25 if rate_limited else 1.0) * (2**attempt))
        raise AssertionError("unreachable")

    @staticmethod
    def _error_type(exc: Exception) -> str:
        details = getattr(exc, "details", None)
        if isinstance(details, Mapping):
            value = str(details.get("error_type") or "").strip()
            if value:
                return value
        return type(exc).__name__

    @classmethod
    def _is_transient_read_failure(cls, exc: Exception) -> bool:
        error_type = cls._error_type(exc).lower()
        current: BaseException | None = exc
        chain: list[str] = []
        visited: set[int] = set()
        while current is not None and id(current) not in visited:
            visited.add(id(current))
            chain.append(f"{type(current).__name__} {current}".lower())
            current = current.__cause__ or current.__context__
        evidence = " ".join((error_type, *chain))
        return any(
            token in evidence
            for token in (
                "timeout",
                "network",
                "unavailable",
                "disconnect",
                "onmaintenance",
                "temporarily unavailable",
                '"code":"50001"',
            )
        )

    @staticmethod
    def _is_rate_limited(exc: Exception) -> bool:
        current: BaseException | None = exc
        visited: set[int] = set()
        while current is not None and id(current) not in visited:
            visited.add(id(current))
            details = getattr(current, "details", None)
            error_type = (
                str(details.get("error_type") or "")
                if isinstance(details, Mapping)
                else ""
            )
            if "50011" in str(current) or "ratelimit" in error_type.lower():
                return True
            current = current.__cause__ or current.__context__
        return False

    async def load_markets(self, symbols: tuple[str, ...]) -> Mapping[str, Any]:
        requested = tuple(dict.fromkeys(symbols))
        markets = await self._read(
            "fetch_markets",
            {"instType": "SPOT"},
        )
        requested_ids = {symbol.replace("/", "-") for symbol in requested}
        selected = [
            market
            for market in markets
            if str(market.get("symbol") or "") in requested
            or str(market.get("id") or "") in requested_ids
        ]
        result = self.exchange.set_markets(selected)
        if self.ws_exchange is not self.exchange:
            self.ws_exchange.set_markets(selected)
        return result

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
        return await self._watch("watch_ticker", symbol)

    async def watch_trades(self, symbol: str) -> list[Mapping[str, Any]]:
        return list(await self._watch("watch_trades", symbol))

    async def watch_ohlcv(self, symbol: str, timeframe: str) -> list[list[Any]]:
        return list(await self._watch("watch_ohlcv", symbol, timeframe))

    async def watch_orders(self) -> list[Mapping[str, Any]]:
        return list(await self._watch("watch_orders"))

    async def watch_my_trades(self) -> list[Mapping[str, Any]]:
        return list(await self._watch("watch_my_trades"))

    async def watch_balance(self) -> Mapping[str, Any]:
        return await self._watch("watch_balance", {"type": "spot"})

    async def _watch(self, method: str, *args: Any) -> Any:
        generation = self._websocket_generation
        try:
            return await getattr(self.ws_exchange, method)(*args)
        except BrokerError:
            raise
        except Exception as exc:
            if not self._is_transient_websocket_failure(exc):
                raise BrokerConnectionError(
                    f"OKX Demo {method} websocket request failed",
                    details={
                        "operation": method,
                        "error_type": type(exc).__name__,
                        "websocketGeneration": generation,
                    },
                ) from exc
            reset = await self._reset_websocket(
                failed_generation=generation,
                operation=method,
                error=exc,
            )
            raise BrokerConnectionError(
                f"OKX Demo {method} websocket connection was reset after a transient failure",
                details={
                    "operation": method,
                    "error_type": type(exc).__name__,
                    "websocketGeneration": generation,
                    "resetPerformed": reset,
                    "nextWebsocketGeneration": self._websocket_generation,
                },
            ) from exc

    async def _reset_websocket(
        self,
        *,
        failed_generation: int,
        operation: str,
        error: Exception,
    ) -> bool:
        async with self._websocket_reset_lock:
            if failed_generation != self._websocket_generation:
                return False
            try:
                await self.ws_exchange.close()
            except Exception as close_error:
                raise BrokerConnectionError(
                    "OKX Demo websocket reset failed",
                    details={
                        "operation": operation,
                        "error_type": type(error).__name__,
                        "close_error_type": type(close_error).__name__,
                        "websocketGeneration": failed_generation,
                    },
                ) from close_error
            self._websocket_generation += 1
            LOGGER.warning(
                "OKX Demo websocket connection reset after transient failure",
                extra={
                    "event": "broker.crypto.websocket_reset",
                    "broker.adapter_id": "ccxt_crypto",
                    "broker.operation": operation,
                    "broker.error_type": type(error).__name__,
                    "broker.websocket_generation": self._websocket_generation,
                },
            )
            return True

    @staticmethod
    def _is_transient_websocket_failure(exc: Exception) -> bool:
        value = f"{type(exc).__name__} {exc}".lower()
        return any(
            token in value
            for token in (
                "timeout",
                "ping-pong",
                "keepalive",
                "network",
                "connectionclosed",
                "connection closed",
                "disconnect",
                "temporarily unavailable",
            )
        )

    async def close(self) -> None:
        await self.exchange.close()
        if self.ws_exchange is not self.exchange:
            await self.ws_exchange.close()

    def sandbox_evidence(self) -> dict[str, Any]:
        urls = getattr(self.exchange, "urls", {}) or {}
        hostname = str(getattr(self.exchange, "hostname", "") or "").strip().lower()
        api_hosts = _hosts(
            urls.get("api") if isinstance(urls, Mapping) else {},
            hostname=hostname,
        )
        ws_urls = getattr(self.ws_exchange, "urls", {}) or {}
        ws_hostname = str(getattr(self.ws_exchange, "hostname", "") or "").strip().lower()
        test_hosts = _hosts(
            ws_urls.get("test") if isinstance(ws_urls, Mapping) else {},
            hostname=ws_hostname,
        )
        return {
            "sandboxMode": True,
            "simulatedTradingHeader": True,
            "restHostsApproved": bool(api_hosts & _REST_HOSTS),
            "demoWebsocket": _DEMO_WS_HOST in test_hosts and _PRODUCTION_WS_HOST not in test_hosts,
        }
