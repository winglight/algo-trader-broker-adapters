"""Small CCXT/CCXT Pro boundary fixed to OKX Demo linear swaps."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Mapping
from typing import Any

from algo_trader_broker_sdk import BrokerConnectionError, BrokerError, BrokerOrderError

from .settings import CCXTCryptoPerpetualSettings

LOGGER = logging.getLogger(__name__)


class OKXDemoPerpetualClient:
    def __init__(self, settings: CCXTCryptoPerpetualSettings) -> None:
        try:
            import ccxt.async_support as ccxtasync
            import ccxt.pro as ccxtpro
        except ImportError as exc:  # pragma: no cover - packaging boundary
            raise RuntimeError("ccxt==4.5.56 with CCXT Pro is required") from exc
        config = {
            "apiKey": settings.api_key,
            "secret": settings.secret,
            "password": settings.passphrase,
            "enableRateLimit": True,
            "timeout": settings.request_timeout_ms,
            "headers": {"x-simulated-trading": "1"},
            "options": {
                "defaultType": "swap",
                "fetchMarkets": {"types": ["spot", "swap"]},
                "adjustForTimeDifference": True,
            },
        }
        self._exchange = ccxtasync.okx(config)
        self._public_ws_exchange = ccxtpro.okx(config)
        self._private_ws_exchanges = {
            stream: ccxtpro.okx(config) for stream in ("orders", "positions", "balance")
        }
        self._exchange.set_sandbox_mode(True)
        self._public_ws_exchange.set_sandbox_mode(True)
        for websocket in self._private_ws_exchanges.values():
            websocket.set_sandbox_mode(True)
        self._semaphore = asyncio.Semaphore(4)
        boundaries = (
            "public",
            "private_orders",
            "private_positions",
            "private_balance",
        )
        self._websocket_reset_locks = {boundary: asyncio.Lock() for boundary in boundaries}
        self._websocket_generations = {boundary: 0 for boundary in boundaries}
        required_streams = (
            "watchMarkPrice",
            "watchFundingRate",
            "watchOrders",
            "watchMyTrades",
            "watchPositions",
            "watchBalance",
        )
        stream_exchange = {
            "watchOrders": self._private_ws_exchanges["orders"],
            "watchMyTrades": self._private_ws_exchanges["orders"],
            "watchPositions": self._private_ws_exchanges["positions"],
            "watchBalance": self._private_ws_exchanges["balance"],
        }
        missing = [
            name
            for name in required_streams
            if stream_exchange.get(name, self._public_ws_exchange).has.get(name) is not True
        ]
        if missing:
            raise RuntimeError(
                "ccxt==4.5.56 lacks required OKX Pro capabilities: " + ", ".join(sorted(missing))
            )

    async def close(self) -> None:
        await asyncio.gather(
            self._exchange.close(),
            self._public_ws_exchange.close(),
            *(websocket.close() for websocket in self._private_ws_exchanges.values()),
            return_exceptions=True,
        )

    async def load_markets(self) -> dict[str, Any]:
        available = await self._read("fetch_markets")
        allowed_ids = {
            "BTC-USDT-SWAP",
            "ETH-USDT-SWAP",
            "BTC-USDT",
            "ETH-USDT",
        }
        selected = [market for market in available if str(market.get("id") or "") in allowed_ids]
        markets = self._exchange.set_markets(selected)
        websockets = {
            item
            for item in (
                getattr(self, "_public_ws_exchange", None),
                *getattr(self, "_private_ws_exchanges", {}).values(),
                getattr(self, "_ws_exchange", None),
            )
            if item is not None
        }
        for websocket in websockets:
            websocket.set_markets(selected)
        return {
            symbol: markets[symbol]
            for symbol in ("BTC/USDT:USDT", "ETH/USDT:USDT")
            if symbol in markets
        }

    async def fetch_time(self) -> int:
        remote_time, _, _ = await self.fetch_time_sample()
        return remote_time

    async def fetch_time_sample(self) -> tuple[int, int, int]:
        """Return server time with the successful request's local RTT bounds."""

        for attempt in range(3):
            started_at_ms = time.time_ns() // 1_000_000
            try:
                remote_time = int(await self._call("fetch_time"))
                completed_at_ms = time.time_ns() // 1_000_000
                return remote_time, started_at_ms, completed_at_ms
            except Exception as exc:
                if not self._is_transient(exc) or attempt == 2:
                    raise
                LOGGER.warning(
                    "OKX Demo perpetual read request will be retried",
                    extra={
                        "event": "broker.crypto.perpetual_read_retry",
                        "broker.adapter_id": "ccxt_crypto",
                        "broker.operation": "fetch_time",
                        "broker.retry_attempt": attempt + 1,
                        "broker.error_type": self._error_type(exc),
                    },
                )
                await asyncio.sleep(float(2**attempt))
        raise AssertionError("unreachable")

    async def fetch_position_mode(self, symbol: str) -> dict[str, Any]:
        return dict(await self._read("fetch_position_mode", symbol))

    async def fetch_leverage(self, symbol: str) -> dict[str, Any]:
        return dict(
            await self._read(
                "fetch_leverage",
                symbol,
                {"marginMode": "isolated"},
            )
        )

    async def fetch_balance(self) -> dict[str, Any]:
        return dict(await self._read("fetch_balance", {"type": "swap"}))

    async def fetch_positions(self, symbols: list[str]) -> list[dict[str, Any]]:
        return list(
            await self._read(
                "fetch_positions",
                symbols,
                {"marginMode": "isolated"},
            )
        )

    async def fetch_open_orders(self, symbol: str) -> list[dict[str, Any]]:
        return list(await self._read("fetch_open_orders", symbol))

    async def fetch_closed_orders(self, symbol: str) -> list[dict[str, Any]]:
        return list(await self._read("fetch_closed_orders", symbol, None, 100))

    async def fetch_my_trades(self, symbol: str) -> list[dict[str, Any]]:
        return list(await self._read("fetch_my_trades", symbol, None, 100))

    async def fetch_funding_history(self, symbol: str) -> list[dict[str, Any]]:
        return list(await self._read("fetch_funding_history", symbol, None, 100))

    async def fetch_funding_rate(self, symbol: str) -> dict[str, Any]:
        return dict(await self._read("fetch_funding_rate", symbol))

    async def fetch_mark_price(self, symbol: str) -> dict[str, Any]:
        return dict(await self._read("fetch_mark_price", symbol))

    async def fetch_index_price(self, symbol: str) -> dict[str, Any]:
        index_instrument_id = symbol.split("/", 1)[0] + "-USDT"
        response = await self._read(
            "public_get_market_index_tickers",
            {"instId": index_instrument_id},
        )
        data = response.get("data") if isinstance(response, Mapping) else None
        row = data[0] if isinstance(data, list) and data else None
        if not isinstance(row, Mapping) or not row.get("idxPx"):
            raise BrokerConnectionError("OKX index-price response is incomplete")
        return {
            "symbol": symbol,
            "indexPrice": row["idxPx"],
            "timestamp": row.get("ts"),
        }

    async def watch_orders(self) -> list[dict[str, Any]]:
        return list(await self._watch("watch_orders"))

    async def watch_my_trades(self) -> list[dict[str, Any]]:
        return list(await self._watch("watch_my_trades"))

    def trade_from_order(self, order: Mapping[str, Any]) -> dict[str, Any] | None:
        info = order.get("info") if isinstance(order.get("info"), Mapping) else {}
        if not info.get("tradeId"):
            return None
        trade = self._private_ws_exchanges["orders"].order_to_trade(dict(order))
        return dict(trade) if isinstance(trade, Mapping) else None

    async def watch_positions(self, symbols: list[str]) -> list[dict[str, Any]]:
        return list(await self._watch("watch_positions", symbols))

    async def watch_balance(self) -> dict[str, Any]:
        return dict(await self._watch("watch_balance", {"type": "swap"}))

    async def watch_funding_rate(self, symbol: str) -> dict[str, Any]:
        return dict(await self._watch("watch_funding_rate", symbol))

    async def watch_mark_price(self, symbol: str) -> dict[str, Any]:
        return dict(await self._watch("watch_mark_price", symbol))

    async def watch_index_price(self, symbol: str) -> dict[str, Any]:
        # OKX index-tickers uses the index instrument id (BTC-USDT), not the
        # swap instrument id (BTC-USDT-SWAP). CCXT derives that id from the
        # Spot-shaped unified symbol while still parsing an index ticker.
        index_symbol = symbol.split(":", 1)[0]
        return dict(
            await self._watch(
                "watch_mark_price",
                index_symbol,
                {"channel": "index-tickers"},
            )
        )

    async def create_order(
        self,
        symbol: str,
        order_type: str,
        side: str,
        amount: str,
        price: str | None,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        return dict(
            await self._call(
                "create_order",
                symbol,
                order_type,
                side,
                amount,
                price,
                params,
            )
        )

    async def cancel_order(self, order_id: str, symbol: str) -> dict[str, Any]:
        return dict(await self._call("cancel_order", order_id, symbol))

    async def fetch_order(self, order_id: str, symbol: str) -> dict[str, Any]:
        return dict(await self._read("fetch_order", order_id, symbol))

    async def fetch_order_by_client_id(
        self, client_order_id: str, symbol: str
    ) -> dict[str, Any] | None:
        open_orders, closed_orders = await asyncio.gather(
            self.fetch_open_orders(symbol),
            self.fetch_closed_orders(symbol),
        )
        for order in [*open_orders, *closed_orders]:
            info = order.get("info") if isinstance(order.get("info"), Mapping) else {}
            value = str(order.get("clientOrderId") or info.get("clOrdId") or "")
            if value == client_order_id:
                return dict(order)
        return None

    async def _call(self, method: str, *args: Any) -> Any:
        async with self._semaphore:
            try:
                return await getattr(self._exchange, method)(*args)
            except BrokerError:
                raise
            except Exception as exc:
                if method in {"create_order", "cancel_order"} and not self._is_transient(exc):
                    raise BrokerOrderError(
                        f"OKX Demo perpetual rejected {method}",
                        code="provider_order_error",
                        details={
                            "operation": method,
                            "error_type": type(exc).__name__,
                            "outcome_unknown": False,
                        },
                    ) from exc
                raise BrokerConnectionError(
                    f"OKX Demo perpetual {method} request failed",
                    details={"operation": method, "error_type": type(exc).__name__},
                ) from exc

    async def _read(self, method: str, *args: Any) -> Any:
        """Bounded retries for idempotent reads; never used by mutations."""

        for attempt in range(3):
            try:
                return await self._call(method, *args)
            except Exception as exc:
                if not self._is_transient(exc) or attempt == 2:
                    raise
                LOGGER.warning(
                    "OKX Demo perpetual read request will be retried",
                    extra={
                        "event": "broker.crypto.perpetual_read_retry",
                        "broker.adapter_id": "ccxt_crypto",
                        "broker.operation": method,
                        "broker.retry_attempt": attempt + 1,
                        "broker.error_type": self._error_type(exc),
                    },
                )
                await asyncio.sleep(float(2**attempt))
        raise AssertionError("unreachable")

    async def _watch(self, method: str, *args: Any) -> Any:
        boundary = self._websocket_boundary(method)
        websocket = self._websocket_exchange(boundary)
        generations = getattr(self, "_websocket_generations", {boundary: 0})
        generation = generations[boundary]
        try:
            return await getattr(websocket, method)(*args)
        except BrokerError:
            raise
        except Exception as exc:
            reset = False
            current_generation = getattr(self, "_websocket_generations", {boundary: generation})[
                boundary
            ]
            generation_advanced = current_generation != generation
            transient = generation_advanced or self._is_transient(exc)
            if transient and not generation_advanced and hasattr(self, "_websocket_reset_locks"):
                reset = await self._reset_websocket(
                    failed_generation=generation,
                    boundary=boundary,
                    operation=method,
                    error=exc,
                )
            raise BrokerConnectionError(
                f"OKX Demo perpetual {method} websocket failed",
                details={
                    "operation": method,
                    "error_type": type(exc).__name__,
                    "websocketBoundary": boundary,
                    "websocketGeneration": generation,
                    "websocketResetHandled": transient,
                    "resetPerformed": reset,
                    "nextWebsocketGeneration": getattr(
                        self, "_websocket_generations", {boundary: generation}
                    )[boundary],
                },
            ) from exc

    @staticmethod
    def _websocket_boundary(method: str) -> str:
        if method in {"watch_orders", "watch_my_trades"}:
            return "private_orders"
        if method == "watch_positions":
            return "private_positions"
        if method == "watch_balance":
            return "private_balance"
        return "public"

    def _websocket_exchange(self, boundary: str) -> Any:
        if boundary.startswith("private_"):
            exchanges = getattr(self, "_private_ws_exchanges", None)
            if exchanges is not None:
                return exchanges[boundary.removeprefix("private_")]
        exchange = getattr(self, f"_{boundary}_ws_exchange", None)
        if exchange is not None:
            return exchange
        return getattr(self, "_ws_exchange", self._exchange)

    async def _reset_websocket(
        self,
        *,
        failed_generation: int,
        boundary: str,
        operation: str,
        error: Exception,
    ) -> bool:
        async with self._websocket_reset_locks[boundary]:
            if failed_generation != self._websocket_generations[boundary]:
                return False
            await self._websocket_exchange(boundary).close()
            self._websocket_generations[boundary] += 1
            LOGGER.warning(
                "OKX Demo perpetual websocket reset after transient failure",
                extra={
                    "event": "broker.crypto.perpetual_websocket_reset",
                    "broker.adapter_id": "ccxt_crypto",
                    "broker.operation": operation,
                    "broker.websocket_boundary": boundary,
                    "broker.error_type": type(error).__name__,
                    "broker.websocket_generation": self._websocket_generations[boundary],
                },
            )
            return True

    @staticmethod
    def _error_type(exc: Exception) -> str:
        details = getattr(exc, "details", None)
        if isinstance(details, Mapping):
            value = str(details.get("error_type") or "").strip()
            if value:
                return value
        return type(exc).__name__

    @classmethod
    def _is_transient(cls, exc: Exception) -> bool:
        current: BaseException | None = exc
        evidence: list[str] = [cls._error_type(exc).lower()]
        visited: set[int] = set()
        while current is not None and id(current) not in visited:
            visited.add(id(current))
            evidence.append(f"{type(current).__name__} {current}".lower())
            current = current.__cause__ or current.__context__
        rendered = " ".join(evidence)
        return any(
            token in rendered
            for token in (
                "timeout",
                "network",
                "unavailable",
                "disconnect",
                "closedbyuser",
                "closed by user",
                "connection is closed",
                "1006",
                "ratelimit",
                "50011",
            )
        )
