"""Small CCXT/CCXT Pro boundary fixed to OKX Demo linear swaps."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from typing import Any

from algo_trader_broker_sdk import BrokerConnectionError, BrokerError

from .settings import CCXTCryptoPerpetualSettings

LOGGER = logging.getLogger(__name__)


class OKXDemoPerpetualClient:
    def __init__(self, settings: CCXTCryptoPerpetualSettings) -> None:
        try:
            import ccxt.pro as ccxtpro
        except ImportError as exc:  # pragma: no cover - packaging boundary
            raise RuntimeError("ccxt==4.5.56 with CCXT Pro is required") from exc
        self._exchange = ccxtpro.okx(
            {
                "apiKey": settings.api_key,
                "secret": settings.secret,
                "password": settings.passphrase,
                "enableRateLimit": True,
                "timeout": settings.request_timeout_ms,
                "headers": {"x-simulated-trading": "1"},
                "options": {
                    "defaultType": "swap",
                    "adjustForTimeDifference": True,
                },
            }
        )
        self._exchange.set_sandbox_mode(True)
        self._semaphore = asyncio.Semaphore(4)
        required_streams = (
            "watchMarkPrice",
            "watchFundingRate",
            "watchOrders",
            "watchMyTrades",
            "watchPositions",
            "watchBalance",
        )
        missing = [name for name in required_streams if self._exchange.has.get(name) is not True]
        if missing:
            raise RuntimeError(
                "ccxt==4.5.56 lacks required OKX Pro capabilities: "
                + ", ".join(sorted(missing))
            )

    async def close(self) -> None:
        await self._exchange.close()

    async def load_markets(self) -> dict[str, Any]:
        swaps = await self._read("fetch_markets", {"instType": "SWAP"})
        spots = await self._read("fetch_markets", {"instType": "SPOT"})
        allowed_ids = {
            "BTC-USDT-SWAP",
            "ETH-USDT-SWAP",
            "BTC-USDT",
            "ETH-USDT",
        }
        selected = [
            market
            for market in [*swaps, *spots]
            if str(market.get("id") or "") in allowed_ids
        ]
        markets = self._exchange.set_markets(selected)
        return {
            symbol: markets[symbol]
            for symbol in ("BTC/USDT:USDT", "ETH/USDT:USDT")
            if symbol in markets
        }

    async def fetch_time(self) -> int:
        return int(await self._read("fetch_time"))

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

    async def watch_orders(self) -> list[dict[str, Any]]:
        return list(await self._watch("watch_orders"))

    async def watch_my_trades(self) -> list[dict[str, Any]]:
        return list(await self._watch("watch_my_trades"))

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

    async def _call(self, method: str, *args: Any) -> Any:
        async with self._semaphore:
            try:
                return await getattr(self._exchange, method)(*args)
            except BrokerError:
                raise
            except Exception as exc:
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
        try:
            return await getattr(self._exchange, method)(*args)
        except BrokerError:
            raise
        except Exception as exc:
            raise BrokerConnectionError(
                f"OKX Demo perpetual {method} websocket failed",
                details={"operation": method, "error_type": type(exc).__name__},
            ) from exc

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
                "ratelimit",
                "50011",
            )
        )
