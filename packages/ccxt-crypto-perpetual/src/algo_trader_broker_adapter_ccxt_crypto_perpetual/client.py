"""Small CCXT/CCXT Pro boundary fixed to OKX Demo linear swaps."""

from __future__ import annotations

from typing import Any

from .settings import CCXTCryptoPerpetualSettings


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

    async def close(self) -> None:
        await self._exchange.close()

    async def load_markets(self) -> dict[str, Any]:
        return await self._exchange.load_markets(reload=True)

    async def fetch_time(self) -> int:
        return int(await self._exchange.fetch_time())

    async def fetch_position_mode(self, symbol: str) -> dict[str, Any]:
        return dict(await self._exchange.fetch_position_mode(symbol))

    async def fetch_leverage(self, symbol: str) -> dict[str, Any]:
        return dict(await self._exchange.fetch_leverage(symbol, {"marginMode": "isolated"}))

    async def fetch_balance(self) -> dict[str, Any]:
        return dict(await self._exchange.fetch_balance({"type": "swap"}))

    async def fetch_positions(self, symbols: list[str]) -> list[dict[str, Any]]:
        return list(await self._exchange.fetch_positions(symbols, {"marginMode": "isolated"}))

    async def fetch_open_orders(self, symbol: str) -> list[dict[str, Any]]:
        return list(await self._exchange.fetch_open_orders(symbol))

    async def fetch_closed_orders(self, symbol: str) -> list[dict[str, Any]]:
        return list(await self._exchange.fetch_closed_orders(symbol, limit=100))

    async def fetch_my_trades(self, symbol: str) -> list[dict[str, Any]]:
        return list(await self._exchange.fetch_my_trades(symbol, limit=100))

    async def fetch_funding_history(self, symbol: str) -> list[dict[str, Any]]:
        return list(await self._exchange.fetch_funding_history(symbol, limit=100))

    async def fetch_funding_rate(self, symbol: str) -> dict[str, Any]:
        return dict(await self._exchange.fetch_funding_rate(symbol))

    async def watch_orders(self) -> list[dict[str, Any]]:
        return list(await self._exchange.watch_orders())

    async def watch_my_trades(self) -> list[dict[str, Any]]:
        return list(await self._exchange.watch_my_trades())

    async def watch_positions(self, symbols: list[str]) -> list[dict[str, Any]]:
        return list(await self._exchange.watch_positions(symbols))

    async def watch_balance(self) -> dict[str, Any]:
        return dict(await self._exchange.watch_balance({"type": "swap"}))

    async def watch_funding_rate(self, symbol: str) -> dict[str, Any]:
        return dict(await self._exchange.watch_funding_rate(symbol))

    async def create_order(
        self,
        symbol: str,
        order_type: str,
        side: str,
        amount: str,
        price: str | None,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        return dict(await self._exchange.create_order(symbol, order_type, side, amount, price, params))

    async def cancel_order(self, order_id: str, symbol: str) -> dict[str, Any]:
        return dict(await self._exchange.cancel_order(order_id, symbol))
