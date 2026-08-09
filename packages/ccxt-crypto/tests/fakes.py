from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

MARKETS = {
    "BTC/USDT": {
        "id": "BTC-USDT",
        "type": "spot",
        "spot": True,
        "active": True,
        "precision": {"amount": "0.00000001", "price": "0.1"},
        "limits": {"amount": {"min": "0.00001"}},
        "info": {
            "instId": "BTC-USDT",
            "instType": "SPOT",
            "tickSz": "0.1",
            "lotSz": "0.00000001",
            "minSz": "0.00001",
            "maxLmtSz": "100",
            "maxMktSz": "10",
            "state": "live",
            "listTime": "0",
        },
    },
    "ETH/USDT": {
        "id": "ETH-USDT",
        "type": "spot",
        "spot": True,
        "active": True,
        "precision": {"amount": "0.0000001", "price": "0.01"},
        "limits": {"amount": {"min": "0.0001"}},
        "info": {
            "instId": "ETH-USDT",
            "instType": "SPOT",
            "tickSz": "0.01",
            "lotSz": "0.0000001",
            "minSz": "0.0001",
            "maxLmtSz": "1000",
            "maxMktSz": "100",
            "state": "live",
            "listTime": "0",
        },
    },
}


BALANCE = {
    "total": {"BTC": "0", "ETH": "0", "USDT": "1000"},
    "free": {"BTC": "0", "ETH": "0", "USDT": "1000"},
    "used": {"BTC": "0", "ETH": "0", "USDT": "0"},
    "info": {
        "data": [
            {
                "details": [
                    {"ccy": "BTC", "cashBal": "0", "availBal": "0", "frozenBal": "0", "liab": "0"},
                    {"ccy": "ETH", "cashBal": "0", "availBal": "0", "frozenBal": "0", "liab": "0"},
                    {"ccy": "USDT", "cashBal": "1000", "availBal": "1000", "frozenBal": "0", "liab": "0"},
                ]
            }
        ]
    },
}


ORDER = {
    "id": "10001",
    "clientOrderId": "client123",
    "symbol": "BTC/USDT",
    "status": "open",
    "amount": "0.001",
    "filled": "0",
    "average": None,
    "timestamp": 1786262400000,
    "info": {"ordId": "10001", "clOrdId": "client123", "state": "live", "uTime": "1786262400000", "sCode": "0"},
}


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.create_timeout = False
        self.cancel_timeout = False
        self.fetch_order_fails = False
        self.reconciled_order: dict[str, Any] | None = None
        self.closed = False
        self._block = asyncio.Event()

    async def load_markets(self):
        self.calls.append(("load_markets", None))
        return MARKETS

    async def fetch_time(self):
        return int(datetime.now(UTC).timestamp() * 1000)

    async def fetch_balance(self):
        self.calls.append(("fetch_balance", None))
        return BALANCE

    async def fetch_ticker(self, symbol):
        self.calls.append(("fetch_ticker", symbol))
        return {
            "bid": "9999",
            "ask": "10001",
            "last": "10000",
            "timestamp": int(datetime.now(UTC).timestamp() * 1000),
        }

    async def fetch_trading_fee(self, symbol):
        self.calls.append(("fetch_trading_fee", symbol))
        return {"maker": "0.0008", "taker": "0.001"}

    async def create_order(self, symbol, order_type, side, amount, price, params):
        self.calls.append(("create_order", (symbol, order_type, side, amount, price, params)))
        if self.create_timeout:
            raise TimeoutError
        result = dict(ORDER)
        result["clientOrderId"] = params["clOrdId"]
        result["info"] = {**ORDER["info"], "clOrdId": params["clOrdId"]}
        return result

    async def fetch_order_by_client_id(self, client_id, symbol):
        self.calls.append(("fetch_order_by_client_id", (client_id, symbol)))
        return self.reconciled_order

    async def cancel_order(self, order_id, symbol):
        self.calls.append(("cancel_order", (order_id, symbol)))
        if self.cancel_timeout:
            raise TimeoutError
        return {"id": order_id}

    async def fetch_order(self, order_id, symbol):
        if self.fetch_order_fails:
            raise TimeoutError
        return {**ORDER, "id": order_id, "symbol": symbol, "status": "canceled", "info": {**ORDER["info"], "ordId": order_id, "state": "canceled"}}

    async def fetch_open_orders(self, symbol):
        return [ORDER] if symbol == "BTC/USDT" else []

    async def fetch_closed_orders(self, symbol):
        return []

    async def fetch_my_trades(self, symbol):
        return []

    async def fetch_ohlcv(self, symbol, timeframe, since=None, limit=None):
        return [[1786262400000, "1", "2", "0.5", "1.5", "10"]]

    async def watch_orders(self):
        await self._block.wait()
        return []

    async def watch_balance(self):
        await self._block.wait()
        return BALANCE

    async def watch_my_trades(self):
        await self._block.wait()
        return []

    async def close(self):
        self.closed = True

    def sandbox_evidence(self):
        return {"sandboxMode": True, "simulatedTradingHeader": True, "demoWebsocket": True}


def settings(**overrides: Any) -> dict[str, Any]:
    result: dict[str, Any] = {
        "exchange_id": "okx",
        "sandbox": True,
        "live": False,
        "allowed_symbols": "BTC/USDT,ETH/USDT",
        "execution_target_id": "okx-spot-demo-paper-1",
        "market_data_target_id": "okx-spot-demo-market-1",
        "public_data_enabled": True,
        "private_read_enabled": False,
        "trading_enabled": False,
        "market_order_enabled": False,
    }
    result.update(overrides)
    return result
