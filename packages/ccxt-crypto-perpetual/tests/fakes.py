from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any


def market(symbol: str) -> dict[str, Any]:
    base = symbol.split("/", 1)[0]
    return {
        "id": f"{base}-USDT-SWAP",
        "symbol": symbol,
        "swap": True,
        "future": False,
        "linear": True,
        "inverse": False,
        "settle": "USDT",
        "contractSize": 0.01 if base == "BTC" else 0.1,
        "active": True,
        "info": {
            "instId": f"{base}-USDT-SWAP",
            "ctType": "linear",
            "settleCcy": "USDT",
            "ctVal": "0.01" if base == "BTC" else "0.1",
            "tickSz": "0.1" if base == "BTC" else "0.01",
            "lotSz": "0.01",
            "minSz": "0.01",
        },
    }


class FakeBackend:
    def __init__(self) -> None:
        self.hedged = False
        self.margin_mode = "isolated"
        self.leverage = "2"
        self.tick_size = "0.1"
        self.position_contracts = "10"
        self.position_side = "long"
        self.created: list[dict[str, Any]] = []
        self.cancelled: list[tuple[str, str]] = []
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    async def load_markets(self) -> dict[str, Any]:
        result = {symbol: market(symbol) for symbol in ("BTC/USDT:USDT", "ETH/USDT:USDT")}
        result["BTC/USDT:USDT"]["info"]["tickSz"] = self.tick_size
        return result

    async def fetch_time(self) -> int:
        return int(datetime.now(UTC).timestamp() * 1000)

    async def fetch_position_mode(self, symbol: str) -> dict[str, Any]:
        return {"symbol": symbol, "hedged": self.hedged}

    async def fetch_leverage(self, symbol: str) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "marginMode": self.margin_mode,
            "longLeverage": self.leverage,
            "shortLeverage": self.leverage,
        }

    async def fetch_balance(self) -> dict[str, Any]:
        return {
            "total": {"USDT": "1000"},
            "free": {"USDT": "700"},
            "used": {"USDT": "300"},
        }

    async def fetch_positions(self, symbols: list[str]) -> list[dict[str, Any]]:
        assert symbols == ["BTC/USDT:USDT", "ETH/USDT:USDT"]
        return [
            {
                "symbol": "BTC/USDT:USDT",
                "contracts": self.position_contracts,
                "side": self.position_side,
                "entryPrice": "55000",
                "markPrice": "60000",
                "indexPrice": "59990",
                "liquidationPrice": "30000",
                "leverage": "2",
                "marginMode": "isolated",
                "initialMargin": "300",
                "maintenanceMargin": "6",
                "marginRatio": "0.02",
                "maintenanceTierId": "okx:BTC-USDT-SWAP:tier-1",
                "timestamp": int(datetime.now(UTC).timestamp() * 1000),
            },
            {
                "symbol": "ETH/USDT:USDT",
                "contracts": "0",
                "side": "long",
                "timestamp": int(datetime.now(UTC).timestamp() * 1000),
            },
        ]

    async def fetch_open_orders(self, symbol: str) -> list[dict[str, Any]]:
        return []

    async def fetch_closed_orders(self, symbol: str) -> list[dict[str, Any]]:
        return []

    async def fetch_my_trades(self, symbol: str) -> list[dict[str, Any]]:
        return []

    async def fetch_funding_history(self, symbol: str) -> list[dict[str, Any]]:
        if symbol != "BTC/USDT:USDT":
            return []
        now = int(datetime.now(UTC).timestamp() * 1000)
        return [
            {
                "id": "funding-bill-1",
                "symbol": symbol,
                "signedContracts": "10",
                "markNotional": "600",
                "fundingRate": "0.0001",
                "amount": "-0.06",
                "code": "USDT",
                "timestamp": now,
            }
        ]

    async def fetch_funding_rate(self, symbol: str) -> dict[str, Any]:
        now = datetime.now(UTC)
        return {
            "symbol": symbol,
            "markPrice": "60000" if symbol.startswith("BTC") else "3000",
            "indexPrice": "59990" if symbol.startswith("BTC") else "2999",
            "fundingRate": "0.0001",
            "fundingTimestamp": int((now + timedelta(hours=8)).timestamp() * 1000),
            "timestamp": int(now.timestamp() * 1000),
        }

    async def fetch_mark_price(self, symbol: str) -> dict[str, Any]:
        now = datetime.now(UTC)
        return {
            "symbol": symbol,
            "markPrice": "60000" if symbol.startswith("BTC") else "3000",
            "timestamp": int(now.timestamp() * 1000),
        }

    async def fetch_index_price(self, symbol: str) -> dict[str, Any]:
        now = datetime.now(UTC)
        return {
            "symbol": symbol,
            "indexPrice": "59990" if symbol.startswith("BTC") else "2999",
            "timestamp": int(now.timestamp() * 1000),
        }

    async def create_order(
        self,
        symbol: str,
        order_type: str,
        side: str,
        amount: str,
        price: str | None,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        self.created.append(
            {
                "symbol": symbol,
                "type": order_type,
                "side": side,
                "amount": amount,
                "price": price,
                "params": params,
            }
        )
        now = int(datetime.now(UTC).timestamp() * 1000)
        return {
            "id": "order-1",
            "clientOrderId": params["clOrdId"],
            "symbol": symbol,
            "amount": amount,
            "filled": "0",
            "status": "open",
            "timestamp": now,
            "info": {"ordId": "order-1", "clOrdId": params["clOrdId"], "uTime": now},
        }

    async def cancel_order(self, order_id: str, symbol: str) -> dict[str, Any]:
        self.cancelled.append((order_id, symbol))
        now = int(datetime.now(UTC).timestamp() * 1000)
        return {
            "id": order_id,
            "symbol": symbol,
            "amount": "1",
            "filled": "0",
            "status": "canceled",
            "timestamp": now,
            "info": {"ordId": order_id, "uTime": now},
        }
