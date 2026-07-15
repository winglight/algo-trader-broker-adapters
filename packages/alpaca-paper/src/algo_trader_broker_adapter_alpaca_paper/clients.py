"""Official Alpaca SDK/REST client boundary.

Imports are lazy so package contract tests can run with injected fake clients.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any, AsyncIterator, Callable

from algo_trader_broker_sdk import BrokerConnectionError, BrokerOrderError

from .mapping import value
from .settings import AlpacaPaperSettings
from .streams import ThreadedAlpacaStream, TradeUpdateStream


PAPER_TRADING_BASE_URL = "https://paper-api.alpaca.markets"


class AlpacaClients:
    """Small async facade around alpaca-py and the documented activities REST API."""

    def __init__(self, settings: AlpacaPaperSettings) -> None:
        self.settings = settings
        self._semaphore = asyncio.Semaphore(settings.max_concurrency)
        self._trading: Any | None = None
        self._historical: Any | None = None
        self._trade_stream: TradeUpdateStream | None = None

    def _load(self) -> None:
        if self._trading is not None:
            return
        try:
            from alpaca.data.historical import StockHistoricalDataClient
            from alpaca.trading.client import TradingClient
        except ImportError as exc:
            raise BrokerConnectionError(
                "alpaca-py is required for the Alpaca Paper adapter",
                details={"dependency": "alpaca-py==0.43.5"},
            ) from exc
        self._trading = TradingClient(
            self.settings.api_key_id,
            self.settings.secret_key,
            paper=True,
        )
        self._historical = StockHistoricalDataClient(
            self.settings.api_key_id,
            self.settings.secret_key,
        )

    async def _call(self, operation: str, function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    asyncio.to_thread(function, *args, **kwargs),
                    timeout=self.settings.request_timeout_seconds,
                )
            except asyncio.TimeoutError:
                raise
            except BrokerOrderError:
                raise
            except Exception as exc:
                raise BrokerConnectionError(
                    f"Alpaca request failed during {operation}",
                    details={"operation": operation, "error_type": type(exc).__name__},
                ) from exc

    async def get_account(self) -> Any:
        self._load()
        return await self._call("get_account", self._trading.get_account)

    async def get_positions(self) -> list[Any]:
        self._load()
        return list(await self._call("get_positions", self._trading.get_all_positions))

    async def get_asset(self, symbol: str) -> Any:
        self._load()
        return await self._call("get_asset", self._trading.get_asset, symbol)

    @staticmethod
    def make_order_request(request: Any) -> Any:
        from alpaca.trading.enums import OrderSide, TimeInForce
        from alpaca.trading.requests import (
            LimitOrderRequest,
            MarketOrderRequest,
            StopLimitOrderRequest,
            StopOrderRequest,
        )

        common = {
            "symbol": request.symbol.strip().upper(),
            "qty": int(request.quantity),
            "side": OrderSide.BUY if request.side == "BUY" else OrderSide.SELL,
            "time_in_force": (
                TimeInForce.DAY if request.tif.strip().upper() == "DAY" else TimeInForce.GTC
            ),
            "client_order_id": str(request.client_order_id),
            "extended_hours": False,
        }
        order_type = request.order_type.strip().upper()
        if order_type == "MKT":
            return MarketOrderRequest(**common)
        if order_type == "LMT":
            return LimitOrderRequest(limit_price=request.limit_price, **common)
        if order_type == "STP":
            return StopOrderRequest(stop_price=request.stop_price, **common)
        return StopLimitOrderRequest(
            stop_price=request.stop_price,
            limit_price=request.limit_price,
            **common,
        )

    @staticmethod
    def make_timeframe(bar_size: str) -> Any:
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

        normalized = " ".join(str(bar_size).strip().lower().split())
        mapping = {
            "1 min": (1, TimeFrameUnit.Minute),
            "1 mins": (1, TimeFrameUnit.Minute),
            "5 mins": (5, TimeFrameUnit.Minute),
            "15 mins": (15, TimeFrameUnit.Minute),
            "30 mins": (30, TimeFrameUnit.Minute),
            "1 hour": (1, TimeFrameUnit.Hour),
            "1 day": (1, TimeFrameUnit.Day),
            "1 week": (1, TimeFrameUnit.Week),
            "1 month": (1, TimeFrameUnit.Month),
        }
        params = mapping.get(normalized)
        if params is None:
            return None
        return TimeFrame(*params)

    async def submit_order(self, request: Any) -> Any:
        self._load()
        return await self._call("submit_order", self._trading.submit_order, order_data=request)

    async def get_order_by_client_id(self, client_order_id: str) -> Any | None:
        self._load()
        try:
            return await self._call(
                "get_order_by_client_id",
                self._trading.get_order_by_client_id,
                client_order_id,
            )
        except BrokerConnectionError as exc:
            cause = exc.__cause__
            if getattr(cause, "status_code", None) == 404:
                return None
            raise

    async def get_order_by_id(self, order_id: str) -> Any | None:
        self._load()
        try:
            return await self._call("get_order_by_id", self._trading.get_order_by_id, order_id)
        except BrokerConnectionError as exc:
            cause = exc.__cause__
            if getattr(cause, "status_code", None) == 404:
                return None
            raise

    async def cancel_order(self, order_id: str) -> None:
        self._load()
        await self._call("cancel_order", self._trading.cancel_order_by_id, order_id)

    async def get_orders(self, *, status: str, after: datetime | None = None) -> list[Any]:
        self._load()
        from alpaca.trading.enums import QueryOrderStatus
        from alpaca.trading.requests import GetOrdersRequest

        query_status = QueryOrderStatus.OPEN if status == "open" else QueryOrderStatus.CLOSED
        request = GetOrdersRequest(status=query_status, after=after, limit=500)
        return list(await self._call("get_orders", self._trading.get_orders, filter=request))

    async def get_fill_activities(self, since: datetime) -> list[dict[str, Any]]:
        import httpx

        headers = {
            "APCA-API-KEY-ID": self.settings.api_key_id,
            "APCA-API-SECRET-KEY": self.settings.secret_key,
        }
        params: dict[str, Any] = {
            "after": since.astimezone(UTC).isoformat(),
            "direction": "asc",
            "page_size": 100,
        }
        activities: list[dict[str, Any]] = []
        async with httpx.AsyncClient(
            base_url=PAPER_TRADING_BASE_URL,
            headers=headers,
            timeout=self.settings.request_timeout_seconds,
        ) as client:
            while True:
                response = await client.get("/v2/account/activities/FILL", params=params)
                try:
                    response.raise_for_status()
                except httpx.HTTPError as exc:
                    raise BrokerConnectionError(
                        "Alpaca fill activity request failed",
                        details={
                            "operation": "get_fill_activities",
                            "status_code": response.status_code,
                            "request_id": response.headers.get("x-request-id"),
                        },
                    ) from exc
                page = response.json()
                if not isinstance(page, list):
                    raise BrokerConnectionError(
                        "Alpaca fill activity response is invalid",
                        details={"operation": "get_fill_activities"},
                    )
                activities.extend(item for item in page if isinstance(item, dict))
                if len(page) < 100:
                    break
                token = value(page[-1], "id")
                if not token:
                    break
                params["page_token"] = str(token)
        return activities

    async def get_bars(
        self,
        symbol: str,
        *,
        timeframe: Any,
        start: datetime,
        end: datetime,
    ) -> list[Any]:
        self._load()
        from alpaca.data.enums import DataFeed
        from alpaca.data.requests import StockBarsRequest

        request = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=timeframe,
            start=start,
            end=end,
            feed=DataFeed.IEX if self.settings.data_feed == "iex" else DataFeed.SIP,
        )
        result = await self._call("get_stock_bars", self._historical.get_stock_bars, request)
        data = value(result, "data", {})
        if isinstance(data, dict):
            return list(data.get(symbol, ()))
        try:
            return list(result[symbol])
        except (KeyError, TypeError):
            return []

    async def get_snapshot(self, symbol: str) -> Any:
        self._load()
        from alpaca.data.enums import DataFeed
        from alpaca.data.requests import StockSnapshotRequest

        request = StockSnapshotRequest(
            symbol_or_symbols=[symbol],
            feed=DataFeed.IEX if self.settings.data_feed == "iex" else DataFeed.SIP,
        )
        result = await self._call(
            "get_stock_snapshot", self._historical.get_stock_snapshot, request
        )
        if isinstance(result, dict):
            return result.get(symbol)
        try:
            return result[symbol]
        except (KeyError, TypeError):
            return None

    async def stream_stock(self, symbol: str, kind: str) -> ThreadedAlpacaStream:
        from alpaca.data.enums import DataFeed
        from alpaca.data.live import StockDataStream

        stream = StockDataStream(
            self.settings.api_key_id,
            self.settings.secret_key,
            feed=DataFeed.IEX if self.settings.data_feed == "iex" else DataFeed.SIP,
        )
        subscribe = {
            "bars": lambda handler: stream.subscribe_bars(handler, symbol),
            "trades": lambda handler: stream.subscribe_trades(handler, symbol),
            "quotes": lambda handler: stream.subscribe_quotes(handler, symbol),
        }.get(kind)
        if subscribe is None:
            raise ValueError(f"Unknown Alpaca stock stream kind: {kind}")
        return await ThreadedAlpacaStream(
            stream,
            subscribe,
            queue_size=self.settings.stream_queue_size,
            name=f"alpaca-paper.stock-{kind}-{symbol}",
        ).start()

    async def start_trade_updates(
        self,
        handler: Callable[[Any], Any],
        failure_handler: Callable[[BaseException], Any] | None = None,
    ) -> None:
        if self._trade_stream is not None:
            return
        from alpaca.trading.stream import TradingStream

        stream = TradingStream(
            self.settings.api_key_id,
            self.settings.secret_key,
            paper=True,
        )
        threaded = ThreadedAlpacaStream(
            stream,
            lambda callback: stream.subscribe_trade_updates(callback),
            queue_size=self.settings.stream_queue_size,
            name="alpaca-paper.trade-updates",
        )
        managed = TradeUpdateStream(threaded, handler, failure_handler)
        await managed.start()
        self._trade_stream = managed

    async def stop_trade_updates(self) -> None:
        stream = self._trade_stream
        self._trade_stream = None
        if stream is not None:
            await stream.close()

    async def close(self) -> None:
        await self.stop_trade_updates()


def duration_window(duration: str, end: datetime | str | None) -> tuple[datetime, datetime]:
    if end is None:
        end_at = datetime.now(UTC)
    elif isinstance(end, datetime):
        end_at = end if end.tzinfo else end.replace(tzinfo=UTC)
        end_at = end_at.astimezone(UTC)
    else:
        end_at = datetime.fromisoformat(str(end).strip().replace("Z", "+00:00"))
        end_at = end_at.replace(tzinfo=end_at.tzinfo or UTC).astimezone(UTC)
    tokens = str(duration or "").strip().upper().split()
    if len(tokens) != 2:
        raise BrokerOrderError("Alpaca historical duration must use '<count> <unit>'")
    try:
        count = int(tokens[0])
    except ValueError as exc:
        raise BrokerOrderError("Alpaca historical duration count must be an integer") from exc
    units = {
        "S": timedelta(seconds=count),
        "D": timedelta(days=count),
        "W": timedelta(weeks=count),
        "M": timedelta(days=30 * count),
        "Y": timedelta(days=365 * count),
    }
    delta = units.get(tokens[1])
    if count <= 0 or delta is None:
        raise BrokerOrderError("Alpaca historical duration unit is unsupported")
    return end_at - delta, end_at
