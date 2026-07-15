"""Alpaca Paper Broker SDK adapter."""

from __future__ import annotations

import asyncio
import inspect
from datetime import UTC, datetime, timedelta
from typing import Any, AsyncIterator, Awaitable, Callable, Iterable, Mapping

from algo_trader_broker_sdk import (
    AccountSummaryItem,
    BrokerAdapterManifest,
    BrokerCapabilities,
    BrokerConnectionError,
    BrokerConnectionState,
    BrokerContractError,
    BrokerOrderError,
    DOMSnapshot,
    FutureOrderRequest,
    HistoricalBar,
    HistoricalTickBidAsk,
    HistoricalTickLast,
    OptionOrderRequest,
    OrderResult,
    PositionItem,
    RealTimePrice,
    StockOrderRequest,
    TickByTickBidAsk,
    TickByTickLast,
    TickByTickMidPoint,
    TradeUpdate,
)

from .clients import AlpacaClients, duration_window
from .errors import outcome_unknown, unsupported
from .mapping import (
    broker_status,
    map_account,
    map_bar,
    map_fill_activity,
    map_order_result,
    map_position,
    map_trade_update,
    number,
    text,
    utc_datetime,
    value,
)
from .settings import AlpacaPaperSettings


_TERMINAL = {"Filled", "Cancelled", "Rejected", "Inactive"}
_ORDER_TYPES = {"MKT", "LMT", "STP", "STP LMT"}
_TIFS = {"DAY", "GTC"}


class AlpacaPaperAdapter:
    adapter_id = "alpaca_paper"

    def __init__(self, settings: Mapping[str, Any], *, backend: Any | None = None) -> None:
        self._settings = AlpacaPaperSettings.from_mapping(settings)
        self._backend = backend or AlpacaClients(self._settings)
        self._connected = False
        self._connected_since: datetime | None = None
        self._reconnect_reason: str | None = None
        self._account_id: str | None = None
        self._trade_update_handler: Callable[[TradeUpdate], Awaitable[None]] | None = None
        self._position_update_handler: Callable[[list[PositionItem]], Awaitable[None]] | None = None
        self._account_update_handler: Callable[[list[AccountSummaryItem]], Awaitable[None]] | None = None
        self._connection_listeners: list[
            Callable[[str, Mapping[str, Any]], Awaitable[None] | None]
        ] = []
        self._resub_tasks: list[Callable[[], Awaitable[None]]] = []
        self._lifecycle_lock = asyncio.Lock()
        self._stream_reconnect_task: asyncio.Task[None] | None = None

    def manifest(self) -> BrokerAdapterManifest:
        return BrokerAdapterManifest(
            adapter_id=self.adapter_id,
            display_name="Alpaca Paper Adapter",
            adapter_version="0.1.0",
            protocol_version="1.0",
            environment="PAPER",
            entrypoint="algo_trader_broker_adapter_alpaca_paper:create_adapter",
            capabilities=self.capabilities(),
        )

    def capabilities(self) -> BrokerCapabilities:
        return BrokerCapabilities(
            adapter_name=self.adapter_id,
            environment="PAPER",
            asset_classes={"STK", "ETF"},
            order_types=set(_ORDER_TYPES),
            time_in_force=set(_TIFS),
            market_data_streams={"historical_bars", "realtime_price", "tick_by_tick"},
            account_features={"summary", "positions", "pnl", "position_updates"},
            supports_fractional=False,
            supports_shorting=True,
            supports_replace=False,
            supports_partial_fills=True,
            supports_scanner=False,
            supports_options=False,
            supports_futures=False,
            native={"dataFeed": self._settings.data_feed, "paperOnly": True},
        )

    async def start(self) -> None:
        await self.connect()

    async def close(self) -> None:
        async with self._lifecycle_lock:
            reconnect_task = self._stream_reconnect_task
            self._stream_reconnect_task = None
            if reconnect_task is not None and reconnect_task is not asyncio.current_task():
                reconnect_task.cancel()
            if hasattr(self._backend, "close"):
                await self._backend.close()
            self._connected = False
            await self._notify_connection("disconnected", {"reason": "closed"})

    async def connect(self) -> None:
        async with self._lifecycle_lock:
            if self._connected:
                return
            account = await self._backend.get_account()
            self._account_id = text(value(account, "id") or value(account, "account_number"))
            await self._start_trade_updates()
            self._connected = True
            self._connected_since = datetime.now(UTC)
            await self._notify_connection("connected", {"adapter_id": self.adapter_id})
            await self._publish_account(account)
            await self._publish_positions()

    async def disconnect(self, reason: str | None = None) -> None:
        async with self._lifecycle_lock:
            self._reconnect_reason = reason
            if hasattr(self._backend, "stop_trade_updates"):
                await self._backend.stop_trade_updates()
            reconnect_task = self._stream_reconnect_task
            self._stream_reconnect_task = None
            if reconnect_task is not None and reconnect_task is not asyncio.current_task():
                reconnect_task.cancel()
            self._connected = False
            await self._notify_connection("disconnected", {"reason": reason})

    async def reconnect(self, *, reason: str | None = None) -> None:
        await self.disconnect(reason=reason)
        await self.connect()
        for factory in tuple(self._resub_tasks):
            await factory()

    async def ensure_connected(self) -> None:
        if not self._connected:
            raise BrokerConnectionError(
                "Alpaca Paper adapter is not connected",
                details={"adapter_id": self.adapter_id},
            )

    def connection_state_snapshot(self) -> BrokerConnectionState:
        return BrokerConnectionState(
            connected=self._connected,
            adapter=self.adapter_id,
            state="connected" if self._connected else "disconnected",
            connected_since=self._connected_since,
            reconnect_reason=self._reconnect_reason,
        )

    def set_trade_update_handler(
        self, handler: Callable[[TradeUpdate], Awaitable[None]] | None
    ) -> None:
        self._trade_update_handler = handler

    def set_position_update_handler(
        self, handler: Callable[[list[PositionItem]], Awaitable[None]] | None
    ) -> None:
        self._position_update_handler = handler

    def set_account_update_handler(
        self, handler: Callable[[list[AccountSummaryItem]], Awaitable[None]] | None
    ) -> None:
        self._account_update_handler = handler

    def add_connection_listener(
        self, listener: Callable[[str, Mapping[str, Any]], Awaitable[None] | None]
    ) -> None:
        if listener not in self._connection_listeners:
            self._connection_listeners.append(listener)

    def remove_connection_listener(
        self, listener: Callable[[str, Mapping[str, Any]], Awaitable[None] | None]
    ) -> None:
        if listener in self._connection_listeners:
            self._connection_listeners.remove(listener)

    def add_resub_task(self, coro_factory: Callable[[], Awaitable[None]]) -> None:
        self._resub_tasks.append(coro_factory)

    async def _notify_connection(self, state: str, payload: Mapping[str, Any]) -> None:
        for listener in tuple(self._connection_listeners):
            result = listener(state, payload)
            if inspect.isawaitable(result):
                await result

    async def _publish_account(self, account: Any) -> list[AccountSummaryItem]:
        items = map_account(account)
        if self._account_update_handler is not None:
            await self._account_update_handler(items)
        return items

    async def _publish_positions(self) -> list[PositionItem]:
        native = await self._backend.get_positions()
        account_id = self._account_id or ""
        positions = [map_position(item, account_id=account_id) for item in native]
        if self._position_update_handler is not None:
            await self._position_update_handler(positions)
        return positions

    async def _handle_native_trade_update(self, payload: Any) -> None:
        event = text(value(payload, "event")).strip().lower()
        order = value(payload, "order") or payload
        if event in {"order_cancel_rejected", "order_replace_rejected"}:
            order_id = text(value(order, "id")).strip()
            refreshed = await self._backend.get_order_by_id(order_id) if order_id else None
            if refreshed is not None:
                order = refreshed
        update = map_trade_update(
            order,
            event=event or None,
            execution_id=(
                text(value(payload, "execution_id")).strip()
                or None
            )
            if event in {"fill", "partial_fill"}
            else None,
            event_time=value(payload, "timestamp") or value(payload, "at"),
            last_fill_price=value(payload, "price"),
            last_fill_quantity=value(payload, "qty"),
        )
        if self._trade_update_handler is not None:
            await self._trade_update_handler(update)
        if event in {"fill", "partial_fill"}:
            await self._publish_positions()
            await self._publish_account(await self._backend.get_account())

    async def _start_trade_updates(self) -> None:
        await self._backend.start_trade_updates(
            self._handle_native_trade_update,
            self._handle_trade_stream_failure,
        )

    async def _handle_trade_stream_failure(self, exc: BaseException) -> None:
        self._connected = False
        await self._notify_connection(
            "disconnected",
            {
                "reason": "trade_stream_failed",
                "error_type": type(exc).__name__,
                "reconciliation_required": True,
            },
        )
        if self._stream_reconnect_task is None or self._stream_reconnect_task.done():
            self._stream_reconnect_task = asyncio.create_task(
                self._recover_trade_stream(),
                name="alpaca-paper.trade-stream-recovery",
            )

    async def _recover_trade_stream(self) -> None:
        for attempt in range(5):
            await asyncio.sleep(min(8.0, 0.5 * (2**attempt)))
            try:
                await self._backend.stop_trade_updates()
                await self._start_trade_updates()
                open_updates = await self.request_open_orders_unchecked()
                await self.request_executions_unchecked()
            except asyncio.CancelledError:
                raise
            except Exception:
                continue
            self._connected = True
            self._connected_since = datetime.now(UTC)
            await self._notify_connection(
                "connected",
                {"reason": "trade_stream_recovered", "reconciled": True},
            )
            if self._trade_update_handler is not None:
                for update in open_updates:
                    await self._trade_update_handler(update)
            return

    async def request_open_orders_unchecked(self) -> list[TradeUpdate]:
        return [map_trade_update(order) for order in await self._backend.get_orders(status="open")]

    async def request_executions_unchecked(self) -> list[TradeUpdate]:
        since_at = datetime.now(UTC) - timedelta(
            hours=self._settings.reconcile_lookback_hours
        )
        activities = await self._backend.get_fill_activities(since_at)
        closed = await self._backend.get_orders(status="closed", after=since_at)
        orders = {text(value(item, "id")): item for item in closed}
        updates = [
            map_fill_activity(item, orders.get(text(value(item, "order_id"))))
            for item in activities
        ]
        if self._trade_update_handler is not None:
            for update in updates:
                await self._trade_update_handler(update)
        return updates

    async def get_account_summary(
        self, account: str | None = None
    ) -> list[AccountSummaryItem]:
        await self.ensure_connected()
        native = await self._backend.get_account()
        items = map_account(native)
        if account and any(item.account != account for item in items):
            raise BrokerContractError("Requested Alpaca account does not match active account")
        return items

    async def get_account_pnl(
        self,
        account: str | None = None,
        *,
        model_code: str | None = None,
        timeout: float = 5.0,
    ) -> dict[str, float | None] | None:
        del model_code, timeout
        await self.ensure_connected()
        native = await self._backend.get_account()
        active_id = text(value(native, "id") or value(native, "account_number"))
        if account and account != active_id:
            raise BrokerContractError("Requested Alpaca account does not match active account")
        equity = value(native, "equity")
        last_equity = value(native, "last_equity")
        return {
            "daily": (
                number(equity) - number(last_equity)
                if equity not in (None, "") and last_equity not in (None, "")
                else None
            ),
            "realized": None,
            "unrealized": None,
        }

    async def get_positions(self) -> list[PositionItem]:
        await self.ensure_connected()
        return await self._publish_positions()

    async def _validate_stock_order(self, request: StockOrderRequest) -> Any:
        if request.currency.strip().upper() != "USD":
            raise BrokerOrderError("Alpaca Paper supports USD equities only")
        if request.quantity <= 0 or not float(request.quantity).is_integer():
            raise BrokerOrderError(
                "Alpaca Paper Phase 4 requires a positive whole-share quantity",
                code="alpaca_fractional_not_supported",
            )
        order_type = request.order_type.strip().upper()
        tif = request.tif.strip().upper()
        if order_type not in _ORDER_TYPES:
            raise unsupported("order_type")
        if tif not in _TIFS:
            raise unsupported("time_in_force")
        if request.outside_rth:
            raise unsupported("extended_hours")
        if request.adapter_parameters:
            raise BrokerOrderError(
                "Alpaca Paper does not accept adapter_parameters in Phase 4",
                code="invalid_adapter_parameters",
                details={"keys": sorted(request.adapter_parameters)},
            )
        client_order_id = str(request.client_order_id or "").strip()
        if not client_order_id:
            raise BrokerOrderError(
                "client_order_id is required before Alpaca submission",
                code="client_order_id_required",
            )
        if len(client_order_id) > 48:
            raise BrokerOrderError(
                "client_order_id exceeds Alpaca's 48 character limit",
                code="client_order_id_invalid",
            )
        if order_type in {"LMT", "STP LMT"} and request.limit_price is None:
            raise BrokerOrderError("limit_price is required for this order type")
        if order_type in {"STP", "STP LMT"} and request.stop_price is None:
            raise BrokerOrderError("stop_price is required for this order type")
        symbol = request.symbol.strip().upper()
        if not symbol:
            raise BrokerOrderError("symbol is required")
        asset = await self._backend.get_asset(symbol)
        if text(value(asset, "class") or value(asset, "asset_class")).lower() != "us_equity":
            raise BrokerOrderError("Alpaca asset is not a US equity or ETF")
        if not bool(value(asset, "active", False)) or not bool(value(asset, "tradable", False)):
            raise BrokerOrderError("Alpaca asset is not active and tradable")
        if request.side == "SELL":
            current_qty = 0.0
            for position in await self._backend.get_positions():
                if text(value(position, "symbol")).strip().upper() == symbol:
                    current_qty = number(value(position, "qty"))
                    break
            if request.quantity > max(0.0, current_qty) and not bool(value(asset, "shortable", False)):
                raise BrokerOrderError("Alpaca asset is not shortable")
        return asset

    async def place_stock_order(self, request: StockOrderRequest) -> OrderResult:
        await self.ensure_connected()
        await self._validate_stock_order(request)
        native_request = self._backend.make_order_request(request)
        client_order_id = str(request.client_order_id)
        try:
            order = await self._backend.submit_order(native_request)
        except asyncio.TimeoutError:
            try:
                order = await self._backend.get_order_by_client_id(client_order_id)
            except Exception as exc:
                raise outcome_unknown(client_order_id=client_order_id) from exc
            if order is None:
                raise outcome_unknown(client_order_id=client_order_id)
        return map_order_result(order)

    async def place_future_order(self, request: FutureOrderRequest) -> OrderResult:
        del request
        raise unsupported("futures_trading")

    async def place_option_order(self, request: OptionOrderRequest) -> OrderResult:
        del request
        raise unsupported("options_trading")

    async def cancel_order(self, order_id: int | str) -> None:
        await self.ensure_connected()
        identifier = str(order_id or "").strip()
        if not identifier:
            raise BrokerOrderError("Alpaca order ID is required")
        try:
            await self._backend.cancel_order(identifier)
        except BrokerConnectionError as exc:
            native = await self._backend.get_order_by_id(identifier)
            if native is not None and broker_status(value(native, "status")) in _TERMINAL:
                return
            raise exc

    async def request_open_orders(self) -> list[TradeUpdate]:
        await self.ensure_connected()
        return await self.request_open_orders_unchecked()

    async def request_completed_orders(self) -> list[TradeUpdate]:
        await self.ensure_connected()
        since = datetime.now(UTC) - timedelta(hours=self._settings.reconcile_lookback_hours)
        return [
            map_trade_update(order)
            for order in await self._backend.get_orders(status="closed", after=since)
        ]

    async def request_executions(
        self, since: datetime | str | None = None
    ) -> list[TradeUpdate]:
        await self.ensure_connected()
        if since is None:
            since_at = datetime.now(UTC) - timedelta(
                hours=self._settings.reconcile_lookback_hours
            )
        else:
            since_at = utc_datetime(since)
        activities = await self._backend.get_fill_activities(since_at)
        closed = await self._backend.get_orders(status="closed", after=since_at)
        orders = {text(value(item, "id")): item for item in closed}
        return [map_fill_activity(item, orders.get(text(value(item, "order_id")))) for item in activities]

    async def qualify_contract(self, contract: Mapping[str, Any]) -> dict[str, Any]:
        sec_type = text(contract.get("secType") or contract.get("sec_type") or "STK").upper()
        if sec_type not in {"STK", "ETF"}:
            raise unsupported("asset_class")
        symbol = text(contract.get("symbol")).strip().upper()
        if not symbol:
            raise BrokerContractError("symbol is required")
        asset = await self._backend.get_asset(symbol)
        if text(value(asset, "class") or value(asset, "asset_class")).lower() != "us_equity":
            raise BrokerContractError("Alpaca asset is not a US equity or ETF")
        if not bool(value(asset, "active", False)) or not bool(value(asset, "tradable", False)):
            raise BrokerContractError("Alpaca asset is not active and tradable")
        return {
            "symbol": symbol,
            "secType": sec_type,
            "exchange": text(value(asset, "exchange")) or "SMART",
            "currency": "USD",
            "tradable": True,
            "shortable": bool(value(asset, "shortable", False)),
            "fractionable": bool(value(asset, "fractionable", False)),
            "assetId": text(value(asset, "id")) or None,
        }

    async def request_contract_details(
        self, contract: Mapping[str, Any]
    ) -> list[dict[str, Any]]:
        return [await self.qualify_contract(contract)]

    async def request_option_parameters(
        self, *, symbol: str, con_id: int, sec_type: str = "STK", exchange: str = ""
    ) -> list[dict[str, Any]]:
        del symbol, con_id, sec_type, exchange
        raise unsupported("options")

    async def request_scanner_parameters(self) -> str:
        raise unsupported("scanner")

    async def request_scanner_data(
        self,
        payload: Mapping[str, Any],
        *,
        tag_filters: Iterable[Mapping[str, Any]] | None = None,
        stream_seconds: float | None = None,
    ) -> list[dict[str, Any]]:
        del payload, tag_filters, stream_seconds
        raise unsupported("scanner")

    async def request_market_snapshot(
        self,
        contract: Mapping[str, Any],
        *,
        generic_tick_list: str = "",
        snapshot_timeout: float | None = None,
    ) -> dict[str, Any]:
        del generic_tick_list, snapshot_timeout
        qualified = await self.qualify_contract(contract)
        symbol = qualified["symbol"]
        snapshot = await self._backend.get_snapshot(symbol)
        if snapshot is None:
            raise BrokerConnectionError(
                "Alpaca stock snapshot is unavailable", details={"symbol": symbol}
            )
        trade = value(snapshot, "latest_trade")
        quote = value(snapshot, "latest_quote")
        daily = value(snapshot, "daily_bar")
        return {
            "symbol": symbol,
            "bid": number(value(quote, "bid_price")) if quote is not None else None,
            "ask": number(value(quote, "ask_price")) if quote is not None else None,
            "bidSize": number(value(quote, "bid_size")) if quote is not None else None,
            "askSize": number(value(quote, "ask_size")) if quote is not None else None,
            "last": number(value(trade, "price")) if trade is not None else None,
            "lastSize": number(value(trade, "size")) if trade is not None else None,
            "close": number(value(daily, "close")) if daily is not None else None,
            "timestamp": utc_datetime(
                value(trade, "timestamp") or value(quote, "timestamp")
            ).isoformat(),
            "dataFeed": self._settings.data_feed,
        }

    async def request_option_snapshot(
        self, contract: Mapping[str, Any], *, snapshot_timeout: float | None = None
    ) -> dict[str, Any]:
        del contract, snapshot_timeout
        raise unsupported("options")

    async def request_option_greeks(
        self, contract: Mapping[str, Any], *, snapshot_timeout: float | None = None
    ) -> dict[str, Any]:
        del contract, snapshot_timeout
        raise unsupported("options")

    def _timeframe(self, bar_size: str) -> Any:
        timeframe = self._backend.make_timeframe(bar_size)
        if timeframe is None:
            raise unsupported("bar_size")
        return timeframe

    async def get_historical_data(
        self,
        contract: Mapping[str, Any],
        *,
        end_datetime: datetime | str | None = None,
        duration: str = "1 D",
        bar_size: str = "1 min",
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ) -> list[HistoricalBar]:
        if what_to_show.strip().upper() != "TRADES":
            raise unsupported("historical_what_to_show")
        if not use_rth:
            raise unsupported("extended_hours")
        qualified = await self.qualify_contract(contract)
        start, end = duration_window(duration, end_datetime)
        bars = await self._backend.get_bars(
            qualified["symbol"],
            timeframe=self._timeframe(bar_size),
            start=start,
            end=end,
        )
        return [map_bar(item) for item in bars]

    async def get_historical_ticks(
        self,
        contract: Mapping[str, Any],
        *,
        start_datetime: datetime | str | None = None,
        end_datetime: datetime | str | None = None,
        number_of_ticks: int = 1000,
        what_to_show: str = "MIDPOINT",
        use_rth: bool = False,
        ignore_size: bool = False,
    ) -> list[HistoricalTickBidAsk | HistoricalTickLast]:
        del contract, start_datetime, end_datetime, number_of_ticks, what_to_show, use_rth, ignore_size
        raise unsupported("historical_ticks")

    async def stream_historical_bars(
        self,
        contract: Mapping[str, Any],
        *,
        end_datetime: datetime | str | None = None,
        duration: str = "1 D",
        bar_size: str = "1 min",
        what_to_show: str = "TRADES",
        use_rth: bool = True,
        keep_up_to_date: bool = True,
        emit_history: bool = False,
    ) -> AsyncIterator[HistoricalBar]:
        qualified = await self.qualify_contract(contract)
        if " ".join(str(bar_size).strip().lower().split()) not in {"1 min", "1 mins"}:
            raise unsupported("live_bar_size")
        if not keep_up_to_date:
            raise BrokerContractError("stream_historical_bars requires keep_up_to_date=true")

        async def iterator() -> AsyncIterator[HistoricalBar]:
            if emit_history:
                for item in await self.get_historical_data(
                    qualified,
                    end_datetime=end_datetime,
                    duration=duration,
                    bar_size=bar_size,
                    what_to_show=what_to_show,
                    use_rth=use_rth,
                ):
                    yield item
            stream = await self._backend.stream_stock(qualified["symbol"], "bars")
            async for item in stream:
                yield map_bar(item)

        return iterator()

    async def stream_real_time_price(
        self, contract: Mapping[str, Any], *, snapshot: bool = False
    ) -> AsyncIterator[RealTimePrice]:
        qualified = await self.qualify_contract(contract)
        symbol = qualified["symbol"]

        async def iterator() -> AsyncIterator[RealTimePrice]:
            state: dict[str, float | None] = {
                "bid": None,
                "ask": None,
                "bid_size": None,
                "ask_size": None,
                "last": None,
                "last_size": None,
            }
            if snapshot:
                initial = await self.request_market_snapshot(qualified)
                state.update(
                    bid=initial.get("bid"),
                    ask=initial.get("ask"),
                    bid_size=initial.get("bidSize"),
                    ask_size=initial.get("askSize"),
                    last=initial.get("last"),
                    last_size=initial.get("lastSize"),
                )
                yield RealTimePrice(symbol=symbol, close=initial.get("close"), timestamp=utc_datetime(initial.get("timestamp")), **state)
            quote_stream = await self._backend.stream_stock(symbol, "quotes")
            trade_stream = await self._backend.stream_stock(symbol, "trades")
            queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue(maxsize=self._settings.stream_queue_size)

            async def pump(kind: str, source: AsyncIterator[Any]) -> None:
                try:
                    async for item in source:
                        await queue.put((kind, item))
                finally:
                    await queue.put(("closed", kind))

            tasks = [
                asyncio.create_task(pump("quote", quote_stream)),
                asyncio.create_task(pump("trade", trade_stream)),
            ]
            try:
                closed_streams = 0
                while closed_streams < 2:
                    kind, item = await queue.get()
                    if kind == "closed":
                        closed_streams += 1
                        continue
                    if kind == "quote":
                        state.update(
                            bid=number(value(item, "bid_price")),
                            ask=number(value(item, "ask_price")),
                            bid_size=number(value(item, "bid_size")),
                            ask_size=number(value(item, "ask_size")),
                        )
                    else:
                        state.update(
                            last=number(value(item, "price")),
                            last_size=number(value(item, "size")),
                        )
                    yield RealTimePrice(
                        symbol=symbol,
                        close=None,
                        timestamp=utc_datetime(value(item, "timestamp")),
                        **state,
                    )
            finally:
                for task in tasks:
                    task.cancel()
                for source in (quote_stream, trade_stream):
                    close = getattr(source, "close", None)
                    if callable(close):
                        result = close()
                        if inspect.isawaitable(result):
                            await result

        return iterator()

    async def stream_tick_by_tick_data(
        self,
        contract: Mapping[str, Any],
        *,
        tick_type: str = "BidAsk",
        number_of_ticks: int = 0,
        ignore_size: bool = False,
    ) -> AsyncIterator[TickByTickBidAsk | TickByTickLast | TickByTickMidPoint]:
        del number_of_ticks, ignore_size
        qualified = await self.qualify_contract(contract)
        symbol = qualified["symbol"]
        normalized = tick_type.strip().lower()
        if normalized not in {"bidask", "last", "alllast", "midpoint"}:
            raise unsupported("tick_type")

        async def iterator() -> AsyncIterator[TickByTickBidAsk | TickByTickLast | TickByTickMidPoint]:
            kind = "trades" if normalized in {"last", "alllast"} else "quotes"
            stream = await self._backend.stream_stock(symbol, kind)
            async for item in stream:
                if kind == "trades":
                    yield TickByTickLast(
                        time=utc_datetime(value(item, "timestamp")),
                        price=number(value(item, "price")),
                        size=number(value(item, "size")),
                        exchange=text(value(item, "exchange")) or None,
                    )
                    continue
                bid = number(value(item, "bid_price"))
                ask = number(value(item, "ask_price"))
                if normalized == "midpoint":
                    yield TickByTickMidPoint(
                        time=utc_datetime(value(item, "timestamp")),
                        mid_price=(bid + ask) / 2.0,
                    )
                else:
                    yield TickByTickBidAsk(
                        time=utc_datetime(value(item, "timestamp")),
                        bid_price=bid,
                        ask_price=ask,
                        bid_size=number(value(item, "bid_size")),
                        ask_size=number(value(item, "ask_size")),
                    )

        return iterator()

    async def stream_market_depth(
        self, contract: Mapping[str, Any], *, depth: int | None = None
    ) -> AsyncIterator[DOMSnapshot]:
        del contract, depth
        raise unsupported("dom")


def create_adapter(settings: Mapping[str, Any]) -> AlpacaPaperAdapter:
    return AlpacaPaperAdapter(settings)
