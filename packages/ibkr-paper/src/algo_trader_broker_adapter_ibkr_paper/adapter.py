"""IBKR Paper adapter plugin.

This module is intentionally outside broker_runner core and imports the IB
wrapper only when the plugin entrypoint is selected.
"""

from __future__ import annotations

import asyncio
import copy
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any, AsyncIterator, Awaitable, Callable, Iterable, Mapping

from algo_trader_broker_sdk import (
    AccountSummaryItem,
    BrokerAdapterManifest,
    BrokerCapabilities,
    BrokerCapabilityError,
    BrokerConnectionError,
    BrokerConnectionState,
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


def dom_not_supported() -> BrokerCapabilityError:
    return BrokerCapabilityError(
        "DOM strategies are deprecated and incompatible with broker runner v1",
        code="dom_not_supported",
    )


def options_trading_not_supported() -> BrokerCapabilityError:
    return BrokerCapabilityError(
        "Options trading is not supported by broker runner v1",
        code="options_trading_not_supported",
    )


def _map_ib_order_result(result: Any, *, open_close: str | None) -> OrderResult:
    native_status = str(result.status or "")
    status = (
        "PendingSubmit"
        if native_status in {"", "Inactive"}
        and float(result.filled or 0.0) == 0.0
        and not getattr(result, "rejection_reason", None)
        else native_status
    )
    return OrderResult(
        adapter_id="ibkr_paper",
        adapter_order_id=str(result.order_id),
        adapter_order_ref=(str(result.perm_id) if result.perm_id not in (None, "") else None),
        status=status,
        filled=float(result.filled or 0.0),
        remaining=float(result.remaining or 0.0),
        avg_fill_price=result.avg_fill_price,
        contract=dict(result.contract or {}),
        adapter_metadata={
            "schemaVersion": 1,
            "native": {"openClose": open_close or ""},
            "diagnostics": (
                {"initialNativeStatus": native_status}
                if status != native_status
                else {}
            ),
            "extensions": {},
        },
    )


def _ib_datetime_payload(value: Any) -> Any:
    if value is None:
        return value
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        else:
            parsed = parsed.astimezone(UTC)
        return parsed.strftime("%Y%m%d-%H:%M:%S")
    if not isinstance(value, str):
        return value
    token = value.strip()
    if not token:
        return value
    parsed: datetime | None = None
    for candidate, fmt in (
        (token, None),
        (token.replace("Z", "+00:00"), None),
        (token, "%Y%m%d-%H:%M:%S"),
        (token, "%Y%m%d %H:%M:%S"),
    ):
        try:
            parsed = (
                datetime.fromisoformat(candidate)
                if fmt is None
                else datetime.strptime(candidate, fmt)
            )
            break
        except ValueError:
            continue
    if parsed is None:
        return value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    else:
        parsed = parsed.astimezone(UTC)
    return parsed.strftime("%Y%m%d-%H:%M:%S")


def _ib_market_kwargs(kwargs: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(kwargs)
    for key in ("start_datetime", "end_datetime"):
        if key in normalized:
            normalized[key] = _ib_datetime_payload(normalized[key])
    return normalized


def _iter_exception_chain(exc: BaseException) -> Iterable[BaseException]:
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def _is_transient_ib_disconnect(exc: BaseException) -> bool:
    for item in _iter_exception_chain(exc):
        if isinstance(item, (asyncio.TimeoutError, ConnectionError)):
            return True
        name = type(item).__name__.lower()
        text = str(item).lower()
        if name in {"ibconnectionerror", "connecterror", "connecttimeout", "readtimeout"}:
            return True
        if any(
            token in text
            for token in (
                "lost connection",
                "unable to connect",
                "disconnected client",
                "not connected",
                "connection reset",
                "all connection attempts failed",
                "timed out",
            )
        ):
            return True
    return False


class IBKRPaperAdapter:
    adapter_id = "ibkr_paper"

    def __init__(
        self,
        settings: Mapping[str, Any],
        *,
        client: Any | None = None,
        supervisor: Any | None = None,
    ) -> None:
        if client is not None:
            self._settings = settings
            self._client = client
            self._supervisor = supervisor or _NoopSupervisor()
            self._qualification_timeout = _float(
                settings.get("ib_qualification_timeout")
                or settings.get("ib_contract_qualification_timeout"),
                8.0,
            )
            return

        from .client import IBAsyncClient
        from .settings import IBGatewaySettings
        from .supervisor import IBClientSupervisor

        ib_settings = IBGatewaySettings(
            host=str(settings.get("ib_gateway_host") or settings.get("ib_host") or "127.0.0.1"),
            port=_int(settings.get("ib_gateway_port") or settings.get("ib_port"), 4002),
            client_id=_int(settings.get("ib_client_id"), 40),
            connect_timeout=_float(settings.get("ib_connect_timeout"), 15.0),
            read_only=_bool(settings.get("ib_read_only"), False),
            account=_optional_str(settings.get("ib_account")),
            market_data_depth_rows=_int(settings.get("ib_market_data_depth_rows"), 10),
            market_data_queue_size=_int(settings.get("ib_market_data_queue_size"), 64),
            historical_timeout=_float(settings.get("ib_historical_timeout"), 300.0),
        )
        ib_settings.validate()
        self._settings = ib_settings
        self._client = IBAsyncClient(ib_settings)
        self._supervisor = IBClientSupervisor(lambda: self._client, name="broker-runner-ibkr-paper")
        self._qualification_timeout = _float(
            settings.get("ib_qualification_timeout")
            or settings.get("ib_contract_qualification_timeout"),
            8.0,
        )

    async def start(self) -> None:
        await self._supervisor.start()

    async def close(self) -> None:
        await self._supervisor.stop()

    async def connect(self) -> None:
        await self._client.connect()

    async def disconnect(self, reason: str | None = None) -> None:
        await self._client.disconnect(reason=reason)

    async def reconnect(self, *, reason: str | None = None) -> None:
        await self._client.reconnect(reason=reason)

    async def ensure_connected(self) -> None:
        await self._client.ensure_connected()

    async def _call_with_transient_reconnect(
        self,
        operation: str,
        call_factory: Callable[[], Awaitable[Any]],
        *,
        attempts: int = 2,
    ) -> Any:
        attempts = max(1, attempts)
        last_exc: BaseException | None = None
        for attempt in range(attempts):
            try:
                return await call_factory()
            except Exception as exc:
                last_exc = exc
                if not _is_transient_ib_disconnect(exc):
                    raise
                reason = f"{operation}_ib_disconnect"
                if attempt >= attempts - 1:
                    raise BrokerConnectionError(
                        f"IB connection unavailable during {operation}",
                        details={
                            "operation": operation,
                            "reconnect_reason": reason,
                            "error_type": type(exc).__name__,
                            "error": str(exc),
                        },
                    ) from exc
                try:
                    await asyncio.wait_for(self.reconnect(reason=reason), timeout=5.0)
                except (Exception, asyncio.CancelledError):
                    pass
                await asyncio.sleep(0.2 * (attempt + 1))
        if last_exc is not None:
            raise BrokerConnectionError(
                f"IB connection unavailable during {operation}",
                details={"operation": operation, "error": str(last_exc)},
            ) from last_exc
        raise BrokerConnectionError(
            f"IB connection unavailable during {operation}",
            details={"operation": operation},
        )

    def manifest(self) -> BrokerAdapterManifest:
        return BrokerAdapterManifest(
            adapter_id=self.adapter_id,
            display_name="IBKR Paper Adapter",
            adapter_version="0.1.0",
            protocol_version="1.0",
            environment="PAPER",
            entrypoint="algo_trader_broker_adapter_ibkr_paper:create_adapter",
            capabilities=self.capabilities(),
        )

    def capabilities(self) -> BrokerCapabilities:
        return BrokerCapabilities(
            adapter_name=self.adapter_id,
            environment="PAPER",
            asset_classes={"STK", "FUT"},
            order_types={"MKT", "LMT", "STP", "STP LMT"},
            time_in_force={"DAY", "GTC"},
            market_data_streams={"historical_bars", "realtime_price", "tick_by_tick"},
            account_features={"summary", "positions", "pnl", "position_updates"},
            supports_fractional=False,
            supports_shorting=True,
            supports_replace=False,
            supports_partial_fills=True,
            supports_scanner=True,
            supports_options=False,
            supports_futures=True,
        )

    def connection_state_snapshot(self) -> BrokerConnectionState:
        snapshot = self._client.connection_state_snapshot()
        return BrokerConnectionState(
            connected=bool(snapshot.get("connected")),
            adapter=self.adapter_id,
            state="connected" if snapshot.get("connected") else "disconnected",
            connected_since=snapshot.get("connected_since"),
            reconnect_reason=snapshot.get("reconnect_reason"),
            host=snapshot.get("host"),
            port=snapshot.get("port"),
            client_id=snapshot.get("client_id"),
        )

    def set_trade_update_handler(
        self, handler: Callable[[TradeUpdate], Awaitable[None]] | None
    ) -> None:
        self._client.set_trade_update_handler(handler)

    def set_position_update_handler(
        self, handler: Callable[[list[PositionItem]], Awaitable[None]] | None
    ) -> None:
        self._client.set_position_update_handler(handler)  # type: ignore[arg-type]

    def set_account_update_handler(
        self, handler: Callable[[list[AccountSummaryItem]], Awaitable[None]] | None
    ) -> None:
        self._client.set_account_update_handler(handler)  # type: ignore[arg-type]

    def add_connection_listener(
        self, listener: Callable[[str, Mapping[str, Any]], Awaitable[None] | None]
    ) -> None:
        self._client.add_connection_listener(listener)

    def remove_connection_listener(
        self, listener: Callable[[str, Mapping[str, Any]], Awaitable[None] | None]
    ) -> None:
        self._client.remove_connection_listener(listener)

    def add_resub_task(self, coro_factory: Callable[[], Awaitable[None]]) -> None:
        self._client.add_resub_task(coro_factory)

    async def get_account_summary(self, account: str | None = None) -> list[AccountSummaryItem]:
        return await self._client.get_account_summary(account)  # type: ignore[return-value]

    async def get_account_pnl(
        self,
        account: str | None = None,
        *,
        model_code: str | None = None,
        timeout: float = 5.0,
    ) -> dict[str, float | None] | None:
        # Let IBAsyncClient resolve the managed account. Passing account IDs
        # through broker_runner creates noisy URLs and does not improve reqPnL.
        return await self._client.get_account_pnl(None, model_code=model_code, timeout=timeout)

    async def get_positions(self) -> list[PositionItem]:
        return await self._client.get_positions()  # type: ignore[return-value]

    async def place_stock_order(self, request: StockOrderRequest) -> OrderResult:
        from .orders import StockOrderRequest as IBStockOrderRequest

        payload = asdict(request)
        adapter_parameters = payload.pop("adapter_parameters", {})
        if adapter_parameters:
            raise BrokerOrderError(
                "IBKR Paper does not accept unrecognized adapter_parameters",
                code="invalid_adapter_parameters",
                details={"keys": sorted(adapter_parameters)},
            )
        effect = payload.pop("position_effect", None)
        native_order_id = payload.pop("adapter_order_id", None)
        payload["open_close"] = "O" if effect == "OPEN" else "C" if effect == "CLOSE" else None
        if native_order_id is not None:
            try:
                payload["order_id"] = int(native_order_id)
            except (TypeError, ValueError) as exc:
                raise BrokerOrderError("IBKR order identifier must be numeric") from exc
        result = await self._client.place_stock_order(IBStockOrderRequest(**payload))
        return _map_ib_order_result(result, open_close=payload.get("open_close"))

    async def place_future_order(self, request: FutureOrderRequest) -> OrderResult:
        from .orders import FutureOrderRequest as IBFutureOrderRequest

        payload = asdict(request)
        adapter_parameters = payload.pop("adapter_parameters", {})
        if adapter_parameters:
            raise BrokerOrderError(
                "IBKR Paper does not accept unrecognized adapter_parameters",
                code="invalid_adapter_parameters",
                details={"keys": sorted(adapter_parameters)},
            )
        effect = payload.pop("position_effect", None)
        native_order_id = payload.pop("adapter_order_id", None)
        payload["open_close"] = "O" if effect == "OPEN" else "C" if effect == "CLOSE" else None
        if native_order_id is not None:
            try:
                payload["order_id"] = int(native_order_id)
            except (TypeError, ValueError) as exc:
                raise BrokerOrderError("IBKR order identifier must be numeric") from exc
        result = await self._client.place_future_order(IBFutureOrderRequest(**payload))
        return _map_ib_order_result(result, open_close=payload.get("open_close"))

    async def place_option_order(self, request: OptionOrderRequest) -> OrderResult:
        raise options_trading_not_supported()

    async def cancel_order(self, order_id: int | str) -> None:
        await self._client.cancel_order(order_id)

    async def request_open_orders(self) -> list[TradeUpdate]:
        return await self._client.request_open_orders()

    async def request_executions(self, since: datetime | str | None = None) -> list[TradeUpdate]:
        return await self._client.request_executions(since)

    async def request_completed_orders(self) -> list[TradeUpdate]:
        return await self._client.request_completed_orders()

    def _build_contract(self, contract: Mapping[str, Any] | Any) -> Any:
        if not isinstance(contract, Mapping):
            return contract
        from .contract_builder import ContractParams, IBContractFactory

        params = ContractParams(
            symbol=str(contract.get("symbol") or ""),
            sec_type=str(contract.get("secType") or contract.get("sec_type") or "STK"),
            exchange=str(contract.get("exchange") or "SMART"),
            currency=str(contract.get("currency") or "USD"),
            primary_exchange=contract.get("primaryExchange") or contract.get("primary_exchange"),
            local_symbol=contract.get("localSymbol") or contract.get("local_symbol"),
            trading_class=contract.get("tradingClass") or contract.get("trading_class"),
            con_id=contract.get("conId") or contract.get("contract_id"),
            last_trade_date_or_contract_month=(
                contract.get("lastTradeDateOrContractMonth")
                or contract.get("last_trade_date")
                or contract.get("contractMonth")
                or contract.get("contract_month")
            ),
            include_expired=contract.get("includeExpired") or contract.get("include_expired"),
            multiplier=contract.get("multiplier"),
            metadata=contract,
        )
        return IBContractFactory().build(params).contract

    async def _qualify_contract_object(self, contract: Mapping[str, Any] | Any) -> Any:
        built = self._build_contract(contract)
        qualified = await asyncio.wait_for(
            self._client.qualify_contract(built),
            timeout=max(1.0, self._qualification_timeout),
        )
        if qualified is not None:
            return qualified
        fallback = copy.copy(built)
        if getattr(fallback, "conId", None):
            fallback.conId = 0
        return fallback

    async def _raise_qualification_timeout(
        self, *, reason: str, operation: str
    ) -> None:
        try:
            await asyncio.wait_for(self.reconnect(reason=reason), timeout=5.0)
        except (Exception, asyncio.CancelledError):
            pass
        raise BrokerConnectionError(
            f"IB contract qualification timed out during {operation}",
            details={
                "operation": operation,
                "timeout_seconds": max(1.0, self._qualification_timeout),
                "reconnect_reason": reason,
            },
        )

    async def qualify_contract(self, contract: Mapping[str, Any]) -> dict[str, Any]:
        try:
            qualified = await self._qualify_contract_object(contract)
        except asyncio.TimeoutError:
            await self._raise_qualification_timeout(
                reason="contract_qualification_timeout",
                operation="qualify_contract",
            )
        return _contract_to_payload(qualified)

    async def request_contract_details(self, contract: Mapping[str, Any]) -> list[dict[str, Any]]:
        try:
            qualified = await self._qualify_contract_object(contract)
        except asyncio.TimeoutError:
            await self._raise_qualification_timeout(
                reason="contract_details_qualification_timeout",
                operation="request_contract_details",
            )
        return await self._client.request_contract_details(qualified)

    async def request_option_parameters(
        self, *, symbol: str, con_id: int, sec_type: str = "STK", exchange: str = ""
    ) -> list[dict[str, Any]]:
        raise options_trading_not_supported()

    async def request_scanner_parameters(self) -> str:
        return await self._client.request_scanner_parameters()

    async def request_scanner_data(
        self,
        payload: Mapping[str, Any],
        *,
        tag_filters: Iterable[Mapping[str, Any]] | None = None,
        stream_seconds: float | None = None,
    ) -> list[dict[str, Any]]:
        return await self._client.request_scanner_data(
            payload, tag_filters=tag_filters, stream_seconds=stream_seconds
        )

    async def request_market_snapshot(
        self,
        contract: Mapping[str, Any],
        *,
        generic_tick_list: str = "",
        snapshot_timeout: float | None = None,
    ) -> dict[str, Any]:
        try:
            qualified = await self._qualify_contract_object(contract)
        except asyncio.TimeoutError:
            await self._raise_qualification_timeout(
                reason="market_snapshot_qualification_timeout",
                operation="request_market_snapshot",
            )
        return await self._call_with_transient_reconnect(
            "request_market_snapshot",
            lambda: self._client.request_market_snapshot(
                qualified,
                generic_tick_list=generic_tick_list,
                snapshot_timeout=snapshot_timeout,
            ),
        )

    async def request_option_snapshot(
        self, contract: Mapping[str, Any], *, snapshot_timeout: float | None = None
    ) -> dict[str, Any]:
        raise options_trading_not_supported()

    async def request_option_greeks(
        self, contract: Mapping[str, Any], *, snapshot_timeout: float | None = None
    ) -> dict[str, Any]:
        raise options_trading_not_supported()

    async def get_historical_data(self, contract: Mapping[str, Any], **kwargs: Any) -> list[HistoricalBar]:
        try:
            qualified = await self._qualify_contract_object(contract)
        except asyncio.TimeoutError:
            await self._raise_qualification_timeout(
                reason="historical_data_qualification_timeout",
                operation="get_historical_data",
            )
        market_kwargs = _ib_market_kwargs(kwargs)
        return await self._call_with_transient_reconnect(
            "get_historical_data",
            lambda: self._client.get_historical_data(qualified, **market_kwargs),
        )  # type: ignore[return-value]

    async def get_historical_ticks(
        self, contract: Mapping[str, Any], **kwargs: Any
    ) -> list[HistoricalTickBidAsk | HistoricalTickLast]:
        try:
            qualified = await self._qualify_contract_object(contract)
        except asyncio.TimeoutError:
            await self._raise_qualification_timeout(
                reason="historical_ticks_qualification_timeout",
                operation="get_historical_ticks",
            )
        market_kwargs = _ib_market_kwargs(kwargs)
        return await self._call_with_transient_reconnect(
            "get_historical_ticks",
            lambda: self._client.get_historical_ticks(qualified, **market_kwargs),
        )  # type: ignore[return-value]

    async def stream_historical_bars(self, contract: Mapping[str, Any], **kwargs: Any) -> AsyncIterator[HistoricalBar]:
        try:
            qualified = await self._qualify_contract_object(contract)
        except asyncio.TimeoutError:
            await self._raise_qualification_timeout(
                reason="historical_stream_qualification_timeout",
                operation="stream_historical_bars",
            )
        market_kwargs = _ib_market_kwargs(kwargs)
        return await self._call_with_transient_reconnect(
            "stream_historical_bars",
            lambda: self._client.stream_historical_bars(qualified, **market_kwargs),
        )  # type: ignore[return-value]

    async def stream_real_time_price(
        self, contract: Mapping[str, Any], *, snapshot: bool = False
    ) -> AsyncIterator[RealTimePrice]:
        try:
            qualified = await self._qualify_contract_object(contract)
        except asyncio.TimeoutError:
            await self._raise_qualification_timeout(
                reason="realtime_price_qualification_timeout",
                operation="stream_real_time_price",
            )
        return await self._call_with_transient_reconnect(
            "stream_real_time_price",
            lambda: self._client.stream_real_time_price(qualified, snapshot=snapshot),
        )  # type: ignore[return-value]

    async def stream_tick_by_tick_data(self, contract: Mapping[str, Any], **kwargs: Any) -> AsyncIterator[TickByTickBidAsk | TickByTickLast | TickByTickMidPoint]:
        try:
            qualified = await self._qualify_contract_object(contract)
        except asyncio.TimeoutError:
            await self._raise_qualification_timeout(
                reason="tick_by_tick_qualification_timeout",
                operation="stream_tick_by_tick_data",
            )
        market_kwargs = _ib_market_kwargs(kwargs)
        return await self._call_with_transient_reconnect(
            "stream_tick_by_tick_data",
            lambda: self._client.stream_tick_by_tick_data(qualified, **market_kwargs),
        )  # type: ignore[return-value]

    async def stream_market_depth(
        self, contract: Mapping[str, Any], *, depth: int | None = None
    ) -> AsyncIterator[DOMSnapshot]:
        raise dom_not_supported()


def create_adapter(settings: Mapping[str, Any]) -> IBKRPaperAdapter:
    return IBKRPaperAdapter(settings)


class _NoopSupervisor:
    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None


def _contract_to_payload(contract: Any) -> dict[str, Any]:
    return {
        "conId": getattr(contract, "conId", None),
        "symbol": getattr(contract, "symbol", None),
        "secType": getattr(contract, "secType", None),
        "exchange": getattr(contract, "exchange", None),
        "currency": getattr(contract, "currency", None),
        "localSymbol": getattr(contract, "localSymbol", None),
        "tradingClass": getattr(contract, "tradingClass", None),
        "primaryExchange": getattr(contract, "primaryExchange", None),
    }


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _int(value: Any, default: int) -> int:
    try:
        if value in (None, ""):
            return default
        return int(value)
    except Exception:
        return default


def _float(value: Any, default: float) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _bool(value: Any, default: bool) -> bool:
    if value in (None, ""):
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}
