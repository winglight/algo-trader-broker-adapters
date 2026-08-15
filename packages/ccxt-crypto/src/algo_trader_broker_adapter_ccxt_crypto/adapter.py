"""OKX Demo Spot implementation behind the reviewed ``ccxt_crypto`` profile."""

from __future__ import annotations

import asyncio
import inspect
import logging
import random
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from contextlib import suppress
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from algo_trader_broker_adapter_ccxt_crypto_perpetual import PerpetualContext

from algo_trader_broker_sdk import (
    AccountSummaryItem,
    BrokerAdapterManifest,
    BrokerCapabilities,
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
    TickByTickLast,
    TradeUpdate,
)

from .client import OKXDemoClient
from .errors import order_error, unknown_outcome, unsupported
from .fee_policy import CryptoSpotFeePolicy, reported_fee_tier
from .mapping import (
    account_summary,
    legacy_fill_update,
    legacy_positions,
    legacy_trade_update,
    order_update,
    timestamp,
)
from .quantizer import MarketRules, canonical, native_client_order_id
from .reconciliation import Reconciler, clock_skew_ms
from .settings import CCXTCryptoSettings

_SYMBOL_BY_INSTRUMENT = {
    "crypto-spot:BTC-USDT:OKX": "BTC/USDT",
    "crypto-spot:ETH-USDT:OKX": "ETH/USDT",
}
LOGGER = logging.getLogger(__name__)
_PUBLIC_TICKER_MAX_AGE_MS = 120_000
_PUBLIC_TRADE_MAX_AGE_MS = 120_000
_PUBLIC_TRADE_DEDUP_WINDOW = 4_096


def _field(payload: Mapping[str, Any], camel: str, snake: str | None = None) -> Any:
    if camel in payload:
        return payload[camel]
    return payload.get(snake or camel)


class CCXTCryptoAdapter:
    adapter_id = "ccxt_crypto"

    def __init__(
        self,
        settings: Mapping[str, Any],
        *,
        backend: Any | None = None,
        perpetual_backend: Any | None = None,
    ) -> None:
        self._settings = CCXTCryptoSettings.from_mapping(settings)
        self._client = backend or OKXDemoClient(self._settings)
        self._perpetual: PerpetualContext | None = None
        if self._settings.perpetual_enabled:
            self._perpetual = PerpetualContext(
                {
                    "exchange_id": "okx",
                    "sandbox": True,
                    "live": False,
                    "api_key": self._settings.api_key,
                    "secret": self._settings.secret,
                    "passphrase": self._settings.passphrase,
                    "allowed_symbols": self._settings.perpetual_allowed_symbols,
                    "execution_target_id": self._settings.perpetual_execution_target_id,
                    "market_data_target_id": self._settings.perpetual_market_data_target_id,
                    "account_id": self._settings.perpetual_account_id,
                    "position_mode": "ONE_WAY",
                    "margin_mode": "ISOLATED",
                    "fixed_leverage": self._settings.perpetual_fixed_leverage,
                    "request_timeout_ms": self._settings.request_timeout_ms,
                    "reconcile_interval_seconds": self._settings.reconcile_interval_seconds,
                    "clock_skew_block_ms": self._settings.clock_skew_block_ms,
                },
                backend=perpetual_backend,
            )
        self._reconciler = Reconciler(self._client, self._settings)
        self._connected = False
        self._state = "installed"
        self._connected_since: datetime | None = None
        self._reconnect_reason: str | None = None
        self._generation = 0
        self._rules: dict[str, MarketRules] = {}
        self._order_symbols: dict[str, str] = {}
        self._balance: Mapping[str, Any] = {}
        self._prices_in_usdt: dict[str, Decimal] = {"USDT": Decimal(1)}
        self._trade_update_handler: Callable[[TradeUpdate], Awaitable[None]] | None = None
        self._position_update_handler: Callable[[list[PositionItem]], Awaitable[None]] | None = None
        self._account_update_handler: Callable[[list[AccountSummaryItem]], Awaitable[None]] | None = None
        self._reconciliation_handler: (
            Callable[[Mapping[str, Any], int], Awaitable[None] | None] | None
        ) = None
        self._connection_listeners: list[
            Callable[[str, Mapping[str, Any]], Awaitable[None] | None]
        ] = []
        self._resub_tasks: list[Callable[[], Awaitable[None]]] = []
        self._stream_tasks: list[asyncio.Task[None]] = []
        self._recovery_task: asyncio.Task[None] | None = None
        self._closing = False
        self._metadata_approval_required = False
        self._fee_policy = CryptoSpotFeePolicy()
        self._reported_fee_tiers: dict[str, dict[str, str | None]] = {}
        self._public_streams_ready: set[str] = set()
        self._private_streams_ready: set[str] = set()
        self._lifecycle_lock = asyncio.Lock()

    def manifest(self) -> BrokerAdapterManifest:
        return BrokerAdapterManifest(
            adapter_id=self.adapter_id,
            display_name="OKX Demo Spot + USDT Perpetual (CCXT)",
            adapter_version="0.1.0",
            protocol_version="1.0",
            environment="PAPER",
            entrypoint="algo_trader_broker_adapter_ccxt_crypto:create_adapter",
            capabilities=self.capabilities(),
        )

    def capabilities(self) -> BrokerCapabilities:
        return BrokerCapabilities(
            adapter_name=self.adapter_id,
            environment="PAPER",
            asset_classes=(
                {"CRYPTO_SPOT", "CRYPTO_PERPETUAL"}
                if self._perpetual is not None
                else {"CRYPTO_SPOT"}
            ),
            order_types={"MKT", "LMT"},
            time_in_force={"GTC", "IOC"} if self._perpetual is not None else {"GTC"},
            market_data_streams=(
                {"historical_bars", "realtime_price", "tick_by_tick", "mark", "index", "funding"}
                if self._perpetual is not None
                else {"historical_bars", "realtime_price", "tick_by_tick"}
            ),
            account_features=(
                {
                    "summary",
                    "positions",
                    "position_updates",
                    "reconciliation_v2",
                    "position_risk_v1",
                    "funding_ledger_v1",
                }
                if self._perpetual is not None
                else {"summary", "positions", "position_updates", "reconciliation_v2"}
            ),
            supports_fractional=True,
            supports_shorting=self._perpetual is not None,
            supports_replace=False,
            supports_partial_fills=True,
            supports_scanner=False,
            supports_options=False,
            supports_futures=self._perpetual is not None,
            default_asset_class="CRYPTO_SPOT",
            symbol_examples={
                "CRYPTO_SPOT": list(self._settings.allowed_symbols),
                **(
                    {"CRYPTO_PERPETUAL": list(self._settings.perpetual_allowed_symbols)}
                    if self._perpetual is not None
                    else {}
                ),
            },
            native={
                "exchangeId": "okx",
                "paperOnly": True,
                "sandboxRequired": True,
                "simulatedTradingHeaderRequired": True,
                "executionTargetId": self._settings.execution_target_id,
                "marketDataTargetId": self._settings.market_data_target_id,
                "executionTargets": {
                    "CRYPTO_SPOT": self._settings.execution_target_id,
                    **(
                        {
                            "CRYPTO_PERPETUAL": self._settings.perpetual_execution_target_id
                        }
                        if self._perpetual is not None
                        else {}
                    ),
                },
                "marketDataTargets": {
                    "CRYPTO_SPOT": self._settings.market_data_target_id,
                    **(
                        {
                            "CRYPTO_PERPETUAL": self._settings.perpetual_market_data_target_id
                        }
                        if self._perpetual is not None
                        else {}
                    ),
                },
                "targetCapabilities": {
                    "CRYPTO_SPOT": {
                        "supportsFractional": True,
                        "supportsShorting": False,
                        "timeInForce": ["GTC"],
                    },
                    **(
                        {
                            "CRYPTO_PERPETUAL": {
                                "supportsFractional": False,
                                "supportsShorting": True,
                                "timeInForce": ["GTC", "IOC"],
                            }
                        }
                        if self._perpetual is not None
                        else {}
                    ),
                },
                "perpetualPolicy": {
                    "positionMode": "ONE_WAY",
                    "marginMode": "ISOLATED",
                    "fixedLeverage": "2",
                } if self._perpetual is not None else None,
                "allowedSymbols": list(self._settings.allowed_symbols),
            },
        )

    async def start(self) -> None:
        await self.connect()

    async def connect(self) -> None:
        async with self._lifecycle_lock:
            if self._connected:
                return
            self._closing = False
            await self._load_and_validate_markets()
            skew = clock_skew_ms(await self._client.fetch_time())
            if skew > self._settings.clock_skew_block_ms:
                raise BrokerConnectionError(
                    "OKX clock skew exceeds the configured block threshold",
                    details={"clock_skew_ms": skew},
                )
            fee_payloads = await asyncio.gather(
                *(
                    self._client.fetch_trading_fee(symbol)
                    for symbol in self._settings.allowed_symbols
                )
            )
            self._reported_fee_tiers = {
                symbol: reported_fee_tier(payload)
                for symbol, payload in zip(
                    self._settings.allowed_symbols,
                    fee_payloads,
                    strict=True,
                )
            }
            snapshot = await self._capture_reconciliation(
                generation=self._generation + 1
            )
            self._balance = await self._client.fetch_balance()
            await self._refresh_valuation_prices()
            if snapshot.get("orderUpdates") is None:
                raise BrokerConnectionError("OKX initial reconciliation did not complete")
            if self._perpetual is not None:
                await self._perpetual.start()
            self._start_private_streams()
            next_state = "trading_ready"
            self._generation += 1
            self._connected = True
            self._connected_since = datetime.now(UTC)
            self._set_state(next_state, reason="connect")
            await self._notify_connection(
                "connected",
                {
                    "adapter_id": self.adapter_id,
                    "state": self._state,
                    "generation": self._generation,
                    "sandbox": True,
                },
            )

    async def close(self) -> None:
        async with self._lifecycle_lock:
            self._closing = True
            recovery, self._recovery_task = self._recovery_task, None
            if recovery is not None:
                recovery.cancel()
                with suppress(asyncio.CancelledError):
                    await recovery
            tasks, self._stream_tasks = self._stream_tasks, []
            for task in tasks:
                task.cancel()
            for task in tasks:
                with suppress(asyncio.CancelledError):
                    await task
            await self._client.close()
            if self._perpetual is not None:
                await self._perpetual.close()
            self._connected = False
            self._set_state("disconnected", reason="closed")
            await self._notify_connection("disconnected", {"reason": "closed"})

    async def disconnect(self, reason: str | None = None) -> None:
        self._reconnect_reason = reason
        recovery, self._recovery_task = self._recovery_task, None
        if recovery is not None and recovery is not asyncio.current_task():
            recovery.cancel()
            with suppress(asyncio.CancelledError):
                await recovery
        tasks, self._stream_tasks = self._stream_tasks, []
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task
        if self._perpetual is not None:
            await self._perpetual.disconnect(reason)
        self._connected = False
        self._set_state("disconnected", reason=reason or "disconnect")
        await self._notify_connection(
            "disconnected",
            {
                "reason": reason,
                "executionTargetId": self._settings.execution_target_id,
                "marketDataTargetId": self._settings.market_data_target_id,
            },
        )

    async def reconnect(self, *, reason: str | None = None) -> None:
        await self.disconnect(reason=reason)
        await self.connect()
        await self._capture_reconciliation()
        for factory in tuple(self._resub_tasks):
            await factory()

    async def ensure_connected(self) -> None:
        if not self._connected:
            raise BrokerConnectionError("OKX Demo adapter is not connected")

    def connection_state_snapshot(self) -> BrokerConnectionState:
        return BrokerConnectionState(
            connected=self._connected,
            adapter=self.adapter_id,
            state=self._state,
            connected_since=self._connected_since,
            reconnect_reason=self._reconnect_reason,
            host="OKX Demo" if self._connected else None,
        )

    def connection_diagnostics(self) -> dict[str, Any]:
        evidence = self._client.sandbox_evidence() if hasattr(self._client, "sandbox_evidence") else {}
        try:
            spot_reconciliation_ready = bool(
                self._reconciler.cached_snapshot().get("executionTargetId")
            )
        except BrokerConnectionError:
            spot_reconciliation_ready = False
        perpetual = (
            self._perpetual.connection_diagnostics()
            if self._perpetual is not None
            else None
        )
        return {
            "adapter_id": self.adapter_id,
            "state": self._state,
            "generation": self._generation,
            "metadataApprovalRequired": self._metadata_approval_required,
            "feePolicy": self._fee_policy.as_dict(),
            "reportedFeeTiers": self._reported_fee_tiers,
            "sandbox": True,
            "live": False,
            "contexts": {
                "CRYPTO_SPOT": {
                    "executionTargetId": self._settings.execution_target_id,
                    "marketDataTargetId": self._settings.market_data_target_id,
                    "state": self._state,
                    "reconciliationReady": spot_reconciliation_ready,
                },
                **(
                    {
                        "CRYPTO_PERPETUAL": {
                            "executionTargetId": self._settings.perpetual_execution_target_id,
                            "marketDataTargetId": self._settings.perpetual_market_data_target_id,
                            "state": perpetual.get("state"),
                            "generation": perpetual.get("generation"),
                            "reconciliationReady": perpetual.get("reconciliationReady"),
                        }
                    }
                    if isinstance(perpetual, Mapping)
                    else {}
                ),
            },
            **evidence,
            **self._settings.redacted(),
        }

    async def _load_and_validate_markets(self) -> None:
        markets = await self._client.load_markets(self._settings.allowed_symbols)
        rules: dict[str, MarketRules] = {}
        for symbol in self._settings.allowed_symbols:
            market = markets.get(symbol)
            if not isinstance(market, Mapping):
                raise BrokerConnectionError(
                    "An allowlisted OKX Demo symbol is unavailable",
                    details={"symbol": symbol},
                )
            rule = MarketRules.from_ccxt(
                symbol, market, minimum_notional=self._settings.minimum_notional
            )
            if not rule.active:
                raise BrokerConnectionError(
                    "An allowlisted OKX Demo symbol is not live",
                    details={"symbol": symbol},
                )
            rules[symbol] = rule
        self._rules = rules
        LOGGER.warning(
            "OKX Demo market metadata accepted",
            extra={
                "event": "broker.crypto.metadata_accepted",
                "broker.adapter_id": self.adapter_id,
                "broker.execution_target_id": self._settings.execution_target_id,
                "broker.instruments": sorted(
                    rule.instrument_id for rule in rules.values()
                ),
                "broker.metadata_hashes": {
                    symbol: rule.metadata_hash for symbol, rule in sorted(rules.items())
                },
            },
        )

    async def market_metadata_v2(
        self, execution_target_id: str | None = None
    ) -> list[dict[str, Any]]:
        await self.ensure_connected()
        if execution_target_id == self._settings.perpetual_execution_target_id:
            if self._perpetual is None:
                raise BrokerConnectionError("Perpetual execution context is disabled")
            return await self._perpetual.market_metadata_v2()
        if execution_target_id not in {None, self._settings.execution_target_id}:
            raise BrokerConnectionError("Unknown execution target")
        if not self._rules:
            await self._load_and_validate_markets()
        return [
            {
                "instrumentId": rule.instrument_id,
                "symbol": rule.symbol,
                "nativeInstrumentId": rule.native_instrument_id,
                "metadataVersion": 1,
                "metadataHash": rule.metadata_hash,
                "tickSize": canonical(rule.tick_size),
                "lotSize": canonical(rule.lot_size),
                "minSize": canonical(rule.min_size),
                "maxLimitSize": canonical(rule.max_limit_size) if rule.max_limit_size else None,
                "maxMarketSize": canonical(rule.max_market_size) if rule.max_market_size else None,
                "minimumNotional": canonical(rule.minimum_notional),
                "active": rule.active,
            }
            for rule in self._rules.values()
        ]

    async def place_order_v2(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        await self.ensure_connected()
        target = str(_field(payload, "executionTargetId", "execution_target_id") or "")
        if target == self._settings.perpetual_execution_target_id:
            if self._perpetual is None:
                raise order_error("Perpetual execution context is disabled", code="execution_target_disabled")
            return await self._perpetual.place_order_v2(payload)
        if self._state != "trading_ready":
            raise order_error(
                "OKX Demo is not trading ready",
                code="trading_not_ready",
            )
        if target != self._settings.execution_target_id:
            raise order_error("execution target mismatch", code="execution_target_mismatch")
        instrument = str(_field(payload, "instrumentId", "instrument_id") or "")
        symbol = _SYMBOL_BY_INSTRUMENT.get(instrument)
        if not symbol or symbol not in self._settings.allowed_symbols:
            raise order_error("instrument is not allowlisted", code="instrument_not_allowed")
        if bool(_field(payload, "reduceOnly", "reduce_only")):
            raise order_error("reduceOnly is not supported for Spot", code="unsupported_order_semantics")
        if str(_field(payload, "positionEffect", "position_effect") or "") != "AUTO":
            raise order_error("Spot orders require positionEffect=AUTO", code="unsupported_order_semantics")
        order_type = str(_field(payload, "orderType", "order_type") or "").upper()
        if order_type not in {"MARKET", "LIMIT"}:
            raise order_error("only Market and Limit are supported", code="unsupported_order_type")
        if str(_field(payload, "timeInForce", "time_in_force") or "").upper() != "GTC":
            raise order_error("OKX Demo Phase 4 requires GTC", code="unsupported_time_in_force")
        if not self._rules:
            await self._load_and_validate_markets()
        rule = self._rules[symbol]
        quantity = rule.quantize_quantity(
            _field(payload, "quantityDecimal", "quantity_decimal"), market=order_type == "MARKET"
        )
        side = str(payload.get("side") or "").upper()
        if side not in {"BUY", "SELL"}:
            raise order_error("side must be BUY or SELL", code="invalid_order_side")
        price: Decimal | None = None
        if order_type == "LIMIT":
            price = rule.quantize_price(
                _field(payload, "limitPriceDecimal", "limit_price_decimal"), side=side
            )
            rule.validate_notional(quantity, price)
        else:
            ticker = await self._client.fetch_ticker(symbol)
            ticker_time = ticker.get("timestamp")
            if ticker_time in (None, ""):
                raise order_error("fresh bid/ask is required", code="market_price_unavailable")
            ticker_age = datetime.now(UTC).timestamp() - float(ticker_time) / 1000
            if ticker_age < -1 or ticker_age > 10:
                raise order_error(
                    "market bid/ask is stale",
                    code="market_price_stale",
                    details={"age_seconds": round(ticker_age, 3)},
                )
            reference = ticker.get("ask") if side == "BUY" else ticker.get("bid")
            if reference in (None, ""):
                raise order_error("fresh bid/ask is required", code="market_price_unavailable")
            rule.validate_notional(quantity, Decimal(str(reference)))
        full_client_id = str(_field(payload, "clientOrderId", "client_order_id") or "")
        native_client_id = native_client_order_id(full_client_id)
        params: dict[str, Any] = {"tdMode": "cash", "clOrdId": native_client_id}
        if order_type == "MARKET":
            params["tgtCcy"] = "base_ccy"
        command_id = str(_field(payload, "commandId", "command_id") or "")
        try:
            order = await self._client.create_order(
                symbol,
                order_type.lower(),
                side.lower(),
                canonical(quantity),
                canonical(price) if price is not None else None,
                params,
            )
        except Exception as exc:
            if self._mutation_outcome_is_unknown(exc):
                order = await self._client.fetch_order_by_client_id(native_client_id, symbol)
                if order is None:
                    raise self._unknown_order_outcome(
                        operation="place_order", client_order_id=full_client_id
                    ) from None
            else:
                raise order_error(
                    "OKX Demo rejected the order request",
                    code="provider_order_error",
                    details={"error_type": type(exc).__name__},
                ) from exc
        result = order_update(
            order,
            execution_target_id=self._settings.execution_target_id,
            command_id=command_id,
            client_order_id=full_client_id,
        )
        broker_id = result["identity"]["brokerOrderId"]
        if not broker_id:
            raise self._unknown_order_outcome(
                operation="place_order_acknowledgement",
                client_order_id=full_client_id,
            )
        self._order_symbols[broker_id] = symbol
        self._reconciler.record_order(order)
        return result

    async def cancel_order_v2(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        await self.ensure_connected()
        target = str(_field(payload, "executionTargetId", "execution_target_id") or "")
        if target == self._settings.perpetual_execution_target_id:
            if self._perpetual is None:
                raise order_error("Perpetual execution context is disabled", code="execution_target_disabled")
            return await self._perpetual.cancel_order_v2(payload)
        if target != self._settings.execution_target_id:
            raise order_error("execution target mismatch", code="execution_target_mismatch")
        instrument = str(_field(payload, "instrumentId", "instrument_id") or "")
        symbol = _SYMBOL_BY_INSTRUMENT.get(instrument)
        if not symbol or symbol not in self._settings.allowed_symbols:
            raise order_error("instrument is not allowlisted", code="instrument_not_allowed")
        order_id = str(_field(payload, "brokerOrderId", "broker_order_id") or "").strip()
        if not order_id:
            raise order_error("brokerOrderId is required", code="order_id_required")
        command_id = str(_field(payload, "commandId", "command_id") or "").strip()
        if not command_id:
            raise order_error("commandId is required", code="command_id_required")
        try:
            await self._client.cancel_order(order_id, symbol)
            order = await self._client.fetch_order(order_id, symbol)
        except Exception as exc:
            if not self._mutation_outcome_is_unknown(exc):
                raise order_error(
                    "OKX Demo rejected the cancel request",
                    code="provider_cancel_error",
                    details={"error_type": type(exc).__name__},
                ) from exc
            try:
                order = await self._client.fetch_order(order_id, symbol)
            except Exception:  # noqa: BLE001 - provider failure preserves UNKNOWN outcome
                raise self._unknown_order_outcome(
                    operation="cancel_order", client_order_id=order_id
                ) from None
        self._order_symbols[order_id] = symbol
        self._reconciler.record_order(order)
        return order_update(
            order,
            execution_target_id=self._settings.execution_target_id,
            command_id=command_id,
        )

    @staticmethod
    def _mutation_outcome_is_unknown(exc: Exception) -> bool:
        """Return true only when the exchange may have accepted the mutation."""

        if isinstance(exc, (asyncio.TimeoutError, TimeoutError, ConnectionError)):
            return True
        error_type = type(exc).__name__.lower()
        message = str(exc).lower()
        return (
            any(token in error_type for token in ("timeout", "network", "unavailable"))
            or "50004" in message
            or any(f"{status}" in message for status in range(500, 600))
        )

    async def reconcile_v2(
        self, execution_target_id: str | None = None
    ) -> dict[str, Any]:
        await self.ensure_connected()
        if execution_target_id == self._settings.perpetual_execution_target_id:
            if self._perpetual is None:
                raise BrokerConnectionError("Perpetual execution context is disabled")
            return await self._perpetual.reconcile_v2()
        if execution_target_id is None and self._perpetual is not None:
            raise BrokerConnectionError("executionTargetId is required for multi-target reconciliation")
        if execution_target_id not in {None, self._settings.execution_target_id}:
            raise BrokerConnectionError("Unknown execution target")
        # Private streams and the periodic reconciler continuously refresh the
        # authoritative snapshot. Read endpoints must not start another fan-out
        # of OKX REST calls: concurrent probes otherwise amplify a transient
        # provider slowdown into request timeouts and readiness churn.
        return self._reconciler.cached_snapshot()

    async def market_data_objects_v1(
        self, market_data_target_id: str | None = None
    ) -> list[dict[str, Any]]:
        if self._perpetual is None:
            raise BrokerConnectionError("Perpetual market-data context is disabled")
        if market_data_target_id != self._settings.perpetual_market_data_target_id:
            raise BrokerConnectionError("marketDataTargetId is required for perpetual market data")
        return await self._perpetual.market_data_objects_v1()

    async def position_risk_v1(
        self, execution_target_id: str | None = None
    ) -> list[dict[str, Any]]:
        if self._perpetual is None:
            raise BrokerConnectionError("Perpetual execution context is disabled")
        if execution_target_id != self._settings.perpetual_execution_target_id:
            raise BrokerConnectionError("executionTargetId is required for perpetual position risk")
        return await self._perpetual.position_risk_v1()

    async def funding_ledger_v1(
        self, execution_target_id: str | None = None
    ) -> list[dict[str, Any]]:
        if self._perpetual is None:
            raise BrokerConnectionError("Perpetual execution context is disabled")
        if execution_target_id != self._settings.perpetual_execution_target_id:
            raise BrokerConnectionError("executionTargetId is required for perpetual funding ledger")
        return await self._perpetual.funding_ledger_v1()

    def set_reconciliation_handler(
        self,
        handler: Callable[[Mapping[str, Any], int], Awaitable[None] | None] | None,
    ) -> None:
        self._reconciliation_handler = handler
        if self._perpetual is not None:
            self._perpetual.set_reconciliation_handler(handler)

    async def _capture_reconciliation(
        self, *, generation: int | None = None
    ) -> dict[str, Any]:
        snapshot = await self._reconciler.snapshot()
        handler = self._reconciliation_handler
        if handler is not None:
            result = handler(
                snapshot,
                self._generation if generation is None else int(generation),
            )
            if inspect.isawaitable(result):
                await result
        LOGGER.warning(
            "OKX Demo reconciliation completed",
            extra={
                "event": "broker.crypto.reconciliation_completed",
                "broker.adapter_id": self.adapter_id,
                "broker.execution_target_id": self._settings.execution_target_id,
                "broker.generation": (
                    self._generation if generation is None else int(generation)
                ),
                "broker.order_update_count": len(snapshot.get("orderUpdates") or []),
                "broker.fill_count": len(snapshot.get("fills") or []),
                "broker.position_count": len(snapshot.get("positions") or []),
                "broker.balance_count": len(snapshot.get("balances") or []),
            },
        )
        return snapshot

    def _set_state(self, state: str, *, reason: str) -> None:
        previous = self._state
        self._state = state
        if previous == state:
            return
        LOGGER.warning(
            "OKX Demo adapter readiness changed",
            extra={
                "event": "broker.crypto.readiness_changed",
                "broker.adapter_id": self.adapter_id,
                "broker.execution_target_id": self._settings.execution_target_id,
                "broker.previous_state": previous,
                "broker.state": state,
                "broker.reason": reason,
                "broker.generation": self._generation,
            },
        )

    def _mark_public_stream_ready(self, stream: str, symbol: str) -> None:
        key = f"{stream}:{symbol}"
        if key in self._public_streams_ready:
            return
        self._public_streams_ready.add(key)
        LOGGER.warning(
            "OKX Demo public stream received its first event",
            extra={
                "event": "broker.crypto.public_stream_ready",
                "broker.adapter_id": self.adapter_id,
                "broker.market_data_target_id": self._settings.market_data_target_id,
                "broker.stream": stream,
                "broker.symbol": symbol,
            },
        )

    def _mark_private_stream_ready(self, stream: str) -> None:
        if stream in self._private_streams_ready:
            return
        self._private_streams_ready.add(stream)
        LOGGER.warning(
            "OKX Demo private stream received its first event",
            extra={
                "event": "broker.crypto.private_stream_ready",
                "broker.adapter_id": self.adapter_id,
                "broker.execution_target_id": self._settings.execution_target_id,
                "broker.stream": stream,
                "broker.ready_streams": sorted(self._private_streams_ready),
            },
        )

    def _unknown_order_outcome(
        self, *, operation: str, client_order_id: str
    ) -> BrokerOrderError:
        LOGGER.error(
            "OKX Demo order outcome is unknown",
            extra={
                "event": "broker.crypto.order_unknown",
                "broker.adapter_id": self.adapter_id,
                "broker.execution_target_id": self._settings.execution_target_id,
                "broker.operation": operation,
            },
        )
        return unknown_outcome(client_order_id=client_order_id)

    def set_trade_update_handler(
        self, handler: Callable[[TradeUpdate], Awaitable[None]] | None
    ) -> None:
        self._trade_update_handler = handler
        if self._perpetual is not None:
            self._perpetual.set_trade_update_handler(handler)

    def set_position_update_handler(
        self, handler: Callable[[list[PositionItem]], Awaitable[None]] | None
    ) -> None:
        self._position_update_handler = handler
        if self._perpetual is not None:
            self._perpetual.set_position_update_handler(handler)

    def set_account_update_handler(
        self, handler: Callable[[list[AccountSummaryItem]], Awaitable[None]] | None
    ) -> None:
        self._account_update_handler = handler
        if self._perpetual is not None:
            self._perpetual.set_account_update_handler(handler)

    def add_connection_listener(
        self, listener: Callable[[str, Mapping[str, Any]], Awaitable[None] | None]
    ) -> None:
        if listener not in self._connection_listeners:
            self._connection_listeners.append(listener)
        if self._perpetual is not None:
            self._perpetual.add_connection_listener(listener)

    def remove_connection_listener(
        self, listener: Callable[[str, Mapping[str, Any]], Awaitable[None] | None]
    ) -> None:
        with suppress(ValueError):
            self._connection_listeners.remove(listener)
        if self._perpetual is not None:
            self._perpetual.remove_connection_listener(listener)

    def add_resub_task(self, coro_factory: Callable[[], Awaitable[None]]) -> None:
        self._resub_tasks.append(coro_factory)
        if self._perpetual is not None:
            self._perpetual.add_resub_task(coro_factory)

    async def _notify_connection(self, state: str, payload: Mapping[str, Any]) -> None:
        for listener in tuple(self._connection_listeners):
            result = listener(state, payload)
            if inspect.isawaitable(result):
                await result

    def _start_private_streams(self) -> None:
        if self._stream_tasks:
            return
        self._stream_tasks = [
            asyncio.create_task(self._watch_orders(), name="ccxt-crypto.okx-demo.orders"),
            asyncio.create_task(self._watch_my_trades(), name="ccxt-crypto.okx-demo.trades"),
            asyncio.create_task(self._watch_balance(), name="ccxt-crypto.okx-demo.balance"),
            asyncio.create_task(
                self._periodic_reconciliation(),
                name="ccxt-crypto.okx-demo.reconciliation",
            ),
        ]

    async def _periodic_reconciliation(self) -> None:
        elapsed = 0
        while True:
            try:
                await asyncio.sleep(self._settings.reconcile_interval_seconds)
                elapsed += self._settings.reconcile_interval_seconds
                await self._capture_reconciliation()
                if elapsed >= self._settings.full_reconcile_interval_seconds:
                    previous = {
                        symbol: rule.metadata_hash for symbol, rule in self._rules.items()
                    }
                    await self._load_and_validate_markets()
                    current = {
                        symbol: rule.metadata_hash for symbol, rule in self._rules.items()
                    }
                    if previous != current:
                        self._metadata_approval_required = True
                        raise BrokerConnectionError(
                            "OKX market metadata changed; operator acceptance is required",
                            details={"previous": previous, "current": current},
                        )
                    elapsed = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - supervisor must fail closed
                await self._stream_failed("reconciliation", exc)
                return

    async def _watch_orders(self) -> None:
        while True:
            try:
                orders = await self._client.watch_orders()
                self._mark_private_stream_ready("orders")
                for order in orders:
                    self._reconciler.record_order(order)
                    if self._trade_update_handler is not None:
                        await self._trade_update_handler(legacy_trade_update(order))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - supervisor must fail closed
                if self._private_websocket_was_reset(exc):
                    await asyncio.sleep(0)
                    continue
                await self._stream_failed("orders", exc)
                return

    async def _watch_balance(self) -> None:
        while True:
            try:
                self._balance = await self._client.watch_balance()
                self._mark_private_stream_ready("balance")
                if self._account_update_handler is not None:
                    await self._account_update_handler(
                        await self._account_summary_items(account_id="okx-demo")
                    )
                if self._position_update_handler is not None:
                    await self._position_update_handler(
                        legacy_positions(self._balance, account_id="okx-demo")
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - supervisor must fail closed
                if self._private_websocket_was_reset(exc):
                    await asyncio.sleep(0)
                    continue
                await self._stream_failed("balance", exc)
                return

    async def _watch_my_trades(self) -> None:
        while True:
            try:
                trades = await self._client.watch_my_trades()
                self._mark_private_stream_ready("trades")
                for trade in trades:
                    self._reconciler.record_trade(trade)
                    if self._trade_update_handler is not None:
                        await self._trade_update_handler(legacy_fill_update(trade))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - supervisor must fail closed
                if self._private_websocket_was_reset(exc):
                    await asyncio.sleep(0)
                    continue
                await self._stream_failed("trades", exc)
                return

    @staticmethod
    def _private_websocket_was_reset(exc: Exception) -> bool:
        """Recognize the client's completed, recoverable private WS reset."""

        details = getattr(exc, "details", None)
        return bool(
            isinstance(details, Mapping)
            and details.get("websocketBoundary") == "private"
            and details.get("nextWebsocketGeneration") is not None
        )

    async def _stream_failed(self, stream: str, exc: Exception) -> None:
        self._set_state("blocked", reason=f"private_{stream}_stream_failed")
        await self._notify_connection(
            "disconnected",
            {
                "reason": f"private_{stream}_stream_failed",
                "error_type": type(exc).__name__,
                "reconciliation_required": True,
            },
        )
        if not self._closing and not self._metadata_approval_required and (
            self._recovery_task is None or self._recovery_task.done()
        ):
            self._recovery_task = asyncio.create_task(
                self._recover_private_streams(),
                name="ccxt-crypto.okx-demo.recovery",
            )

    async def _recover_private_streams(self) -> None:
        """Single-flight bounded reconnect; reconciliation precedes readiness."""

        attempt = 0
        while self._connected and not self._closing:
            attempt += 1
            delay = min(30.0, float(2 ** (attempt - 1)))
            await asyncio.sleep(delay + random.uniform(0.0, delay * 0.25))
            try:
                await self._capture_reconciliation(
                    generation=self._generation + 1
                )
                previous = asyncio.current_task()
                stale, self._stream_tasks = self._stream_tasks, []
                for task in stale:
                    if task is not previous and not task.done():
                        task.cancel()
                for task in stale:
                    if task is not previous:
                        with suppress(asyncio.CancelledError):
                            await task
                self._generation += 1
                self._start_private_streams()
                self._set_state(
                    "trading_ready",
                    reason="private_stream_recovered",
                )
                await self._notify_connection(
                    "connected",
                    {
                        "adapter_id": self.adapter_id,
                        "state": self._state,
                        "generation": self._generation,
                        "reconciled": True,
                    },
                )
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - bounded provider recovery loop
                self._reconnect_reason = f"reconciliation_failed:{type(exc).__name__}"

    async def _account_summary_items(self, *, account_id: str) -> list[AccountSummaryItem]:
        if any(
            symbol.split("/", 1)[0] not in self._prices_in_usdt
            for symbol in self._settings.allowed_symbols
        ):
            await self._refresh_valuation_prices()
        return account_summary(
            self._balance,
            account_id=account_id,
            prices_in_usdt=self._prices_in_usdt,
        )

    async def _refresh_valuation_prices(self) -> None:
        symbols = self._settings.allowed_symbols
        tickers = await asyncio.gather(
            *(self._client.fetch_ticker(symbol) for symbol in symbols)
        )
        for symbol, ticker in zip(symbols, tickers, strict=True):
            self._update_valuation_price(symbol, ticker)

    def _update_valuation_price(self, symbol: str, ticker: Mapping[str, Any]) -> None:
        raw_price = ticker.get("last") or ticker.get("close")
        price = Decimal(str(raw_price or "0"))
        if price <= 0:
            raise BrokerConnectionError(
                f"OKX Demo returned no valuation price for {symbol}",
                details={"symbol": symbol},
            )
        self._prices_in_usdt[symbol.split("/", 1)[0]] = price

    async def get_account_summary(self, account: str | None = None) -> list[AccountSummaryItem]:
        await self.ensure_connected()
        if self._perpetual is not None and account == self._settings.perpetual_account_id:
            return await self._perpetual.get_account_summary(account)
        spot = await self._account_summary_items(account_id=account or "okx-demo")
        if self._perpetual is None or account is not None:
            return spot
        return [*spot, *await self._perpetual.get_account_summary()]

    async def get_account_pnl(
        self,
        account: str | None = None,
        *,
        model_code: str | None = None,
        timeout: float = 5.0,
    ) -> dict[str, float | None] | None:
        return {"daily": None, "realized": None, "unrealized": None}

    async def get_positions(self) -> list[PositionItem]:
        await self.ensure_connected()
        spot = legacy_positions(self._balance, account_id="okx-demo")
        if self._perpetual is None:
            return spot
        return [*spot, *await self._perpetual.get_positions()]

    async def place_stock_order(self, request: StockOrderRequest) -> OrderResult:
        raise unsupported("stock orders; use the broker V2 crypto order endpoint")

    async def place_future_order(self, request: FutureOrderRequest) -> OrderResult:
        raise unsupported("futures orders")

    async def place_option_order(self, request: OptionOrderRequest) -> OrderResult:
        raise unsupported("option orders")

    async def cancel_order(self, order_id: int | str) -> None:
        await self.ensure_connected()
        order_key = str(order_id)
        symbol = self._order_symbols.get(order_key)
        if not symbol:
            raise BrokerOrderError(
                "Legacy cancel lacks the OKX symbol; use cancel_order_v2",
                code="symbol_required_for_cancel",
            )
        await self._client.cancel_order(order_key, symbol)

    async def request_open_orders(self) -> list[TradeUpdate]:
        await self.ensure_connected()
        open_orders, _, _ = self._reconciler.legacy_snapshot()
        spot = [legacy_trade_update(item) for item in open_orders]
        if self._perpetual is None:
            return spot
        return [*spot, *await self._perpetual.request_open_orders()]

    async def request_executions(self, since: datetime | str | None = None) -> list[TradeUpdate]:
        await self.ensure_connected()
        _, _, trades = self._reconciler.legacy_snapshot()
        updates = [legacy_fill_update(item) for item in trades]
        if self._perpetual is not None:
            updates.extend(await self._perpetual.request_executions())
        if since is None:
            return updates
        since_at = since if isinstance(since, datetime) else datetime.fromisoformat(since)
        if since_at.tzinfo is None:
            since_at = since_at.replace(tzinfo=UTC)
        return [
            update
            for update in updates
            if update.event_time is not None and update.event_time >= since_at
        ]

    async def request_completed_orders(self) -> list[TradeUpdate]:
        await self.ensure_connected()
        _, completed_orders, _ = self._reconciler.legacy_snapshot()
        spot = [legacy_trade_update(item) for item in completed_orders]
        if self._perpetual is None:
            return spot
        return [*spot, *await self._perpetual.request_completed_orders()]

    async def qualify_contract(self, contract: Mapping[str, Any]) -> dict[str, Any]:
        await self.ensure_connected()
        symbol = str(contract.get("symbol") or "").upper()
        if symbol not in self._settings.allowed_symbols:
            raise unsupported("non-allowlisted instruments")
        if not self._rules:
            await self._load_and_validate_markets()
        rule = self._rules[symbol]
        return {
            "symbol": symbol,
            "secType": "CRYPTO_SPOT",
            "exchange": "OKX",
            "currency": "USDT",
            "localSymbol": rule.native_instrument_id,
            "instrumentId": rule.instrument_id,
        }

    async def request_contract_details(self, contract: Mapping[str, Any]) -> list[dict[str, Any]]:
        return [await self.qualify_contract(contract)]

    async def request_market_snapshot(
        self,
        contract: Mapping[str, Any],
        *,
        generic_tick_list: str = "",
        snapshot_timeout: float | None = None,
    ) -> dict[str, Any]:
        await self.ensure_connected()
        symbol = str(contract.get("symbol") or "").upper()
        if symbol not in self._settings.allowed_symbols:
            raise unsupported("non-allowlisted instruments")
        ticker = await self._client.fetch_ticker(symbol)
        self._update_valuation_price(symbol, ticker)
        return {
            "symbol": symbol,
            "bid": ticker.get("bid"),
            "ask": ticker.get("ask"),
            "last": ticker.get("last"),
            "timestamp": timestamp(ticker.get("timestamp")),
        }

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
        await self.ensure_connected()
        symbol = str(contract.get("symbol") or "").upper()
        if symbol not in self._settings.allowed_symbols:
            raise unsupported("non-allowlisted instruments")
        timeframe = {"1 min": "1m", "5 mins": "5m", "1 hour": "1h", "1 day": "1d"}.get(bar_size)
        if timeframe is None:
            raise unsupported(f"bar size {bar_size}")
        rows = await self._client.fetch_ohlcv(symbol, timeframe)
        return [self._bar(row) for row in rows]

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
        await self.ensure_connected()
        symbol = str(contract.get("symbol") or "").upper()
        timeframe = {"1 min": "1m", "5 mins": "5m", "1 hour": "1h", "1 day": "1d"}.get(bar_size)
        if symbol not in self._settings.allowed_symbols or timeframe is None:
            raise unsupported("requested historical bar stream")

        async def iterator() -> AsyncIterator[HistoricalBar]:
            while True:
                rows = await self._client.watch_ohlcv(symbol, timeframe)
                if rows:
                    self._mark_public_stream_ready("bar", symbol)
                    yield self._bar(rows[-1])

        return iterator()

    async def stream_real_time_price(
        self, contract: Mapping[str, Any], *, snapshot: bool = False
    ) -> AsyncIterator[RealTimePrice]:
        await self.ensure_connected()
        symbol = str(contract.get("symbol") or "").upper()
        if symbol not in self._settings.allowed_symbols:
            raise unsupported("non-allowlisted instruments")

        async def iterator() -> AsyncIterator[RealTimePrice]:
            last_key: tuple[Any, ...] | None = None
            while True:
                ticker = await self._client.watch_ticker(symbol)
                raw_timestamp = ticker.get("timestamp")
                if raw_timestamp in (None, ""):
                    continue
                try:
                    ticker_timestamp_ms = int(str(raw_timestamp))
                except (TypeError, ValueError):
                    continue
                now_ms = int(datetime.now(UTC).timestamp() * 1000)
                if now_ms - ticker_timestamp_ms > _PUBLIC_TICKER_MAX_AGE_MS:
                    continue
                key = (
                    ticker_timestamp_ms,
                    ticker.get("bid"),
                    ticker.get("ask"),
                    ticker.get("last"),
                    ticker.get("close"),
                )
                if key == last_key:
                    continue
                last_key = key
                self._update_valuation_price(symbol, ticker)
                self._mark_public_stream_ready("ticker", symbol)
                yield RealTimePrice(
                    symbol=symbol,
                    bid=float(ticker["bid"]) if ticker.get("bid") is not None else None,
                    ask=float(ticker["ask"]) if ticker.get("ask") is not None else None,
                    last=float(ticker["last"]) if ticker.get("last") is not None else None,
                    last_size=None,
                    close=float(ticker["close"]) if ticker.get("close") is not None else None,
                    timestamp=datetime.fromisoformat(timestamp(ticker_timestamp_ms)),
                )
                if snapshot:
                    return

        return iterator()

    async def stream_tick_by_tick_data(
        self,
        contract: Mapping[str, Any],
        *,
        tick_type: str = "Last",
        number_of_ticks: int = 0,
        ignore_size: bool = False,
    ) -> AsyncIterator[TickByTickLast]:
        await self.ensure_connected()
        if tick_type.lower() not in {"last", "alllast"}:
            raise unsupported("non-trade tick streams")
        symbol = str(contract.get("symbol") or "").upper()
        if symbol not in self._settings.allowed_symbols:
            raise unsupported("non-allowlisted instruments")

        async def iterator() -> AsyncIterator[TickByTickLast]:
            emitted = 0
            seen: set[tuple[Any, ...]] = set()
            seen_order: deque[tuple[Any, ...]] = deque()
            while number_of_ticks <= 0 or emitted < number_of_ticks:
                trades = await self._client.watch_trades(symbol)
                for trade in sorted(
                    trades,
                    key=lambda item: int(item.get("timestamp") or 0),
                ):
                    raw_timestamp = trade.get("timestamp")
                    if raw_timestamp in (None, ""):
                        continue
                    trade_id = trade.get("id") or _field(
                        trade.get("info") or {}, "tradeId"
                    )
                    key = (
                        ("id", str(trade_id))
                        if trade_id not in (None, "")
                        else (
                            "payload",
                            str(raw_timestamp),
                            str(trade.get("price")),
                            str(trade.get("amount")),
                            str(trade.get("side")),
                        )
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    seen_order.append(key)
                    if len(seen_order) > _PUBLIC_TRADE_DEDUP_WINDOW:
                        seen.discard(seen_order.popleft())

                    trade_timestamp_ms = int(str(raw_timestamp))
                    now_ms = int(datetime.now(UTC).timestamp() * 1000)
                    if now_ms - trade_timestamp_ms > _PUBLIC_TRADE_MAX_AGE_MS:
                        continue
                    self._mark_public_stream_ready("trade", symbol)
                    yield TickByTickLast(
                        time=datetime.fromisoformat(timestamp(trade_timestamp_ms)),
                        price=float(trade.get("price") or 0),
                        size=float(trade.get("amount") or 0),
                        exchange="OKX",
                    )
                    emitted += 1
                    if number_of_ticks > 0 and emitted >= number_of_ticks:
                        return

        return iterator()

    async def get_historical_ticks(self, *args: Any, **kwargs: Any) -> list[HistoricalTickBidAsk | HistoricalTickLast]:
        raise unsupported("historical ticks")

    async def stream_market_depth(self, *args: Any, **kwargs: Any) -> AsyncIterator[DOMSnapshot]:
        raise unsupported("market depth")
        if False:  # pragma: no cover
            yield DOMSnapshot((), (), datetime.now(UTC))

    async def request_option_parameters(self, **kwargs: Any) -> list[dict[str, Any]]:
        raise unsupported("option parameters")

    async def request_scanner_parameters(self) -> str:
        raise unsupported("scanner parameters")

    async def request_scanner_data(
        self,
        payload: Mapping[str, Any],
        *,
        tag_filters: Iterable[Mapping[str, Any]] | None = None,
        stream_seconds: float | None = None,
    ) -> list[dict[str, Any]]:
        raise unsupported("scanner data")

    async def request_option_snapshot(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        raise unsupported("option snapshots")

    async def request_option_greeks(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        raise unsupported("option greeks")

    @staticmethod
    def _bar(row: list[Any]) -> HistoricalBar:
        return HistoricalBar(
            time=datetime.fromtimestamp(int(row[0]) / 1000, tz=UTC),
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
            wap=None,
            count=None,
        )
