"""OKX Demo one-way isolated 2x USDT perpetual adapter."""

from __future__ import annotations

import asyncio
import inspect
from contextlib import suppress
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from algo_trader_broker_sdk import (
    AccountSummaryItem,
    BrokerAdapterManifest,
    BrokerCapabilities,
    BrokerCapabilityError,
    BrokerConnectionError,
    BrokerConnectionState,
    BrokerOrderError,
    FutureOrderRequest,
    OptionOrderRequest,
    OrderResult,
    PositionItem,
    StockOrderRequest,
    TradeUpdate,
)

from .client import OKXDemoPerpetualClient
from .mapping import (
    balance_payloads,
    fill,
    funding_entry,
    market_data_objects,
    order_update,
    position_payload,
)
from .quantizer import PerpetualMarketRules, canonical, instrument_id
from .settings import CCXTCryptoPerpetualSettings

_SYMBOL_BY_INSTRUMENT = {
    "crypto-perpetual:BTC-USDT:USDT:OKX": "BTC/USDT:USDT",
    "crypto-perpetual:ETH-USDT:USDT:OKX": "ETH/USDT:USDT",
}


def _field(payload: Mapping[str, Any], camel: str, snake: str | None = None) -> Any:
    return payload[camel] if camel in payload else payload.get(snake or camel)


class CCXTCryptoPerpetualAdapter:
    adapter_id = "ccxt_crypto_perpetual"

    def __init__(self, settings: Mapping[str, Any], *, backend: Any | None = None) -> None:
        self._settings = CCXTCryptoPerpetualSettings.from_mapping(settings)
        self._client = backend or OKXDemoPerpetualClient(self._settings)
        self._connected = False
        self._state = "installed"
        self._connected_since: datetime | None = None
        self._reconnect_reason: str | None = None
        self._generation = 0
        self._rules: dict[str, PerpetualMarketRules] = {}
        self._snapshot: dict[str, Any] | None = None
        self._position_risk: list[dict[str, Any]] = []
        self._funding_ledger: list[dict[str, Any]] = []
        self._sequence = 0
        self._stream_tasks: list[asyncio.Task[None]] = []
        self._closing = False
        self._market_data_cache: dict[str, list[dict[str, Any]]] = {}
        self._trade_update_handler: Callable[[TradeUpdate], Awaitable[None]] | None = None
        self._position_update_handler: Callable[[list[PositionItem]], Awaitable[None]] | None = None
        self._account_update_handler: (
            Callable[[list[AccountSummaryItem]], Awaitable[None]] | None
        ) = None
        self._reconciliation_handler: (
            Callable[[Mapping[str, Any], int], Awaitable[None] | None] | None
        ) = None
        self._connection_listeners: list[
            Callable[[str, Mapping[str, Any]], Awaitable[None] | None]
        ] = []
        self._resub_tasks: list[Callable[[], Awaitable[None]]] = []

    def manifest(self) -> BrokerAdapterManifest:
        return BrokerAdapterManifest(
            adapter_id=self.adapter_id,
            display_name="OKX Demo USDT Perpetual (CCXT)",
            adapter_version="0.1.0",
            protocol_version="1.0",
            environment="PAPER",
            entrypoint=(
                "algo_trader_broker_adapter_ccxt_crypto_perpetual:create_adapter"
            ),
            capabilities=self.capabilities(),
        )

    def capabilities(self) -> BrokerCapabilities:
        return BrokerCapabilities(
            adapter_name=self.adapter_id,
            environment="PAPER",
            asset_classes={"CRYPTO_PERPETUAL"},
            order_types={"MKT", "LMT"},
            time_in_force={"GTC", "IOC"},
            market_data_streams={"mark", "index", "funding"},
            account_features={
                "summary",
                "positions",
                "reconciliation_v2",
                "position_risk_v1",
                "funding_ledger_v1",
            },
            supports_fractional=False,
            supports_shorting=True,
            supports_partial_fills=True,
            supports_futures=True,
            default_asset_class="CRYPTO_PERPETUAL",
            symbol_examples={"CRYPTO_PERPETUAL": list(self._settings.allowed_symbols)},
            native={
                "exchangeId": "okx",
                "paperOnly": True,
                "sandboxRequired": True,
                "simulatedTradingHeaderRequired": True,
                "executionTargetId": self._settings.execution_target_id,
                "marketDataTargetId": self._settings.market_data_target_id,
                "positionMode": "ONE_WAY",
                "marginMode": "ISOLATED",
                "fixedLeverage": "2",
                "live": False,
            },
        )

    async def start(self) -> None:
        await self.connect()
        self._start_streams()

    async def connect(self) -> None:
        if self._connected:
            return
        markets = await self._client.load_markets()
        for symbol in self._settings.allowed_symbols:
            market = markets.get(symbol)
            if not isinstance(market, Mapping):
                raise BrokerConnectionError("Allowlisted perpetual market is unavailable")
            rule = PerpetualMarketRules.from_ccxt(symbol, market)
            if not rule.active:
                raise BrokerConnectionError("Allowlisted perpetual market is inactive")
            self._rules[symbol] = rule
        remote_time = int(await self._client.fetch_time())
        skew = abs(int(datetime.now(UTC).timestamp() * 1000) - remote_time)
        if skew > self._settings.clock_skew_block_ms:
            raise BrokerConnectionError(
                "OKX clock skew exceeds Phase 5 threshold",
                details={"clock_skew_ms": skew},
            )
        for symbol in self._settings.allowed_symbols:
            mode = await self._client.fetch_position_mode(symbol)
            if bool(mode.get("hedged")):
                raise BrokerConnectionError("OKX position mode must be one-way/net")
            leverage = await self._client.fetch_leverage(symbol)
            margin_mode = str(leverage.get("marginMode") or "").lower()
            values = {
                str(leverage.get("longLeverage") or leverage.get("leverage") or ""),
                str(leverage.get("shortLeverage") or leverage.get("leverage") or ""),
            }
            if margin_mode != "isolated" or values != {"2"}:
                raise BrokerConnectionError("OKX margin mode/leverage does not match policy")
        await self.reconcile_v2()
        self._connected = True
        self._generation += 1
        self._connected_since = datetime.now(UTC)
        self._state = "trading_ready"
        await self._notify_connection("connected", self.connection_diagnostics())

    async def close(self) -> None:
        self._closing = True
        await self._stop_streams()
        await self._client.close()
        self._connected = False
        self._state = "disconnected"

    async def _stop_streams(self) -> None:
        tasks, self._stream_tasks = self._stream_tasks, []
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task

    async def disconnect(self, reason: str | None = None) -> None:
        await self._stop_streams()
        self._reconnect_reason = reason
        self._connected = False
        self._state = "disconnected"

    async def reconnect(self, *, reason: str | None = None) -> None:
        await self.disconnect(reason)
        self._closing = False
        await self.connect()
        self._start_streams()
        for factory in tuple(self._resub_tasks):
            await factory()

    async def ensure_connected(self) -> None:
        if not self._connected:
            raise BrokerConnectionError("OKX Demo perpetual adapter is not connected")

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
        return {
            "adapter_id": self.adapter_id,
            "state": self._state,
            "generation": self._generation,
            "reconciliationReady": self._snapshot is not None,
            **self._settings.redacted(),
        }

    async def market_metadata_v2(self) -> list[dict[str, Any]]:
        await self.ensure_connected()
        return [
            {
                "instrumentId": instrument_id(rule.symbol),
                "assetClass": "CRYPTO_PERPETUAL",
                "symbol": rule.symbol,
                "nativeInstrumentId": rule.native_instrument_id,
                "metadataVersion": 1,
                "metadataHash": rule.metadata_hash,
                "perpetualContractKind": "LINEAR",
                "settlementCurrency": "USDT",
                "contractMultiplier": canonical(rule.contract_multiplier),
                "tickSize": canonical(rule.tick_size),
                "quantityStep": canonical(rule.quantity_step),
                "minimumQuantity": canonical(rule.minimum_quantity),
                "active": rule.active,
            }
            for rule in self._rules.values()
        ]

    async def market_data_objects_v1(self) -> list[dict[str, Any]]:
        await self.ensure_connected()
        result: list[dict[str, Any]] = []
        for symbol in self._settings.allowed_symbols:
            cached = self._market_data_cache.get(symbol)
            if cached is not None:
                result.extend(cached)
                continue
            result.extend(await self._fetch_market_data_objects(symbol))
        return result

    async def _fetch_market_data_objects(self, symbol: str) -> list[dict[str, Any]]:
        rate = await self._client.fetch_funding_rate(symbol)
        objects = market_data_objects(
            rate,
            symbol=symbol,
            market_data_target_id=self._settings.market_data_target_id,
            first_sequence=self._sequence + 1,
        )
        self._sequence += 3
        self._market_data_cache[symbol] = objects
        return objects

    async def place_order_v2(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        await self.ensure_connected()
        if self._state != "trading_ready":
            raise BrokerOrderError("Perpetual adapter is not trading ready")
        target = str(_field(payload, "executionTargetId", "execution_target_id") or "")
        if target != self._settings.execution_target_id:
            raise BrokerOrderError("Execution target mismatch", code="execution_target_mismatch")
        instrument = str(_field(payload, "instrumentId", "instrument_id") or "")
        symbol = _SYMBOL_BY_INSTRUMENT.get(instrument)
        if symbol is None or symbol not in self._rules:
            raise BrokerOrderError("Instrument is not allowlisted", code="instrument_not_allowed")
        side = str(payload.get("side") or "").upper()
        if side not in {"BUY", "SELL"}:
            raise BrokerOrderError("Order side must be BUY or SELL")
        order_type = str(_field(payload, "orderType", "order_type") or "").upper()
        if order_type not in {"MARKET", "LIMIT"}:
            raise BrokerOrderError("Only MARKET and LIMIT are supported")
        tif = str(_field(payload, "timeInForce", "time_in_force") or "").upper()
        if tif not in {"GTC", "IOC"}:
            raise BrokerOrderError("Only GTC and IOC are supported")
        reduce_only = bool(_field(payload, "reduceOnly", "reduce_only"))
        effect = str(_field(payload, "positionEffect", "position_effect") or "").upper()
        if reduce_only != (effect == "CLOSE"):
            raise BrokerOrderError(
                "reduceOnly and positionEffect=CLOSE must be declared together",
                code="unsupported_order_semantics",
            )
        rules = self._rules[symbol]
        contracts = rules.quantize_contracts(
            _field(payload, "quantityDecimal", "quantity_decimal")
        )
        if reduce_only:
            current = next(
                (
                    item
                    for item in (self._snapshot or {}).get("positions", [])
                    if item.get("instrumentId") == instrument
                ),
                None,
            )
            signed = canonical(current.get("quantityDecimal")) if current else "0"
            current_contracts = Decimal(signed)
            closes_current_side = (current_contracts > 0 and side == "SELL") or (
                current_contracts < 0 and side == "BUY"
            )
            if not closes_current_side or contracts > abs(current_contracts):
                raise BrokerOrderError(
                    "reduce-only order would cross zero or increase exposure",
                    code="reduce_only_projection_failed",
                )
        price: str | None = None
        if order_type == "LIMIT":
            price = canonical(
                rules.quantize_price(
                    _field(payload, "limitPriceDecimal", "limit_price_decimal"),
                    side=side,
                )
            )
        params = {
            "tdMode": "isolated",
            "posSide": "net",
            "reduceOnly": reduce_only,
            "clOrdId": str(_field(payload, "clientOrderId", "client_order_id") or "")[:32],
            "tag": str(_field(payload, "commandId", "command_id") or "")[:16],
        }
        order = await self._client.create_order(
            symbol,
            order_type.lower(),
            side.lower(),
            canonical(contracts),
            price,
            params,
        )
        return order_update(
            order,
            execution_target_id=self._settings.execution_target_id,
            command_id=str(_field(payload, "commandId", "command_id") or ""),
            client_order_id=str(_field(payload, "clientOrderId", "client_order_id") or ""),
        )

    async def cancel_order_v2(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        await self.ensure_connected()
        target = str(_field(payload, "executionTargetId", "execution_target_id") or "")
        if target != self._settings.execution_target_id:
            raise BrokerOrderError("Execution target mismatch", code="execution_target_mismatch")
        instrument = str(_field(payload, "instrumentId", "instrument_id") or "")
        symbol = _SYMBOL_BY_INSTRUMENT.get(instrument)
        if symbol is None:
            raise BrokerOrderError("Instrument is not allowlisted")
        order = await self._client.cancel_order(
            str(_field(payload, "brokerOrderId", "broker_order_id") or ""), symbol
        )
        return order_update(
            order,
            execution_target_id=self._settings.execution_target_id,
            command_id=str(_field(payload, "commandId", "command_id") or ""),
        )

    async def reconcile_v2(self) -> dict[str, Any]:
        balance = await self._client.fetch_balance()
        positions = await self._client.fetch_positions(list(self._settings.allowed_symbols))
        mapped_positions: list[dict[str, Any]] = []
        position_risk: list[dict[str, Any]] = []
        for row in positions:
            symbol = str(row.get("symbol") or "")
            if symbol not in self._rules:
                if row.get("contracts") not in (None, 0, "0"):
                    raise BrokerConnectionError("Unexpected non-flat perpetual position")
                continue
            mapped, risk = position_payload(
                row,
                rules=self._rules[symbol],
                account_id=self._settings.account_id,
                execution_target_id=self._settings.execution_target_id,
            )
            mapped_positions.append(mapped)
            if risk is not None:
                position_risk.append(risk)
        orders: dict[str, Mapping[str, Any]] = {}
        trades: dict[str, Mapping[str, Any]] = {}
        funding_rows: dict[str, Mapping[str, Any]] = {}
        for symbol in self._settings.allowed_symbols:
            for row in [
                *await self._client.fetch_open_orders(symbol),
                *await self._client.fetch_closed_orders(symbol),
            ]:
                order_id = str(row.get("id") or "")
                if order_id:
                    orders[order_id] = row
            for row in await self._client.fetch_my_trades(symbol):
                trade_id = str(row.get("id") or "")
                if trade_id:
                    trades[trade_id] = row
            for row in await self._client.fetch_funding_history(symbol):
                row_id = str(row.get("id") or "")
                if row_id:
                    funding_rows[row_id] = row
        self._position_risk = position_risk
        self._funding_ledger = [
            funding_entry(
                row,
                account_id=self._settings.account_id,
                execution_target_id=self._settings.execution_target_id,
            )
            for _, row in sorted(funding_rows.items())
        ]
        snapshot = {
            "schemaVersion": "broker-reconciliation.v1",
            "executionTargetId": self._settings.execution_target_id,
            "orderUpdates": [
                order_update(row, execution_target_id=self._settings.execution_target_id)
                for _, row in sorted(orders.items())
            ],
            "fills": [
                fill(row, execution_target_id=self._settings.execution_target_id)
                for _, row in sorted(trades.items())
            ],
            "positions": mapped_positions,
            "balances": balance_payloads(
                balance,
                account_id=self._settings.account_id,
                execution_target_id=self._settings.execution_target_id,
            ),
        }
        self._snapshot = snapshot
        if self._reconciliation_handler is not None:
            maybe = self._reconciliation_handler(snapshot, self._generation + 1)
            if inspect.isawaitable(maybe):
                await maybe
        return snapshot

    async def position_risk_v1(self) -> list[dict[str, Any]]:
        await self.ensure_connected()
        return list(self._position_risk)

    async def funding_ledger_v1(self) -> list[dict[str, Any]]:
        await self.ensure_connected()
        return list(self._funding_ledger)

    def _start_streams(self) -> None:
        if self._stream_tasks or self._closing:
            return
        self._stream_tasks = [
            asyncio.create_task(self._watch_orders(), name="ccxt-perpetual.orders"),
            asyncio.create_task(self._watch_trades(), name="ccxt-perpetual.trades"),
            asyncio.create_task(self._watch_positions(), name="ccxt-perpetual.positions"),
            asyncio.create_task(self._watch_balance(), name="ccxt-perpetual.balance"),
            asyncio.create_task(self._watch_market_data(), name="ccxt-perpetual.market-data"),
            asyncio.create_task(
                self._periodic_reconciliation(), name="ccxt-perpetual.reconciliation"
            ),
        ]

    async def _periodic_reconciliation(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._settings.reconcile_interval_seconds)
                await self.reconcile_v2()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - fail closed on drift
                await self._stream_failed("reconciliation", exc)
                return

    async def _watch_orders(self) -> None:
        while True:
            try:
                for row in await self._client.watch_orders():
                    if self._trade_update_handler is not None:
                        await self._trade_update_handler(
                            self._legacy_update(
                                order_update(
                                    row,
                                    execution_target_id=self._settings.execution_target_id,
                                )
                            )
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - fail closed on stream loss
                await self._stream_failed("orders", exc)
                return

    async def _watch_trades(self) -> None:
        while True:
            try:
                for row in await self._client.watch_my_trades():
                    if self._trade_update_handler is not None:
                        mapped = fill(
                            row, execution_target_id=self._settings.execution_target_id
                        )
                        await self._trade_update_handler(
                            TradeUpdate(
                                adapter_id=self.adapter_id,
                                adapter_order_id=mapped["orderIdentity"]["brokerOrderId"],
                                adapter_execution_id=mapped["brokerExecutionId"],
                                status="FILLED",
                                last_fill_price=float(mapped["priceDecimal"]),
                                last_fill_quantity=float(mapped["quantityDecimal"]),
                                commission=float(mapped["feeDecimal"]),
                                event_time=datetime.fromisoformat(
                                    mapped["eventTime"].replace("Z", "+00:00")
                                ),
                            )
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - fail closed on stream loss
                await self._stream_failed("trades", exc)
                return

    async def _watch_positions(self) -> None:
        while True:
            try:
                await self._client.watch_positions(list(self._settings.allowed_symbols))
                await self.reconcile_v2()
                if self._position_update_handler is not None:
                    await self._position_update_handler(await self.get_positions())
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - fail closed on stream loss
                await self._stream_failed("positions", exc)
                return

    async def _watch_balance(self) -> None:
        while True:
            try:
                await self._client.watch_balance()
                await self.reconcile_v2()
                if self._account_update_handler is not None:
                    await self._account_update_handler(await self.get_account_summary())
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - fail closed on stream loss
                await self._stream_failed("balance", exc)
                return

    async def _watch_market_data(self) -> None:
        while True:
            try:
                for symbol in self._settings.allowed_symbols:
                    rate = await self._client.watch_funding_rate(symbol)
                    objects = market_data_objects(
                        rate,
                        symbol=symbol,
                        market_data_target_id=self._settings.market_data_target_id,
                        first_sequence=self._sequence + 1,
                    )
                    self._sequence += 3
                    self._market_data_cache[symbol] = objects
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - fail closed on stream loss
                await self._stream_failed("market_data", exc)
                return

    async def _stream_failed(self, stream: str, exc: Exception) -> None:
        self._state = "blocked"
        self._reconnect_reason = f"{stream}_stream_failed"
        await self._notify_connection(
            "disconnected",
            {
                "adapter_id": self.adapter_id,
                "reason": self._reconnect_reason,
                "error_type": type(exc).__name__,
                "reconciliation_required": True,
            },
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

    def set_reconciliation_handler(
        self, handler: Callable[[Mapping[str, Any], int], Awaitable[None] | None] | None
    ) -> None:
        self._reconciliation_handler = handler

    def add_connection_listener(
        self, listener: Callable[[str, Mapping[str, Any]], Awaitable[None] | None]
    ) -> None:
        self._connection_listeners.append(listener)

    def remove_connection_listener(
        self, listener: Callable[[str, Mapping[str, Any]], Awaitable[None] | None]
    ) -> None:
        if listener in self._connection_listeners:
            self._connection_listeners.remove(listener)

    def add_resub_task(self, factory: Callable[[], Awaitable[None]]) -> None:
        self._resub_tasks.append(factory)

    async def _notify_connection(self, state: str, payload: Mapping[str, Any]) -> None:
        for listener in tuple(self._connection_listeners):
            maybe = listener(state, payload)
            if inspect.isawaitable(maybe):
                await maybe

    async def get_account_summary(self, account: str | None = None) -> list[AccountSummaryItem]:
        await self.ensure_connected()
        balances = {row["balanceType"]: row for row in (self._snapshot or {}).get("balances", [])}
        return [
            AccountSummaryItem(
                account=account or self._settings.account_id,
                tag=tag,
                value=str(balances.get(balance_type, {}).get("amountDecimal", "0")),
                currency="USDT",
            )
            for tag, balance_type in (
                ("NetLiquidation", "EQUITY"),
                ("AvailableFunds", "AVAILABLE"),
                ("InitialMargin", "MARGIN_USED"),
            )
        ]

    async def get_account_pnl(
        self,
        account: str | None = None,
        *,
        model_code: str | None = None,
        timeout: float = 5.0,
    ) -> dict[str, float | None]:
        del account, model_code, timeout
        return {
            "daily_pnl": None,
            "unrealized_pnl": sum(
                float(item["unrealizedPnlDecimal"]) for item in self._position_risk
            ),
            "realized_pnl": None,
        }

    async def get_positions(self) -> list[PositionItem]:
        await self.ensure_connected()
        return [
            PositionItem(
                account=self._settings.account_id,
                contract_id=None,
                symbol=item["instrumentId"],
                sec_type="CRYPTO_PERPETUAL",
                exchange="OKX",
                currency="USDT",
                position=float(item["quantityDecimal"]),
                avg_cost=float(item["averagePriceDecimal"] or 0),
            )
            for item in (self._snapshot or {}).get("positions", [])
            if item["quantityDecimal"] != "0"
        ]

    async def request_open_orders(self) -> list[TradeUpdate]:
        await self.ensure_connected()
        return [
            self._legacy_update(item)
            for item in (self._snapshot or {}).get("orderUpdates", [])
            if item["status"] in {"PENDING_SUBMIT", "SUBMITTED", "PARTIALLY_FILLED"}
        ]

    async def request_completed_orders(self) -> list[TradeUpdate]:
        await self.ensure_connected()
        return [
            self._legacy_update(item)
            for item in (self._snapshot or {}).get("orderUpdates", [])
            if item["status"] not in {"PENDING_SUBMIT", "SUBMITTED", "PARTIALLY_FILLED"}
        ]

    async def request_executions(self, since: Any = None) -> list[TradeUpdate]:
        del since
        await self.ensure_connected()
        return [
            TradeUpdate(
                adapter_id=self.adapter_id,
                adapter_order_id=item["orderIdentity"]["brokerOrderId"],
                adapter_execution_id=item["brokerExecutionId"],
                status="FILLED",
                last_fill_price=float(item["priceDecimal"]),
                last_fill_quantity=float(item["quantityDecimal"]),
                commission=float(item["feeDecimal"]),
                event_time=datetime.fromisoformat(item["eventTime"].replace("Z", "+00:00")),
            )
            for item in (self._snapshot or {}).get("fills", [])
        ]

    def _legacy_update(self, item: Mapping[str, Any]) -> TradeUpdate:
        return TradeUpdate(
            adapter_id=self.adapter_id,
            adapter_order_id=str(item["identity"]["brokerOrderId"]),
            status=str(item["status"]),
            filled=float(item["cumulativeFilledDecimal"]),
            remaining=float(item["remainingDecimal"]),
            avg_fill_price=(
                float(item["averageFillPriceDecimal"])
                if item.get("averageFillPriceDecimal") is not None
                else None
            ),
            client_order_id=str(item["clientOrderId"]),
            event_time=datetime.fromisoformat(str(item["eventTime"]).replace("Z", "+00:00")),
        )

    async def place_stock_order(self, request: StockOrderRequest) -> OrderResult:
        del request
        raise BrokerCapabilityError("Perpetual adapter requires Broker V2 orders")

    async def place_future_order(self, request: FutureOrderRequest) -> OrderResult:
        del request
        raise BrokerCapabilityError("Perpetual adapter requires Broker V2 orders")

    async def place_option_order(self, request: OptionOrderRequest) -> OrderResult:
        del request
        raise BrokerCapabilityError("Perpetual adapter does not support options")

    async def cancel_order(self, order_id: int | str) -> None:
        del order_id
        raise BrokerCapabilityError("Perpetual adapter requires target-scoped V2 cancel")
