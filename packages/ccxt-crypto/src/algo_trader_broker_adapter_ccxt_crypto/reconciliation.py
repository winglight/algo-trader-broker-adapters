"""Deterministic two-symbol OKX Demo reconciliation."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from algo_trader_broker_sdk import BrokerConnectionError

from .mapping import (
    balance_payloads,
    fill,
    instrument_id,
    order_status,
    order_update,
    positions,
)
from .quantizer import canonical
from .settings import CCXTCryptoSettings

LOGGER = logging.getLogger(__name__)


class Reconciler:
    def __init__(self, client: Any, settings: CCXTCryptoSettings) -> None:
        self.client = client
        self.settings = settings
        self._lock = asyncio.Lock()
        self._external_asset_baselines: dict[str, Decimal] = {}
        self._external_asset_baseline_quantities: dict[str, Decimal] = {}
        self._legacy_snapshot: tuple[
            tuple[Mapping[str, Any], ...],
            tuple[Mapping[str, Any], ...],
            tuple[Mapping[str, Any], ...],
        ] | None = None

    def legacy_snapshot(
        self,
    ) -> tuple[
        list[Mapping[str, Any]],
        list[Mapping[str, Any]],
        list[Mapping[str, Any]],
    ]:
        """Return the last fully reconciled open, completed, and fill rows.

        Legacy read endpoints must not launch another set of remote OKX reads.
        Reconciliation already collects the authoritative private state and
        publishes it atomically; reusing that snapshot avoids request storms
        and keeps all three endpoints on the same generation.
        """

        if self._legacy_snapshot is None:
            raise BrokerConnectionError("OKX reconciliation snapshot is unavailable")
        open_orders, completed_orders, trades = self._legacy_snapshot
        return (
            deepcopy(list(open_orders)),
            deepcopy(list(completed_orders)),
            deepcopy(list(trades)),
        )

    def record_order(self, order: Mapping[str, Any]) -> None:
        """Fold a mutation acknowledgement or private stream update into the cache."""

        if self._legacy_snapshot is None:
            return
        open_orders, completed_orders, trades = self._legacy_snapshot
        order_id = str(order.get("id") or "").strip()
        if not order_id:
            return
        next_open = [row for row in open_orders if str(row.get("id") or "").strip() != order_id]
        next_completed = [
            row for row in completed_orders if str(row.get("id") or "").strip() != order_id
        ]
        if order_status(order) in {"SUBMITTED", "PARTIALLY_FILLED"}:
            next_open.append(order)
        else:
            next_completed.append(order)
        self._legacy_snapshot = (tuple(next_open), tuple(next_completed), trades)

    def record_trade(self, trade: Mapping[str, Any]) -> None:
        """Fold a private fill stream row into the cached execution snapshot."""

        if self._legacy_snapshot is None:
            return
        info = trade.get("info") if isinstance(trade.get("info"), Mapping) else {}
        trade_id = str(
            trade.get("id") or info.get("tradeId") or info.get("fillId") or ""
        ).strip()
        if not trade_id:
            return
        open_orders, completed_orders, trades = self._legacy_snapshot
        next_trades: list[Mapping[str, Any]] = []
        for row in trades:
            row_info = row.get("info") if isinstance(row.get("info"), Mapping) else {}
            row_id = str(
                row.get("id") or row_info.get("tradeId") or row_info.get("fillId") or ""
            ).strip()
            if row_id != trade_id:
                next_trades.append(row)
        next_trades.append(trade)
        self._legacy_snapshot = (open_orders, completed_orders, tuple(next_trades))

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            balance = await self.client.fetch_balance()
            self._validate_balance_boundary(balance)
            orders: list[Mapping[str, Any]] = []
            trades: list[Mapping[str, Any]] = []
            open_order_rows: list[Mapping[str, Any]] = []
            completed_order_rows: list[Mapping[str, Any]] = []
            symbol_snapshots = await asyncio.gather(
                *(
                    asyncio.gather(
                        self.client.fetch_open_orders(symbol),
                        self.client.fetch_closed_orders(symbol),
                        self.client.fetch_my_trades(symbol),
                    )
                    for symbol in self.settings.allowed_symbols
                )
            )
            for open_orders, closed_orders, symbol_trades in symbol_snapshots:
                open_order_rows.extend(open_orders)
                completed_order_rows.extend(closed_orders)
                by_id: dict[str, Mapping[str, Any]] = {}
                for order in [*open_orders, *closed_orders]:
                    order_id = str(order.get("id") or "").strip()
                    if order_id:
                        by_id[order_id] = order
                orders.extend(by_id.values())
                trades.extend(symbol_trades)

            trades_by_id: dict[str, Mapping[str, Any]] = {}
            for trade in trades:
                info = trade.get("info") if isinstance(trade.get("info"), Mapping) else {}
                trade_id = str(
                    trade.get("id") or info.get("tradeId") or info.get("fillId") or ""
                ).strip()
                if not trade_id:
                    raise ValueError("OKX trade is missing its deterministic identity")
                fee = trade.get("fee") if isinstance(trade.get("fee"), Mapping) else {}
                fee_currency = str(
                    fee.get("currency") or info.get("feeCcy") or "USDT"
                ).upper()
                if fee_currency not in {"BTC", "ETH", "USDT"}:
                    raise ValueError(
                        f"OKX fee currency requires an approved conversion: {fee_currency}"
                    )
                trades_by_id[trade_id] = trade
            trades = list(trades_by_id.values())

            account_id = "okx-demo"
            mapped_positions = positions(
                balance,
                account_id=account_id,
                execution_target_id=self.settings.execution_target_id,
            )
            average_by_instrument = self._average_prices(trades)
            for item in mapped_positions:
                quantity = Decimal(item["quantityDecimal"])
                if quantity == 0:
                    continue
                instrument = item["instrumentId"]
                if instrument not in self._external_asset_baseline_quantities:
                    self._external_asset_baseline_quantities[instrument] = quantity
                baseline_quantity = self._external_asset_baseline_quantities[instrument]
                baseline_average = self._external_asset_baselines.get(instrument)
                if baseline_quantity > 0 and baseline_average is None:
                    symbol = next(
                        (
                            candidate
                            for candidate in self.settings.allowed_symbols
                            if instrument_id(candidate) == instrument
                        ),
                        None,
                    )
                    if symbol is None:
                        raise BrokerConnectionError(
                            "OKX asset balance has no supported instrument mapping",
                            details={"instrument_id": instrument},
                        )
                    ticker = await self.client.fetch_ticker(symbol)
                    baseline_average = self._baseline_price(ticker)
                    self._external_asset_baselines[instrument] = baseline_average

                incremental_quantity = max(Decimal(0), quantity - baseline_quantity)
                trade_average = average_by_instrument.get(instrument)
                if baseline_quantity > 0 and incremental_quantity == 0:
                    average = baseline_average
                    item["positionGroupId"] = "external-asset-baseline"
                    item["markPriceDecimal"] = canonical(baseline_average)
                elif baseline_quantity > 0 and baseline_average is not None:
                    incremental_average = trade_average or baseline_average
                    average = (
                        baseline_quantity * baseline_average
                        + incremental_quantity * incremental_average
                    ) / quantity
                elif trade_average is not None:
                    average = trade_average
                else:
                    symbol = next(
                        (
                            candidate
                            for candidate in self.settings.allowed_symbols
                            if instrument_id(candidate) == instrument
                        ),
                        None,
                    )
                    if symbol is None:
                        raise BrokerConnectionError(
                            "OKX asset balance has no supported instrument mapping",
                            details={"instrument_id": instrument},
                        )
                    ticker = await self.client.fetch_ticker(symbol)
                    average = self._baseline_price(ticker)
                item["averagePriceDecimal"] = canonical(average)

            snapshot = {
                "schemaVersion": "broker-reconciliation.v1",
                "executionTargetId": self.settings.execution_target_id,
                "orderUpdates": [
                    order_update(order, execution_target_id=self.settings.execution_target_id)
                    for order in sorted(orders, key=lambda item: str(item.get("id") or ""))
                ],
                "fills": [
                    fill(trade, execution_target_id=self.settings.execution_target_id)
                    for trade in sorted(trades, key=lambda item: str(item.get("id") or ""))
                ],
                "positions": mapped_positions,
                "balances": balance_payloads(
                    balance,
                    account_id=account_id,
                    execution_target_id=self.settings.execution_target_id,
                ),
            }
            self._legacy_snapshot = (
                tuple(self._deduplicate_orders(open_order_rows)),
                tuple(self._deduplicate_orders(completed_order_rows)),
                tuple(trades),
            )
            return snapshot

    @staticmethod
    def _deduplicate_orders(
        rows: list[Mapping[str, Any]],
    ) -> list[Mapping[str, Any]]:
        by_id: dict[str, Mapping[str, Any]] = {}
        anonymous: list[Mapping[str, Any]] = []
        for row in rows:
            order_id = str(row.get("id") or "").strip()
            if order_id:
                by_id[order_id] = row
            else:
                anonymous.append(row)
        return [*by_id.values(), *anonymous]

    @staticmethod
    def _validate_balance_boundary(balance: Mapping[str, Any]) -> None:
        info = balance.get("info") if isinstance(balance.get("info"), Mapping) else {}
        data = info.get("data") if isinstance(info.get("data"), list) else []
        details = data[0].get("details", []) if data and isinstance(data[0], Mapping) else []
        for item in details:
            if not isinstance(item, Mapping):
                continue
            currency = str(item.get("ccy") or "").upper()
            liability = Decimal(str(item.get("liab") or "0"))
            if liability != 0:
                LOGGER.error(
                    "OKX Demo balance reconciliation found a non-zero liability",
                    extra={
                        "event": "broker.crypto.balance_drift",
                        "broker.adapter_id": "ccxt_crypto",
                        "broker.drift_type": "non_zero_liability",
                        "broker.currency": currency,
                    },
                )
                raise BrokerConnectionError(
                    "OKX Demo Spot account contains a non-zero liability",
                    details={"currency": currency},
                )

    @staticmethod
    def _average_prices(trades: list[Mapping[str, Any]]) -> dict[str, Decimal]:
        quantities: dict[str, Decimal] = {}
        costs: dict[str, Decimal] = {}
        from .mapping import instrument_id

        for trade in trades:
            symbol = str(trade.get("symbol") or "")
            if not symbol:
                continue
            key = instrument_id(symbol)
            quantity = Decimal(str(trade.get("amount") or "0"))
            price = Decimal(str(trade.get("price") or "0"))
            side = str(trade.get("side") or "").lower()
            if side == "buy":
                fee = trade.get("fee") if isinstance(trade.get("fee"), Mapping) else {}
                base_currency = symbol.partition("/")[0].upper()
                fee_currency = str(fee.get("currency") or "").upper()
                base_fee = (
                    Decimal(str(fee.get("cost") or "0"))
                    if fee_currency == base_currency
                    else Decimal(0)
                )
                quantities[key] = quantities.get(key, Decimal(0)) + max(
                    Decimal(0), quantity - base_fee
                )
                costs[key] = costs.get(key, Decimal(0)) + quantity * price
            elif side == "sell":
                previous = quantities.get(key, Decimal(0))
                average = costs.get(key, Decimal(0)) / previous if previous > 0 else Decimal(0)
                quantities[key] = max(Decimal(0), previous - quantity)
                costs[key] = max(Decimal(0), costs.get(key, Decimal(0)) - quantity * average)
        return {
            key: costs[key] / quantity
            for key, quantity in quantities.items()
            if quantity > 0 and costs.get(key, Decimal(0)) > 0
        }

    @staticmethod
    def _baseline_price(ticker: Mapping[str, Any]) -> Decimal:
        for key in ("last", "close"):
            try:
                value = Decimal(str(ticker.get(key) or "0"))
            except InvalidOperation:
                continue
            if value.is_finite() and value > 0:
                return value
        try:
            bid = Decimal(str(ticker.get("bid") or "0"))
            ask = Decimal(str(ticker.get("ask") or "0"))
        except InvalidOperation as exc:
            raise BrokerConnectionError(
                "OKX asset balance has no positive valuation baseline"
            ) from exc
        if bid.is_finite() and ask.is_finite() and bid > 0 and ask > 0:
            return (bid + ask) / Decimal(2)
        raise BrokerConnectionError("OKX asset balance has no positive valuation baseline")


def clock_skew_ms(exchange_time_ms: int) -> int:
    local = int(datetime.now(UTC).timestamp() * 1000)
    return abs(local - int(exchange_time_ms))
