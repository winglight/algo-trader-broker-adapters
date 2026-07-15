"""Pure vendor-to-Broker-SDK mappings."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping

from algo_trader_broker_sdk import (
    AccountSummaryItem,
    HistoricalBar,
    OrderResult,
    PositionItem,
    TradeUpdate,
)


_STATUS_MAP = {
    "accepted": "PendingSubmit",
    "pending_new": "PendingSubmit",
    "accepted_for_bidding": "PendingSubmit",
    "new": "Submitted",
    "held": "Submitted",
    "partially_filled": "Submitted",
    "filled": "Filled",
    "canceled": "Cancelled",
    "cancelled": "Cancelled",
    "replaced": "Cancelled",
    "rejected": "Rejected",
    "expired": "Inactive",
    "done_for_day": "Inactive",
    "stopped": "Inactive",
    "calculated": "Inactive",
    "suspended": "Inactive",
    "pending_cancel": "Submitted",
    "pending_replace": "Submitted",
}


def value(item: Any, name: str, default: Any = None) -> Any:
    if isinstance(item, Mapping):
        return item.get(name, default)
    return getattr(item, name, default)


def scalar(item: Any) -> Any:
    if isinstance(item, Enum):
        return item.value
    return item


def text(item: Any) -> str:
    raw = scalar(item)
    return "" if raw is None else str(raw)


def number(item: Any, default: float = 0.0) -> float:
    if item in (None, ""):
        return default
    if isinstance(item, Decimal):
        return float(item)
    return float(item)


def utc_datetime(item: Any, *, default: datetime | None = None) -> datetime:
    if isinstance(item, datetime):
        parsed = item
    elif item not in (None, ""):
        parsed = datetime.fromisoformat(str(item).strip().replace("Z", "+00:00"))
    elif default is not None:
        parsed = default
    else:
        parsed = datetime.now(UTC)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def broker_status(native_status: Any) -> str:
    return _STATUS_MAP.get(text(native_status).strip().lower(), "Unknown")


def order_metadata(order: Any, *, event: str | None = None) -> dict[str, Any]:
    native_status = text(value(order, "status"))
    native: dict[str, Any] = {"status": native_status}
    if event:
        native["event"] = event
    replaced_by = value(order, "replaced_by")
    replaces = value(order, "replaces")
    if replaced_by:
        native["replacedBy"] = text(replaced_by)
    if replaces:
        native["replaces"] = text(replaces)
    return {
        "schemaVersion": 1,
        "native": native,
        "diagnostics": {},
        "extensions": {},
    }


def map_order_result(order: Any) -> OrderResult:
    order_id = text(value(order, "id")).strip()
    symbol = text(value(order, "symbol")).strip().upper()
    quantity = number(value(order, "qty"))
    filled = number(value(order, "filled_qty"))
    return OrderResult(
        adapter_id="alpaca_paper",
        adapter_order_id=order_id,
        adapter_order_ref=text(value(order, "client_order_id")).strip() or None,
        status=broker_status(value(order, "status")),
        filled=filled,
        remaining=max(0.0, quantity - filled),
        avg_fill_price=(
            number(value(order, "filled_avg_price"))
            if value(order, "filled_avg_price") not in (None, "")
            else None
        ),
        contract={"symbol": symbol, "secType": "STK", "currency": "USD"},
        adapter_metadata=order_metadata(order),
    )


def map_trade_update(
    order: Any,
    *,
    event: str | None = None,
    execution_id: str | None = None,
    event_time: Any = None,
    last_fill_price: Any = None,
    last_fill_quantity: Any = None,
) -> TradeUpdate:
    quantity = number(value(order, "qty"))
    filled = number(value(order, "filled_qty"))
    return TradeUpdate(
        adapter_id="alpaca_paper",
        adapter_order_id=text(value(order, "id")).strip() or None,
        adapter_order_ref=text(value(order, "client_order_id")).strip() or None,
        adapter_execution_id=str(execution_id).strip() if execution_id else None,
        adapter_metadata=order_metadata(order, event=event),
        status=broker_status(value(order, "status")),
        filled=filled,
        remaining=max(0.0, quantity - filled),
        avg_fill_price=(
            number(value(order, "filled_avg_price"))
            if value(order, "filled_avg_price") not in (None, "")
            else None
        ),
        last_fill_price=(
            number(last_fill_price) if last_fill_price not in (None, "") else None
        ),
        last_fill_quantity=(
            number(last_fill_quantity)
            if last_fill_quantity not in (None, "")
            else None
        ),
        rejection_reason=(
            text(value(order, "reject_reason")).strip() or None
            if broker_status(value(order, "status")) == "Rejected"
            else None
        ),
        event_time=utc_datetime(
            event_time
            or value(order, "updated_at")
            or value(order, "filled_at")
            or value(order, "created_at")
        ),
        client_order_id=text(value(order, "client_order_id")).strip() or None,
        message={
            "symbol": text(value(order, "symbol")).strip().upper(),
            "account": None,
            "source": "alpaca_trade_update" if event else "alpaca_reconciliation",
        },
    )


def map_fill_activity(activity: Any, order: Any | None = None) -> TradeUpdate:
    source = order or {
        "id": value(activity, "order_id"),
        "symbol": value(activity, "symbol"),
        "qty": value(activity, "qty"),
        "filled_qty": value(activity, "qty"),
        "filled_avg_price": value(activity, "price"),
        "status": "filled",
    }
    return map_trade_update(
        source,
        event="fill",
        execution_id=text(value(activity, "id")).strip() or None,
        event_time=value(activity, "transaction_time") or value(activity, "date"),
        last_fill_price=value(activity, "price"),
        last_fill_quantity=value(activity, "qty"),
    )


def map_account(account: Any) -> list[AccountSummaryItem]:
    account_id = text(value(account, "id") or value(account, "account_number"))
    currency = text(value(account, "currency") or "USD")
    fields = {
        "NetLiquidation": value(account, "equity"),
        "CashBalance": value(account, "cash"),
        "BuyingPower": value(account, "buying_power"),
        "RegTBuyingPower": value(account, "regt_buying_power"),
        "DaytradingBuyingPower": value(account, "daytrading_buying_power"),
        "LastEquity": value(account, "last_equity"),
        "InitialMarginReq": value(account, "initial_margin"),
        "MaintenanceMarginReq": value(account, "maintenance_margin"),
    }
    return [
        AccountSummaryItem(
            account=account_id,
            tag=tag,
            value=text(raw),
            currency=currency,
        )
        for tag, raw in fields.items()
        if raw not in (None, "")
    ]


def map_position(position: Any, *, account_id: str) -> PositionItem:
    return PositionItem(
        account=account_id,
        contract_id=None,
        symbol=text(value(position, "symbol")).strip().upper() or None,
        sec_type="STK",
        exchange=text(value(position, "exchange")).strip() or None,
        currency="USD",
        position=number(value(position, "qty")),
        avg_cost=number(value(position, "avg_entry_price")),
        local_symbol=text(value(position, "symbol")).strip().upper() or None,
        primary_exchange=text(value(position, "exchange")).strip() or None,
        trading_class=None,
    )


def map_bar(bar: Any) -> HistoricalBar:
    return HistoricalBar(
        time=utc_datetime(value(bar, "timestamp")),
        open=number(value(bar, "open")),
        high=number(value(bar, "high")),
        low=number(value(bar, "low")),
        close=number(value(bar, "close")),
        volume=number(value(bar, "volume")),
        wap=(number(value(bar, "vwap")) if value(bar, "vwap") not in (None, "") else None),
        count=(int(value(bar, "trade_count")) if value(bar, "trade_count") is not None else None),
    )
