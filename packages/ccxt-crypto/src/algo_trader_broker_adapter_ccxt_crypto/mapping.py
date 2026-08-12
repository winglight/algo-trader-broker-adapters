"""Pure CCXT/OKX to broker-neutral payload mappings."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from algo_trader_broker_sdk import AccountSummaryItem, PositionItem, TradeUpdate

from .quantizer import canonical

_STATUS = {
    "live": "SUBMITTED",
    "open": "SUBMITTED",
    "partially_filled": "PARTIALLY_FILLED",
    "filled": "FILLED",
    "closed": "FILLED",
    "canceled": "CANCELLED",
    "cancelled": "CANCELLED",
    "rejected": "REJECTED",
    "expired": "EXPIRED",
}
_LEGACY_STATUS = {
    "SUBMITTED": "Submitted",
    "PARTIALLY_FILLED": "Submitted",
    "FILLED": "Filled",
    "CANCELLED": "Cancelled",
    "REJECTED": "Rejected",
    "EXPIRED": "Inactive",
}


def value(item: Mapping[str, Any], name: str, default: Any = None) -> Any:
    result = item.get(name, default)
    return default if result is None else result


def decimal(item: Any, default: str = "0") -> Decimal:
    return Decimal(str(default if item in (None, "") else item))


def timestamp(value: Any = None) -> str:
    if value not in (None, ""):
        raw = int(str(value))
        parsed = datetime.fromtimestamp(raw / 1000, tz=UTC)
    else:
        parsed = datetime.now(UTC)
    return parsed.isoformat().replace("+00:00", "Z")


def available_timestamp(event_time: str) -> str:
    """Return observation time without preceding the exact emitted event time."""

    emitted = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    observed = max(datetime.now(UTC), emitted)
    return observed.isoformat().replace("+00:00", "Z")


def instrument_id(symbol: str) -> str:
    base, quote = symbol.upper().split("/", 1)
    return f"crypto-spot:{base}-{quote}:OKX"


def order_status(order: Mapping[str, Any]) -> str:
    info = order.get("info") if isinstance(order.get("info"), Mapping) else {}
    if str(info.get("sCode") or "0") != "0" and not str(order.get("id") or "").strip():
        return "REJECTED"
    return _STATUS.get(str(order.get("status") or info.get("state") or "").strip().lower(), "SUBMITTED")


def order_update(
    order: Mapping[str, Any],
    *,
    execution_target_id: str,
    command_id: str | None = None,
    client_order_id: str | None = None,
) -> dict[str, Any]:
    info = order.get("info") if isinstance(order.get("info"), Mapping) else {}
    broker_id = str(order.get("id") or info.get("ordId") or "").strip()
    client_id = str(
        client_order_id
        or order.get("clientOrderId")
        or info.get("clOrdId")
        or "unknown-client"
    ).strip()
    symbol = str(order.get("symbol") or info.get("instId") or "").replace("-", "/")
    amount = decimal(order.get("amount") or info.get("sz"))
    filled = decimal(order.get("filled") or info.get("accFillSz"))
    remaining = max(Decimal(0), amount - filled)
    average = order.get("average") or info.get("avgPx") or None
    event_ms = info.get("uTime") or order.get("lastTradeTimestamp") or order.get("timestamp")
    event_time = timestamp(event_ms)
    now = available_timestamp(event_time)
    status = order_status(order)
    if status == "FILLED":
        remaining = Decimal(0)
    return {
        "schemaVersion": "broker-order-update.v2",
        "identity": {
            "schemaVersion": "broker-order-identity.v2",
            "executionTargetId": execution_target_id,
            "brokerOrderId": broker_id,
        },
        "commandId": str(command_id or info.get("tag") or client_id),
        "clientOrderId": client_id,
        "instrumentId": instrument_id(symbol),
        "status": status,
        "cumulativeFilledDecimal": canonical(filled),
        "remainingDecimal": canonical(remaining),
        "averageFillPriceDecimal": canonical(decimal(average)) if average not in (None, "", "0") else None,
        "eventTime": event_time,
        "availableAt": now,
    }


def fill(
    trade: Mapping[str, Any],
    *,
    execution_target_id: str,
    position_group_id: str = "crypto-spot-paper",
) -> dict[str, Any]:
    info = trade.get("info") if isinstance(trade.get("info"), Mapping) else {}
    trade_id = str(trade.get("id") or info.get("tradeId") or info.get("fillId") or "").strip()
    order_id = str(trade.get("order") or info.get("ordId") or "").strip()
    symbol = str(trade.get("symbol") or info.get("instId") or "").replace("-", "/")
    fee = trade.get("fee") if isinstance(trade.get("fee"), Mapping) else {}
    fee_cost = abs(decimal(fee.get("cost") if fee else info.get("fee")))
    fee_currency = str(fee.get("currency") if fee else info.get("feeCcy") or "USDT").upper()
    liquidity = str(trade.get("takerOrMaker") or "UNKNOWN").upper()
    if liquidity not in {"MAKER", "TAKER"}:
        liquidity = "UNKNOWN"
    event_ms = trade.get("timestamp") or info.get("fillTime") or info.get("ts")
    return {
        "schemaVersion": "broker-fill.v2",
        "fillId": f"{execution_target_id}:{trade_id}",
        "brokerExecutionId": trade_id,
        "orderIdentity": {
            "schemaVersion": "broker-order-identity.v2",
            "executionTargetId": execution_target_id,
            "brokerOrderId": order_id,
        },
        "instrumentId": instrument_id(symbol),
        "positionGroupId": position_group_id,
        "legId": None,
        "side": str(trade.get("side") or info.get("side") or "BUY").upper(),
        "quantityDecimal": canonical(decimal(trade.get("amount") or info.get("fillSz"))),
        "priceDecimal": canonical(decimal(trade.get("price") or info.get("fillPx"))),
        "feeDecimal": canonical(fee_cost),
        "feeCurrency": fee_currency,
        "liquidityRole": liquidity,
        "eventTime": timestamp(event_ms),
        "availableAt": timestamp(),
    }


def _currency_records(balance: Mapping[str, Any]) -> list[tuple[str, Mapping[str, Any]]]:
    info = balance.get("info") if isinstance(balance.get("info"), Mapping) else {}
    data = info.get("data") if isinstance(info.get("data"), list) else []
    details = data[0].get("details", []) if data and isinstance(data[0], Mapping) else []
    records: dict[str, Mapping[str, Any]] = {
        str(item.get("ccy") or "").upper(): item
        for item in details
        if isinstance(item, Mapping) and str(item.get("ccy") or "").strip()
    }
    for currency in ("BTC", "ETH", "USDT"):
        if currency not in records:
            records[currency] = {
                "ccy": currency,
                "cashBal": (balance.get("total") or {}).get(currency, "0"),
                "availBal": (balance.get("free") or {}).get(currency, "0"),
                "frozenBal": (balance.get("used") or {}).get(currency, "0"),
                "liab": "0",
            }
    return sorted(records.items())


def balance_payloads(
    balance: Mapping[str, Any], *, account_id: str, execution_target_id: str
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    now = timestamp()
    for currency, item in _currency_records(balance):
        if currency not in {"BTC", "ETH", "USDT"}:
            # Unsupported holdings remain at the broker. They are not part of
            # this Adapter's published/tradable asset universe.
            continue
        if decimal(item.get("liab")) != 0:
            raise ValueError(f"OKX Demo liability must be zero: {currency}")
        for balance_type, native_key in (("CASH", "cashBal"), ("AVAILABLE", "availBal")):
            result.append(
                {
                    "schemaVersion": "broker-balance.v2",
                    "accountId": account_id,
                    "executionTargetId": execution_target_id,
                    "currency": currency,
                    "balanceType": balance_type,
                    "amountDecimal": canonical(decimal(item.get(native_key))),
                    "eventTime": now,
                    "availableAt": now,
                }
            )
    return result


def positions(
    balance: Mapping[str, Any], *, account_id: str, execution_target_id: str
) -> list[dict[str, Any]]:
    now = timestamp()
    totals = {currency: decimal(item.get("cashBal")) for currency, item in _currency_records(balance)}
    return [
        {
            "schemaVersion": "broker-position.v2",
            "accountId": account_id,
            "executionTargetId": execution_target_id,
            "instrumentId": instrument_id(f"{currency}/USDT"),
            "positionGroupId": None,
            "legId": None,
            "quantityDecimal": canonical(totals.get(currency, Decimal(0))),
            "averagePriceDecimal": None,
            "markPriceDecimal": None,
            "settlementCurrency": "USDT",
            "eventTime": now,
            "availableAt": now,
        }
        for currency in ("BTC", "ETH")
    ]


def account_summary(
    balance: Mapping[str, Any],
    *,
    account_id: str,
    prices_in_usdt: Mapping[str, Decimal],
) -> list[AccountSummaryItem]:
    """Return one coherent account valuation instead of duplicate per-asset tags.

    OKX reports one balance row per asset. Reusing ``CashBalance`` for every row
    caused the account service to retain the first row's currency while the last
    row overwrote its value. Phase 4 values the allowlisted Spot inventory in
    USDT, with USD/USDC stable-unit views exposed explicitly for the dashboard.
    """

    total_usdt = Decimal(0)
    available_usdt = Decimal(0)
    total_usd = Decimal(0)
    available_usd = Decimal(0)
    for currency, item in _currency_records(balance):
        if currency not in {"BTC", "ETH", "USDT"}:
            continue
        price = prices_in_usdt.get(currency)
        cash = decimal(item.get("cashBal"))
        available = decimal(item.get("availBal"))
        if price is None:
            if cash or available:
                raise ValueError(f"missing USDT valuation price for {currency}")
            price = Decimal(0)
        usdt_value = cash * price
        available_usdt_value = available * price
        total_usdt += usdt_value
        available_usdt += available_usdt_value

        raw_usd_value = item.get("eqUsd")
        usd_value = decimal(raw_usd_value) if raw_usd_value not in (None, "") else usdt_value
        total_usd += usd_value
        if cash:
            available_usd += usd_value * available / cash
        elif not available:
            available_usd += Decimal(0)
        else:
            # A provider row with available funds but no cash balance is
            # inconsistent; retain the bounded USDT fallback rather than
            # inventing a division-based FX rate.
            available_usd += available_usdt_value

    # USD uses OKX's per-asset eqUsd when available. USDC follows the reviewed
    # 1:1 USD stable-unit accounting policy, so no unapproved FX instrument is
    # queried by the adapter.
    def values(total: Decimal, available: Decimal) -> dict[str, Decimal]:
        return {
            "NetLiquidation": total,
            "TotalCashValue": total,
            "CashBalance": total,
            "AvailableFunds": available,
            "InitialMarginRequirement": Decimal(0),
            "MaintenanceMargin": Decimal(0),
            "RealizedPnL": Decimal(0),
            "UnrealizedPnL": Decimal(0),
        }

    usd_values = values(total_usd, available_usd)
    result = [
        AccountSummaryItem(account_id, tag, canonical(amount), "USD")
        for tag, amount in usd_values.items()
    ]
    for currency, currency_values in (
        ("USDT", values(total_usdt, available_usdt)),
        ("USDC", usd_values),
    ):
        result.extend(
            AccountSummaryItem(
                account_id,
                f"{tag}{currency}",
                canonical(amount),
                currency,
            )
            for tag, amount in currency_values.items()
        )
    return result


def legacy_positions(balance: Mapping[str, Any], *, account_id: str) -> list[PositionItem]:
    totals = {currency: decimal(item.get("cashBal")) for currency, item in _currency_records(balance)}
    return [
        PositionItem(
            account=account_id,
            contract_id=None,
            symbol=f"{currency}/USDT",
            sec_type="CRYPTO_SPOT",
            exchange="OKX",
            currency="USDT",
            position=float(totals.get(currency, Decimal(0))),
            avg_cost=0.0,
            local_symbol=f"{currency}-USDT",
        )
        for currency in ("BTC", "ETH")
    ]


def legacy_trade_update(order: Mapping[str, Any]) -> TradeUpdate:
    status = order_status(order)
    amount = decimal(order.get("amount"))
    filled_amount = decimal(order.get("filled"))
    return TradeUpdate(
        adapter_id="ccxt_crypto",
        adapter_order_id=str(order.get("id") or "").strip() or None,
        adapter_order_ref=str(order.get("clientOrderId") or "").strip() or None,
        status=_LEGACY_STATUS.get(status, "Submitted"),
        filled=float(filled_amount),
        remaining=float(max(Decimal(0), amount - filled_amount)),
        avg_fill_price=float(decimal(order.get("average"))) if order.get("average") else None,
        event_time=datetime.fromisoformat(timestamp(order.get("timestamp"))),
        client_order_id=str(order.get("clientOrderId") or "").strip() or None,
        adapter_metadata={
            "schemaVersion": 1,
            "native": {"venue": "OKX", "status": str(order.get("status") or "")},
            "diagnostics": {},
            "extensions": {},
        },
    )


def legacy_fill_update(trade: Mapping[str, Any]) -> TradeUpdate:
    info = trade.get("info") if isinstance(trade.get("info"), Mapping) else {}
    fee = trade.get("fee") if isinstance(trade.get("fee"), Mapping) else {}
    execution_id = str(trade.get("id") or info.get("tradeId") or "").strip() or None
    return TradeUpdate(
        adapter_id="ccxt_crypto",
        adapter_order_id=str(trade.get("order") or info.get("ordId") or "").strip() or None,
        adapter_execution_id=execution_id,
        status=None,
        last_fill_price=float(decimal(trade.get("price") or info.get("fillPx"))),
        last_fill_quantity=float(decimal(trade.get("amount") or info.get("fillSz"))),
        commission=float(abs(decimal(fee.get("cost") if fee else info.get("fee")))),
        event_time=datetime.fromisoformat(
            timestamp(trade.get("timestamp") or info.get("fillTime"))
        ),
        message={
            "symbol": str(trade.get("symbol") or info.get("instId") or ""),
            "source": "okx_private_trades",
            "commissionReport": {
                "execId": execution_id,
                "commission": float(abs(decimal(fee.get("cost") if fee else info.get("fee")))),
                "currency": str(fee.get("currency") if fee else info.get("feeCcy") or "USDT"),
            },
        },
        adapter_metadata={
            "schemaVersion": 1,
            "native": {"venue": "OKX", "liquidity": str(trade.get("takerOrMaker") or "")},
            "diagnostics": {},
            "extensions": {},
        },
    )
