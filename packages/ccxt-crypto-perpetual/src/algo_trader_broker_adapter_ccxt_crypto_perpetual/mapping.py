"""Broker-neutral mapping for OKX USDT linear perpetual payloads."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from .quantizer import PerpetualMarketRules, canonical, instrument_id

_STATUS = {
    "open": "SUBMITTED",
    "live": "SUBMITTED",
    "partially_filled": "PARTIALLY_FILLED",
    "partially-filled": "PARTIALLY_FILLED",
    "closed": "FILLED",
    "filled": "FILLED",
    "canceled": "CANCELLED",
    "cancelled": "CANCELLED",
    "rejected": "REJECTED",
    "expired": "EXPIRED",
}


def timestamp(value: Any = None) -> str:
    if value in (None, ""):
        parsed = datetime.now(UTC)
    elif isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, Decimal)):
        parsed = datetime.fromtimestamp(float(value) / 1000, tz=UTC)
    else:
        token = str(value)
        parsed = datetime.fromisoformat(token[:-1] + "+00:00" if token.endswith("Z") else token)
    return parsed.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def decimal(value: Any, default: str = "0") -> Decimal:
    return Decimal(canonical(default if value in (None, "") else value))


def _info(row: Mapping[str, Any]) -> Mapping[str, Any]:
    return row.get("info") if isinstance(row.get("info"), Mapping) else {}


def _symbol(row: Mapping[str, Any]) -> str:
    info = _info(row)
    symbol = str(row.get("symbol") or "").strip()
    if symbol:
        return symbol
    native = str(info.get("instId") or "")
    if native == "BTC-USDT-SWAP":
        return "BTC/USDT:USDT"
    if native == "ETH-USDT-SWAP":
        return "ETH/USDT:USDT"
    raise ValueError("OKX perpetual payload has no allowlisted symbol")


def order_status(order: Mapping[str, Any]) -> str:
    info = _info(order)
    return _STATUS.get(str(order.get("status") or info.get("state") or "").lower(), "SUBMITTED")


def order_update(
    order: Mapping[str, Any],
    *,
    execution_target_id: str,
    command_id: str | None = None,
    client_order_id: str | None = None,
) -> dict[str, Any]:
    info = _info(order)
    broker_id = str(order.get("id") or info.get("ordId") or "").strip()
    client_id = str(
        client_order_id or order.get("clientOrderId") or info.get("clOrdId") or "unknown-client"
    ).strip()
    amount = decimal(order.get("amount") or info.get("sz"))
    filled = decimal(order.get("filled") or info.get("accFillSz"))
    remaining = max(Decimal(0), amount - filled)
    status = order_status(order)
    if status == "FILLED":
        remaining = Decimal(0)
    event_time = timestamp(
        info.get("uTime") or order.get("lastTradeTimestamp") or order.get("timestamp")
    )
    return {
        "schemaVersion": "broker-order-update.v2",
        "identity": {
            "schemaVersion": "broker-order-identity.v2",
            "executionTargetId": execution_target_id,
            "brokerOrderId": broker_id,
        },
        "commandId": str(command_id or info.get("tag") or client_id),
        "clientOrderId": client_id,
        "instrumentId": instrument_id(_symbol(order)),
        "status": status,
        "cumulativeFilledDecimal": canonical(filled),
        "remainingDecimal": canonical(remaining),
        "averageFillPriceDecimal": (
            canonical(decimal(order.get("average") or info.get("avgPx")))
            if (order.get("average") or info.get("avgPx")) not in (None, "", "0")
            else None
        ),
        "eventTime": event_time,
        "availableAt": timestamp(),
    }


def fill(
    trade: Mapping[str, Any],
    *,
    execution_target_id: str,
    position_group_id: str = "phase5-paper",
) -> dict[str, Any]:
    info = _info(trade)
    trade_id = str(trade.get("id") or info.get("tradeId") or info.get("fillId") or "").strip()
    order_id = str(trade.get("order") or info.get("ordId") or "").strip()
    fee = trade.get("fee") if isinstance(trade.get("fee"), Mapping) else {}
    fee_cost = abs(decimal(fee.get("cost") if fee else info.get("fee")))
    event_time = timestamp(trade.get("timestamp") or info.get("fillTime") or info.get("ts"))
    liquidity_role = str(trade.get("takerOrMaker") or "UNKNOWN").upper()
    if liquidity_role not in {"MAKER", "TAKER"}:
        liquidity_role = "UNKNOWN"
    return {
        "schemaVersion": "broker-fill.v2",
        "fillId": f"{execution_target_id}:{trade_id}",
        "brokerExecutionId": trade_id,
        "orderIdentity": {
            "schemaVersion": "broker-order-identity.v2",
            "executionTargetId": execution_target_id,
            "brokerOrderId": order_id,
        },
        "instrumentId": instrument_id(_symbol(trade)),
        "positionGroupId": position_group_id,
        "legId": None,
        "side": str(trade.get("side") or info.get("side") or "BUY").upper(),
        "quantityDecimal": canonical(decimal(trade.get("amount") or info.get("fillSz"))),
        "priceDecimal": canonical(decimal(trade.get("price") or info.get("fillPx"))),
        "feeDecimal": canonical(fee_cost),
        "feeCurrency": str(fee.get("currency") or info.get("feeCcy") or "USDT").upper(),
        "liquidityRole": liquidity_role,
        "eventTime": event_time,
        "availableAt": timestamp(),
    }


def _signed_contracts(position: Mapping[str, Any]) -> Decimal:
    contracts = decimal(position.get("contracts") or _info(position).get("pos"))
    side = str(position.get("side") or _info(position).get("posSide") or "net").lower()
    if side == "short" and contracts > 0:
        return -contracts
    return contracts


def position_payload(
    position: Mapping[str, Any],
    *,
    rules: PerpetualMarketRules,
    account_id: str,
    execution_target_id: str,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    info = _info(position)
    signed = _signed_contracts(position)
    entry = position.get("entryPrice") or info.get("avgPx")
    mark = position.get("markPrice") or info.get("markPx")
    liquidation = position.get("liquidationPrice") or info.get("liqPx")
    event_time = timestamp(position.get("timestamp") or info.get("uTime"))
    broker_position = {
        "schemaVersion": "broker-position.v2",
        "accountId": account_id,
        "executionTargetId": execution_target_id,
        "instrumentId": instrument_id(rules.symbol),
        "positionGroupId": "phase5-paper" if signed else None,
        "legId": None,
        "quantityDecimal": canonical(signed),
        "averagePriceDecimal": canonical(decimal(entry)) if signed else None,
        "markPriceDecimal": canonical(decimal(mark)) if mark not in (None, "") else None,
        "settlementCurrency": "USDT",
        "eventTime": event_time,
        "availableAt": timestamp(),
    }
    if signed == 0:
        return broker_position, None
    required_risk_fields = {
        "entryPrice": entry,
        "markPrice": mark,
        "indexPrice": position.get("indexPrice") or info.get("idxPx"),
        "liquidationPrice": liquidation,
        "initialMargin": position.get("initialMargin") or info.get("imr"),
        "maintenanceMargin": position.get("maintenanceMargin") or info.get("mmr"),
        "marginRatio": position.get("marginRatio") or info.get("mgnRatio"),
        "maintenanceTierId": position.get("maintenanceTierId") or info.get("tier"),
    }
    missing = sorted(
        name for name, value in required_risk_fields.items() if value in (None, "")
    )
    if missing:
        raise ValueError(
            "Non-flat perpetual position lacks required risk fields: "
            + ", ".join(missing)
        )
    if str(position.get("marginMode") or info.get("mgnMode") or "").lower() != "isolated":
        raise ValueError("Perpetual position is not isolated")
    leverage = decimal(position.get("leverage") or info.get("lever"))
    if leverage != Decimal(2):
        raise ValueError("Perpetual position leverage is not fixed 2x")
    base_exposure = signed * rules.contract_multiplier
    mark_decimal = decimal(mark)
    mark_notional = abs(base_exposure) * mark_decimal
    liquidation_decimal = decimal(liquidation)
    distance = abs(mark_decimal - liquidation_decimal) / mark_decimal
    risk = {
        "schemaVersion": "perpetual-position-risk.v1",
        "accountId": account_id,
        "executionTargetId": execution_target_id,
        "instrumentId": instrument_id(rules.symbol),
        "positionGroupId": "phase5-paper",
        "signedContractsDecimal": canonical(signed),
        "contractMultiplierDecimal": canonical(rules.contract_multiplier),
        "baseExposureDecimal": canonical(base_exposure),
        "markNotionalDecimal": canonical(mark_notional),
        "averageEntryPriceDecimal": canonical(decimal(entry)),
        "markPriceDecimal": canonical(mark_decimal),
        "indexPriceDecimal": canonical(decimal(required_risk_fields["indexPrice"])),
        "leverageDecimal": "2",
        "marginMode": "ISOLATED",
        "positionMode": "ONE_WAY",
        "initialMarginDecimal": canonical(decimal(required_risk_fields["initialMargin"])),
        "maintenanceMarginDecimal": canonical(
            decimal(required_risk_fields["maintenanceMargin"])
        ),
        "marginRatioDecimal": canonical(decimal(required_risk_fields["marginRatio"])),
        "liquidationPriceDecimal": canonical(liquidation_decimal),
        "liquidationDistanceDecimal": canonical(distance),
        "unrealizedPnlDecimal": canonical(
            signed * rules.contract_multiplier * (mark_decimal - decimal(entry))
        ),
        "settlementCurrency": "USDT",
        "maintenanceTierId": str(required_risk_fields["maintenanceTierId"]),
        "metadataVersion": 1,
        "eventTime": event_time,
        "availableAt": timestamp(),
        "reconciledAt": timestamp(),
    }
    return broker_position, risk


def balance_payloads(
    balance: Mapping[str, Any], *, account_id: str, execution_target_id: str
) -> list[dict[str, Any]]:
    total = balance.get("total") if isinstance(balance.get("total"), Mapping) else {}
    free = balance.get("free") if isinstance(balance.get("free"), Mapping) else {}
    used = balance.get("used") if isinstance(balance.get("used"), Mapping) else {}
    now = timestamp()
    values = {
        "CASH": decimal(total.get("USDT")),
        "EQUITY": decimal(total.get("USDT")),
        "AVAILABLE": decimal(free.get("USDT")),
        "MARGIN_USED": decimal(used.get("USDT")),
    }
    return [
        {
            "schemaVersion": "broker-balance.v2",
            "accountId": account_id,
            "executionTargetId": execution_target_id,
            "currency": "USDT",
            "balanceType": balance_type,
            "amountDecimal": canonical(amount),
            "eventTime": now,
            "availableAt": now,
        }
        for balance_type, amount in values.items()
    ]


def funding_entry(
    row: Mapping[str, Any],
    *,
    account_id: str,
    execution_target_id: str,
) -> dict[str, Any]:
    info = _info(row)
    ledger_id = str(row.get("id") or info.get("billId") or "").strip()
    if not ledger_id:
        raise ValueError("Funding ledger row lacks broker identity")
    signed = row.get("signedContracts") or info.get("pos")
    notional = row.get("markNotional") or info.get("notionalUsd")
    rate = row.get("fundingRate") or info.get("fundingRate")
    if signed in (None, "") or notional in (None, "") or rate in (None, ""):
        raise ValueError("Funding ledger row cannot be associated to position evidence")
    event_time = timestamp(row.get("timestamp") or info.get("ts"))
    return {
        "schemaVersion": "funding-ledger-entry.v1",
        "fundingEntryId": f"funding:{execution_target_id}:{ledger_id}",
        "brokerLedgerId": ledger_id,
        "accountId": account_id,
        "executionTargetId": execution_target_id,
        "instrumentId": instrument_id(_symbol(row)),
        "positionGroupId": "phase5-paper",
        "signedContractsDecimal": canonical(decimal(signed)),
        "markNotionalDecimal": canonical(abs(decimal(notional))),
        "fundingRateDecimal": canonical(decimal(rate)),
        "amountDecimal": canonical(decimal(row.get("amount") or info.get("pnl"))),
        "currency": str(row.get("code") or info.get("ccy") or "USDT").upper(),
        "source": "BROKER_ACTUAL",
        "eventTime": event_time,
        "availableAt": timestamp(),
        "reconciledAt": timestamp(),
    }


def market_data_objects(
    rate: Mapping[str, Any],
    *,
    symbol: str,
    market_data_target_id: str,
    metadata_hash: str,
    first_sequence: int,
) -> list[dict[str, Any]]:
    return [
        market_data_object(
            rate,
            object_type=object_type,
            symbol=symbol,
            market_data_target_id=market_data_target_id,
            metadata_hash=metadata_hash,
            sequence=first_sequence + offset,
        )
        for offset, object_type in enumerate(("mark", "index", "funding"))
    ]


def market_data_object(
    row: Mapping[str, Any],
    *,
    object_type: str,
    symbol: str,
    market_data_target_id: str,
    metadata_hash: str,
    sequence: int,
) -> dict[str, Any]:
    info = _info(row)
    event_time = timestamp(row.get("timestamp") or info.get("ts"))
    available = timestamp()
    observed = timestamp()
    common: dict[str, Any] = {
        "schemaVersion": "market-data-object-envelope.v1",
        "instrumentId": instrument_id(symbol),
        "relatedInstrumentId": None,
        "marketDataTargetId": market_data_target_id,
        "source": "OKX",
        "metadataHash": metadata_hash,
        "eventTime": event_time,
        "availableAt": available,
        "observedAt": observed,
    }
    if object_type == "mark":
        value = row.get("markPrice") or row.get("last") or info.get("markPx")
        if value in (None, ""):
            raise ValueError("Mark-price update lacks a mark price")
        return {
            **common,
            "objectType": object_type,
            "objectSchemaVersion": "market-data.mark.v1",
            "sequence": sequence,
            "payload": {"markPriceDecimal": canonical(decimal(value))},
        }
    if object_type == "index":
        value = row.get("indexPrice") or row.get("last") or info.get("idxPx")
        if value in (None, ""):
            raise ValueError("Index-price update lacks an index price")
        return {
            **common,
            "objectType": object_type,
            "objectSchemaVersion": "market-data.index.v1",
            "sequence": sequence,
            "payload": {"indexPriceDecimal": canonical(decimal(value))},
        }
    if object_type == "funding":
        rate = row.get("fundingRate") or info.get("fundingRate")
        next_time = row.get("fundingTimestamp") or info.get("fundingTime")
        if rate in (None, "") or next_time in (None, ""):
            raise ValueError("Funding update lacks rate or next funding time")
        return {
            **common,
            "objectType": object_type,
            "objectSchemaVersion": "market-data.funding.v1",
            "sequence": sequence,
            "payload": {
                "fundingRateDecimal": canonical(decimal(rate)),
                "nextFundingTime": timestamp(next_time),
            },
        }
    raise ValueError(f"Unsupported perpetual market-data object type: {object_type}")
