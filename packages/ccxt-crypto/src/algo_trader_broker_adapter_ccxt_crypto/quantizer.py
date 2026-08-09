"""Decimal-only OKX Spot metadata normalization and order quantization."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_UP, Decimal, InvalidOperation
from typing import Any

from algo_trader_broker_sdk import BrokerContractError, BrokerOrderError

_CLIENT_ID = re.compile(r"^[A-Za-z0-9]{1,32}$")


def decimal_value(value: Any, *, name: str, positive: bool = False) -> Decimal:
    try:
        result = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise BrokerOrderError(
            f"{name} must be a canonical decimal value",
            code="invalid_decimal",
            details={"field": name},
        ) from exc
    if not result.is_finite() or (positive and result <= 0):
        raise BrokerOrderError(
            f"{name} must be finite and greater than zero",
            code="invalid_decimal",
            details={"field": name},
        )
    return result


def canonical(value: Decimal | str | int) -> str:
    number = value if isinstance(value, Decimal) else Decimal(str(value))
    if number == 0:
        return "0"
    rendered = format(number.normalize(), "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def _step_round(value: Decimal, step: Decimal, *, rounding: str) -> Decimal:
    if step <= 0:
        raise BrokerContractError("OKX metadata step must be greater than zero")
    units = (value / step).to_integral_value(rounding=rounding)
    return units * step


def native_client_order_id(client_order_id: str) -> str:
    value = str(client_order_id or "").strip()
    if not value:
        raise BrokerOrderError(
            "clientOrderId is required before submission",
            code="client_order_id_required",
        )
    if _CLIENT_ID.fullmatch(value):
        return value
    return f"ati{hashlib.sha256(value.encode('utf-8')).hexdigest()[:29]}"


@dataclass(frozen=True, slots=True)
class MarketRules:
    symbol: str
    instrument_id: str
    native_instrument_id: str
    tick_size: Decimal
    lot_size: Decimal
    min_size: Decimal
    max_limit_size: Decimal | None
    max_market_size: Decimal | None
    minimum_notional: Decimal
    active: bool
    metadata_hash: str

    @classmethod
    def from_ccxt(
        cls,
        symbol: str,
        market: Mapping[str, Any],
        *,
        minimum_notional: Decimal,
    ) -> MarketRules:
        info = market.get("info") if isinstance(market.get("info"), Mapping) else {}
        native_id = str(market.get("id") or info.get("instId") or "").strip()
        expected_id = symbol.replace("/", "-")
        if native_id != expected_id:
            raise BrokerContractError(
                "OKX market native instrument identity mismatch",
                details={"symbol": symbol, "native_id": native_id},
            )
        if str(info.get("instType") or market.get("type") or "").upper() != "SPOT":
            raise BrokerContractError("OKX market must be SPOT", details={"symbol": symbol})

        def required(name: str, fallback: Any = None) -> Decimal:
            raw = info.get(name, fallback)
            try:
                value = Decimal(str(raw).strip())
            except (InvalidOperation, ValueError) as exc:
                raise BrokerContractError(
                    f"OKX market metadata {name} is missing or invalid",
                    details={"symbol": symbol, "field": name},
                ) from exc
            if value <= 0:
                raise BrokerContractError(
                    f"OKX market metadata {name} must be positive",
                    details={"symbol": symbol, "field": name},
                )
            return value

        def optional(name: str) -> Decimal | None:
            raw = info.get(name)
            if raw in (None, "", "0", 0):
                return None
            return required(name)

        precision = market.get("precision") if isinstance(market.get("precision"), Mapping) else {}
        tick_size = required("tickSz", precision.get("price"))
        lot_size = required("lotSz", precision.get("amount"))
        min_size = required("minSz", (market.get("limits") or {}).get("amount", {}).get("min"))
        state = str(info.get("state") or ("live" if market.get("active") else "")).lower()
        active = bool(market.get("active", True)) and state == "live"
        normalized = {
            "instId": native_id,
            "tickSz": canonical(tick_size),
            "lotSz": canonical(lot_size),
            "minSz": canonical(min_size),
            "maxLmtSz": canonical(optional("maxLmtSz")) if optional("maxLmtSz") else None,
            "maxMktSz": canonical(optional("maxMktSz")) if optional("maxMktSz") else None,
            "state": state,
            "listTime": str(info.get("listTime") or ""),
            "contTdSwTime": str(info.get("contTdSwTime") or ""),
            "minimumNotional": canonical(minimum_notional),
        }
        digest = hashlib.sha256(
            json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        venue = "OKX"
        base, quote = symbol.split("/", 1)
        return cls(
            symbol=symbol,
            instrument_id=f"crypto-spot:{base}-{quote}:{venue}",
            native_instrument_id=native_id,
            tick_size=tick_size,
            lot_size=lot_size,
            min_size=min_size,
            max_limit_size=optional("maxLmtSz"),
            max_market_size=optional("maxMktSz"),
            minimum_notional=minimum_notional,
            active=active,
            metadata_hash=digest,
        )

    def quantize_quantity(self, raw: Any, *, market: bool) -> Decimal:
        quantity = decimal_value(raw, name="quantityDecimal", positive=True)
        quantized = _step_round(quantity, self.lot_size, rounding=ROUND_DOWN)
        if quantized <= 0 or quantity - quantized >= self.lot_size:
            raise BrokerOrderError(
                "quantity cannot be represented by the current OKX lot size",
                code="quantity_precision_invalid",
            )
        if quantized < self.min_size:
            raise BrokerOrderError("quantity is below OKX minSz", code="quantity_below_minimum")
        maximum = self.max_market_size if market else self.max_limit_size
        if maximum is not None and quantized > maximum:
            raise BrokerOrderError("quantity exceeds OKX maximum", code="quantity_above_maximum")
        return quantized

    def quantize_price(self, raw: Any, *, side: str) -> Decimal:
        price = decimal_value(raw, name="limitPriceDecimal", positive=True)
        rounding = ROUND_DOWN if side.upper() == "BUY" else ROUND_UP
        quantized = _step_round(price, self.tick_size, rounding=rounding)
        if abs(quantized - price) >= self.tick_size:
            raise BrokerOrderError(
                "price cannot be represented by the current OKX tick size",
                code="price_precision_invalid",
            )
        return quantized

    def validate_notional(self, quantity: Decimal, price: Decimal) -> None:
        if quantity * price < self.minimum_notional:
            raise BrokerOrderError(
                "order notional is below the approved minimum",
                code="notional_below_minimum",
                details={"minimum_notional": canonical(self.minimum_notional)},
            )
