"""Fail-closed OKX linear swap metadata and Decimal quantization."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from typing import Any

from algo_trader_broker_sdk import BrokerContractError, BrokerOrderError


def canonical(value: Any) -> str:
    if isinstance(value, bool) or isinstance(value, float):
        raise BrokerContractError("Decimal values must not use binary float")
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise BrokerContractError("Invalid Decimal value") from exc
    if not parsed.is_finite():
        raise BrokerContractError("Decimal values must be finite")
    rendered = format(parsed, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return "0" if rendered in {"", "-0"} else rendered


def instrument_id(symbol: str) -> str:
    if symbol == "BTC/USDT:USDT":
        return "crypto-perpetual:BTC-USDT:USDT:OKX"
    if symbol == "ETH/USDT:USDT":
        return "crypto-perpetual:ETH-USDT:USDT:OKX"
    raise BrokerContractError("Perpetual symbol is not allowlisted")


@dataclass(frozen=True, slots=True)
class PerpetualMarketRules:
    symbol: str
    native_instrument_id: str
    contract_multiplier: Decimal
    tick_size: Decimal
    quantity_step: Decimal
    minimum_quantity: Decimal
    active: bool
    metadata_hash: str

    @classmethod
    def from_ccxt(cls, symbol: str, market: Mapping[str, Any]) -> "PerpetualMarketRules":
        info = market.get("info") if isinstance(market.get("info"), Mapping) else {}
        if not bool(market.get("swap")) or bool(market.get("future")):
            raise BrokerContractError("Market must be a perpetual swap")
        if not bool(market.get("linear")) or bool(market.get("inverse")):
            raise BrokerContractError("Market must be linear and not inverse")
        if str(market.get("settle") or info.get("settleCcy") or "").upper() != "USDT":
            raise BrokerContractError("Market settlement currency must be USDT")
        if str(info.get("ctType") or "linear").lower() != "linear":
            raise BrokerContractError("OKX ctType must be linear")
        native_id = str(market.get("id") or info.get("instId") or "").strip()
        if native_id not in {"BTC-USDT-SWAP", "ETH-USDT-SWAP"}:
            raise BrokerContractError("Unexpected OKX perpetual native instrument")
        try:
            multiplier = Decimal(str(market.get("contractSize") or info.get("ctVal")))
            tick = Decimal(str(info.get("tickSz")))
            step = Decimal(str(info.get("lotSz")))
            minimum = Decimal(str(info.get("minSz")))
        except (InvalidOperation, TypeError) as exc:
            raise BrokerContractError("Perpetual metadata is incomplete") from exc
        if min(multiplier, tick, step, minimum) <= 0:
            raise BrokerContractError("Perpetual metadata values must be positive")
        if step != step.to_integral_value() or minimum != minimum.to_integral_value():
            raise BrokerContractError("Phase 5 quantity unit must be integer contracts")
        payload = {
            "symbol": symbol,
            "nativeInstrumentId": native_id,
            "contractMultiplier": canonical(multiplier),
            "tickSize": canonical(tick),
            "quantityStep": canonical(step),
            "minimumQuantity": canonical(minimum),
            "active": bool(market.get("active", True)),
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return cls(
            symbol=symbol,
            native_instrument_id=native_id,
            contract_multiplier=multiplier,
            tick_size=tick,
            quantity_step=step,
            minimum_quantity=minimum,
            active=payload["active"],
            metadata_hash=digest,
        )

    def quantize_contracts(self, value: Any) -> Decimal:
        quantity = Decimal(canonical(value))
        if quantity < self.minimum_quantity:
            raise BrokerOrderError("Quantity is below minimum contracts", code="quantity_too_small")
        quantized = (quantity / self.quantity_step).to_integral_value(
            rounding=ROUND_DOWN
        ) * self.quantity_step
        if quantized != quantity:
            raise BrokerOrderError(
                "Quantity must align to integer contract step",
                code="quantity_step_mismatch",
            )
        return quantized

    def quantize_price(self, value: Any, *, side: str) -> Decimal:
        price = Decimal(canonical(value))
        if price <= 0:
            raise BrokerOrderError("Price must be positive", code="invalid_price")
        rounding = ROUND_DOWN if side == "BUY" else ROUND_UP
        return (price / self.tick_size).to_integral_value(rounding=rounding) * self.tick_size
