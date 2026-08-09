"""Reviewed execution-cost policy for the first OKX Demo rollout."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .quantizer import canonical


@dataclass(frozen=True, slots=True)
class CryptoSpotFeePolicy:
    policy_id: str = "crypto_spot_okx_demo_v1"
    maker_rate: Decimal = Decimal("0.001")
    taker_rate: Decimal = Decimal("0.0015")

    def estimate(self, *, quantity: Any, price: Any, liquidity_role: str) -> Decimal:
        role = str(liquidity_role).strip().upper()
        rate = self.maker_rate if role == "MAKER" else self.taker_rate
        return Decimal(str(quantity)) * Decimal(str(price)) * rate

    def as_dict(self) -> dict[str, str]:
        return {
            "policyId": self.policy_id,
            "makerRate": canonical(self.maker_rate),
            "takerRate": canonical(self.taker_rate),
            "feeCurrency": "USDT",
        }


def reported_fee_tier(payload: Mapping[str, Any]) -> dict[str, str | None]:
    maker = payload.get("maker")
    taker = payload.get("taker")
    return {
        "makerRate": canonical(Decimal(str(maker))) if maker not in (None, "") else None,
        "takerRate": canonical(Decimal(str(taker))) if taker not in (None, "") else None,
    }
