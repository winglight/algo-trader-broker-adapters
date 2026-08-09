"""Fail-closed error helpers for the OKX Demo adapter."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from algo_trader_broker_sdk import BrokerCapabilityError, BrokerOrderError


def unsupported(operation: str) -> BrokerCapabilityError:
    return BrokerCapabilityError(
        f"CCXT Crypto Paper does not support {operation}",
        details={"adapter_id": "ccxt_crypto", "operation": operation},
    )


def order_error(
    message: str,
    *,
    code: str,
    details: Mapping[str, Any] | None = None,
) -> BrokerOrderError:
    return BrokerOrderError(message, code=code, details=details)


def unknown_outcome(*, client_order_id: str) -> BrokerOrderError:
    return order_error(
        "OKX Demo order outcome is unknown; reconciliation is required",
        code="order_outcome_unknown",
        details={
            "client_order_id": client_order_id,
            "reconciliation_required": True,
            "retry_allowed": False,
        },
    )
