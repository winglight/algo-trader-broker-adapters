"""Stable Alpaca adapter errors."""

from algo_trader_broker_sdk import BrokerCapabilityError, BrokerOrderError


def unsupported(capability: str) -> BrokerCapabilityError:
    return BrokerCapabilityError(
        f"Alpaca Paper adapter does not support {capability}",
        code=f"alpaca_{capability}_not_supported",
        details={"adapter_id": "alpaca_paper", "capability": capability},
    )


def outcome_unknown(*, client_order_id: str) -> BrokerOrderError:
    return BrokerOrderError(
        "Alpaca order submission outcome is unknown; reconcile before retrying",
        code="broker_order_outcome_unknown",
        details={"adapter_id": "alpaca_paper", "client_order_id": client_order_id},
    )
