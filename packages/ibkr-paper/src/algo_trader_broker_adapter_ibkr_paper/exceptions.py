"""IB integration errors mapped to the public Broker SDK hierarchy."""

from typing import Any, Mapping
import re

from algo_trader_broker_sdk import BrokerConnectionError, BrokerError, BrokerOrderError


class IBConnectionError(BrokerConnectionError):
    """Raised when the client cannot connect to the IB gateway."""


class IBOrderError(BrokerOrderError):
    """Raised for IB order submission failures."""


class IBMarketDataError(BrokerError):
    """Raised when IB market data subscriptions cannot be established."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "broker_market_data_error",
        details: Mapping[str, Any] | None = None,
    ) -> None:
        projected = dict(details or {})
        native = re.search(r'(?:error\s+|code[=: ]+)(\d+)', message, re.IGNORECASE)
        native_code = int(native.group(1)) if native else None
        if native_code is not None:
            projected.setdefault('nativeCode', native_code)
        # Native data resets are transport events. Permissions, competing
        # sessions and qualification rejections must await actual inputs.
        reconnect = native_code in {317, 366, 1100, 1101, 1102, 10182}
        projected.setdefault('recovery', 'RECONNECT_STREAM' if reconnect else 'WAIT_SOURCE')
        super().__init__(message, code=code, details=projected)


class IBScreenerError(IBMarketDataError):
    """Stable adapter-neutral projection of a native IB Screener failure."""
