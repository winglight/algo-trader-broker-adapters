"""IB integration errors mapped to the public Broker SDK hierarchy."""

from algo_trader_broker_sdk import BrokerConnectionError, BrokerError, BrokerOrderError


class IBConnectionError(BrokerConnectionError):
    """Raised when the client cannot connect to the IB gateway."""


class IBOrderError(BrokerOrderError):
    """Raised for IB order submission failures."""


class IBMarketDataError(BrokerError):
    """Raised when IB market data subscriptions cannot be established."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="broker_market_data_error")
