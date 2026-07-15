from __future__ import annotations

import pytest

from algo_trader_broker_adapter_alpaca_paper.clients import AlpacaClients
from algo_trader_broker_sdk import StockOrderRequest


alpaca = pytest.importorskip("alpaca", reason="official alpaca-py SDK is optional in source tests")


def test_pinned_sdk_accepts_supported_order_request_shapes() -> None:
    from alpaca.trading.requests import (
        LimitOrderRequest,
        MarketOrderRequest,
        StopLimitOrderRequest,
        StopOrderRequest,
    )

    cases = (
        ("MKT", None, None, MarketOrderRequest),
        ("LMT", 201.0, None, LimitOrderRequest),
        ("STP", None, 199.0, StopOrderRequest),
        ("STP LMT", 198.5, 199.0, StopLimitOrderRequest),
    )
    for order_type, limit_price, stop_price, expected_type in cases:
        request = StockOrderRequest(
            symbol="AAPL",
            side="BUY",
            quantity=2,
            order_type=order_type,
            tif="DAY",
            limit_price=limit_price,
            stop_price=stop_price,
            client_order_id=f"phase4-{order_type.replace(' ', '-').lower()}",
        )
        native = AlpacaClients.make_order_request(request)
        assert isinstance(native, expected_type)
        assert native.extended_hours is False
        assert native.qty == 2


def test_pinned_sdk_accepts_supported_timeframes() -> None:
    assert str(AlpacaClients.make_timeframe("1 min")) == "1Min"
    assert str(AlpacaClients.make_timeframe("1 hour")) == "1Hour"
    assert str(AlpacaClients.make_timeframe("1 day")) == "1Day"
    assert AlpacaClients.make_timeframe("2 mins") is None
