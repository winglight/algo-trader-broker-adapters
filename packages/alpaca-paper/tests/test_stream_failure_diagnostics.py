"""Classify rejected source stages without inferring a lost provider reason."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import msgpack
import pytest

from algo_trader_broker_adapter_alpaca_paper.streams import create_stock_stream, source_stream_failure
from algo_trader_broker_sdk import BrokerError
from algo_trader_broker_sdk.errors import is_transport_failure


def native_stock():
    return create_stock_stream(SimpleNamespace(api_key_id="fixture", secret_key="fixture", data_feed="iex", request_timeout_seconds=1))


@pytest.mark.asyncio
@pytest.mark.parametrize("provider_code", [401, 406])
async def test_unmodified_sdk_discards_initial_error_code(monkeypatch, provider_code):
    # The installed SDK discards a rejected initial frame before auth. Both
    # examples therefore have the same observable exception; do not guess 406.
    raw = msgpack.packb([{"T": "error", "code": provider_code, "msg": "private-native-response"}])
    socket = SimpleNamespace(recv=AsyncMock(return_value=raw), close=AsyncMock())
    connect = AsyncMock(return_value=socket)
    monkeypatch.setattr("alpaca.data.live.websocket.websockets_legacy.connect", connect)
    from alpaca.data.live import StockDataStream
    stream = StockDataStream("fixture", "fixture")
    try:
        with pytest.raises(ValueError, match="connected message not received"):
            await stream._connect()
    finally:
        await stream.close()
    connect.assert_awaited_once()
    socket.close.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("method,phase", [("_send_subscribe_msg", "SUBSCRIBE"), ("_consume", "CONSUME")])
async def test_unknown_value_error_keeps_exact_phase_and_does_not_expose_message(method, phase):
    stream = native_stock()
    stream._start_ws = AsyncMock()
    stream._send_subscribe_msg = AsyncMock()
    stream._consume = AsyncMock()
    setattr(stream, method, AsyncMock(side_effect=ValueError("dynamic-order-id private-url body")))
    stream.close = AsyncMock()
    with pytest.raises(BrokerError) as caught:
        await stream._run_forever()
    error = caught.value
    assert error.details["reason"] == "VALUE_ERROR_UNCLASSIFIED"
    assert error.details["phase"] == phase
    assert f"phase={phase}" in str(error)
    assert "dynamic-order-id" not in str(error) and "private-url" not in str(error)
    assert error.code == "MARKET_DATA_STREAM_REJECTED" and not is_transport_failure(error)
    stream.close.assert_awaited_once()


@pytest.mark.parametrize("message,reason", [
    ("failed to authenticate", "AUTHENTICATED_ACK_MISSING"),
    ("auth failed", "AUTHENTICATION_FAILED"),
    ("handler must be a coroutine function", "CALLBACK_NOT_COROUTINE"),
])
def test_fixed_reason_is_visible_but_dynamic_phase_is_not(message, reason):
    error = source_stream_failure(ValueError(message), stream_kind="STOCK", feed="IEX", phase="private-user-input")
    assert error.details["reason"] == reason and f"reason={reason}" in str(error)
    assert "phase" not in error.details and "private-user-input" not in str(error)
