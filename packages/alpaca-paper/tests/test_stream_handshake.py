"""Offline compatibility checks for the pinned SDK's private handshake seam."""

from importlib.metadata import version
from types import SimpleNamespace
from unittest.mock import AsyncMock

import msgpack
import pytest

from algo_trader_broker_adapter_alpaca_paper.streams import create_stock_stream
from algo_trader_broker_adapter_alpaca_paper.options_stream import create_option_stream
from algo_trader_broker_sdk import BrokerError
from algo_trader_broker_sdk.errors import is_transport_failure


def native(kind):
    settings = SimpleNamespace(api_key_id="fixture-key", secret_key="fixture-secret", data_feed="iex",
        request_timeout_seconds=1, stream_queue_size=32)
    return create_stock_stream(settings) if kind == "STOCK" else create_option_stream(settings, "INDICATIVE")


def frame(value):
    return msgpack.packb(value)


def mock_socket(monkeypatch, messages):
    socket = SimpleNamespace(recv=AsyncMock(side_effect=[frame(message) for message in messages]),
        send=AsyncMock(), close=AsyncMock())
    connector = AsyncMock(return_value=socket)
    monkeypatch.setattr("alpaca.data.live.websocket.websockets_legacy.connect", connector)
    return socket, connector


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["STOCK", "OPTION"])
@pytest.mark.parametrize("phase", ["CONNECT", "AUTH"])
@pytest.mark.parametrize("native_code,code,reason", [
    (401, "MARKET_DATA_STREAM_REJECTED", "NOT_AUTHENTICATED"),
    (406, "MARKET_DATA_CONNECTION_LIMIT", "CONNECTION_LIMIT"),
    (409, "FEED_NOT_ENTITLED", "INSUFFICIENT_SUBSCRIPTION"),
])
async def test_known_numeric_control_error_survives_without_native_body(monkeypatch, kind, phase, native_code, code, reason):
    messages = ([[{"T": "success", "msg": "connected"}]] if phase == "AUTH" else [])
    messages.append([{"T": "error", "code": native_code, "msg": "private-response-body", "private_id": "must-not-retain"}])
    socket, connector = mock_socket(monkeypatch, messages)
    stream = native(kind)
    stream._send_subscribe_msg = AsyncMock()
    with pytest.raises(BrokerError) as caught:
        await stream._run_forever()
    error = caught.value
    assert error.code == code
    assert error.details["native_code"] == native_code and error.details["reason"] == reason
    assert error.details["phase"] == phase and error.details["stream_kind"] == kind
    assert f"native_code={native_code}" in str(error)
    assert not is_transport_failure(error)
    assert "private-response-body" not in repr(error.to_payload()) and "must-not-retain" not in repr(error.to_payload())
    connector.assert_awaited_once()
    assert socket.recv.await_count == (2 if phase == "AUTH" else 1)
    assert socket.send.await_count == (1 if phase == "AUTH" else 0)
    stream._send_subscribe_msg.assert_not_awaited()
    socket.close.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["STOCK", "OPTION"])
async def test_success_exchange_matches_pinned_official_sdk_exactly(monkeypatch, kind):
    from alpaca.data.enums import DataFeed, OptionsFeed
    from alpaca.data.live import StockDataStream
    from alpaca.data.live.option import OptionDataStream

    assert version("alpaca-py") == "0.43.5", "Review the private handshake seam when upgrading alpaca-py"
    custom = native(kind)
    official = (StockDataStream("fixture-key", "fixture-secret", feed=DataFeed.IEX) if kind == "STOCK" else
        OptionDataStream("fixture-key", "fixture-secret", raw_data=True, feed=OptionsFeed.INDICATIVE,
            websocket_params=custom._websocket_params))
    observations = []
    for stream in (official, custom):
        socket, connector = mock_socket(monkeypatch, [
            [{"T": "success", "msg": "connected"}], [{"T": "success", "msg": "authenticated"}],
        ])
        await stream._start_ws()
        observations.append((connector.await_args, socket.send.await_args_list, socket.recv.await_count))
        await stream.close()
        connector.assert_awaited_once()
        socket.close.assert_awaited_once()
    assert observations[0] == observations[1]
    assert observations[1][2] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [
    [{"T": "subscription", "msg": "private-invalid-ack"}],
    [{"T": "success", "msg": "authenticated"}],
    [], {},
])
async def test_non_error_bad_connect_frame_never_becomes_authenticated(monkeypatch, bad):
    socket, connector = mock_socket(monkeypatch, [bad])
    stream = native("STOCK")
    with pytest.raises(BrokerError) as caught:
        await stream._run_forever()
    assert caught.value.code != "MARKET_DATA_CONNECTION_LIMIT"
    assert "private-invalid-ack" not in repr(caught.value.to_payload())
    assert "native_code" not in caught.value.details
    socket.send.assert_not_awaited()
    connector.assert_awaited_once()
    socket.close.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("unknown_code", [123456789, "406", True, None])
async def test_unknown_or_noninteger_native_code_is_not_retained_or_guessed(monkeypatch, unknown_code):
    socket, connector = mock_socket(monkeypatch, [[{"T": "error", "code": unknown_code,
        "msg": "connection limit exceeded private-text"}]])
    with pytest.raises(BrokerError) as caught:
        await native("STOCK")._run_forever()
    assert caught.value.code == "MARKET_DATA_STREAM_REJECTED"
    assert caught.value.details["reason"] == "NATIVE_ERROR_UNCLASSIFIED"
    assert "native_code" not in caught.value.details
    assert "private-text" not in repr(caught.value.to_payload())
    connector.assert_awaited_once()
    socket.close.assert_awaited_once()
