"""Synthetic native frames: no network, account credentials, or orders."""

import asyncio
from dataclasses import replace
from decimal import Decimal
from importlib.metadata import version
import json

import pytest

from algo_trader_broker_sdk import BrokerConnectionError, BrokerContractError
from algo_trader_broker_sdk.options import OptionScope, OptionVerifiedAccount
from algo_trader_broker_adapter_alpaca_paper.raw_stream import CapturedTradeFrame, create_trading_stream, decode_native
from algo_trader_broker_adapter_alpaca_paper.settings import AlpacaPaperSettings
from .test_adapter import FakeBackend, SETTINGS, connected


SCOPE = OptionScope("options/1.0", "a" * 64, "profile-a", 1, "stable-account", "paper")
VERIFIED = OptionVerifiedAccount(SCOPE, "ALPACA", "paper-account")
# Deliberately a JSON number: preservation must happen before SDK float casting.
RAW = b'{"stream":"trade_updates","data":{"event":"fill","execution_id":"prefix::full-native-id","price":1.123456789012,"qty":"1","order":{"id":"native-order","asset_class":"us_option","symbol":"SPY261009C00600000"}}}'


class Backend(FakeBackend):
    def set_raw_trade_handler(self, handler):
        self.raw_handler = handler


@pytest.mark.asyncio
async def test_raw_option_bytes_full_id_and_scope_survive_queued_delivery_after_rebind():
    adapter, backend = await connected(Backend())
    retained, stocks = [], []
    async def save(event): retained.append(event)
    async def stock(event): stocks.append(event)
    adapter.set_trade_update_handler(stock)
    adapter.set_option_event_handler(VERIFIED, save)
    queued = CapturedTradeFrame(RAW, backend.raw_handler)
    changed = replace(VERIFIED, scope=replace(SCOPE, expected_generation=2, profile_id="profile-b"))
    adapter.set_option_event_handler(changed, save)
    await backend.trade_handler(queued)
    await backend.trade_handler(CapturedTradeFrame(RAW, backend.raw_handler))
    assert [event.scope for event in retained] == [SCOPE, changed.scope]
    assert all(event.raw_payload == RAW and event.native_event_id == "prefix::full-native-id" for event in retained)
    assert decode_native(retained[0].raw_payload)["data"]["price"] == Decimal("1.123456789012")
    assert stocks == []
    adapter.set_option_event_handler(VERIFIED, None)  # a late detach cannot clear its replacement
    assert backend.raw_handler is not None
    await adapter.close()
    assert backend.raw_handler is None


@pytest.mark.asyncio
async def test_parent_summary_and_unclassified_frames_are_raw_only_and_sink_failure_is_not_acknowledged():
    adapter, backend = await connected(Backend())
    retained, stocks = [], []
    async def save(event): retained.append(event)
    async def stock(event): stocks.append(event)
    adapter.set_option_event_handler(VERIFIED, save)
    adapter.set_trade_update_handler(stock)
    for order in ({"asset_class": "", "order_class": "mleg", "filled_avg_price": "1.62"},
                  {"asset_class": "us_equity", "order_class": "mleg"}, {},
                  {"asset_class": "us_equity", "legs": [{"asset_class": "us_option"}]}):
        raw = json.dumps({"stream": "trade_updates", "data": {"event": "fill", "order": order}}).encode()
        await backend.trade_handler(CapturedTradeFrame(raw, backend.raw_handler))
    assert len(retained) == 4 and not stocks
    async def fail(event): raise RuntimeError("fixture storage failure")
    adapter.set_option_event_handler(VERIFIED, fail)
    with pytest.raises(RuntimeError, match="storage failure"):
        await backend.trade_handler(CapturedTradeFrame(RAW, backend.raw_handler))
    with pytest.raises(BrokerConnectionError, match="original native frame"):
        await backend.trade_handler(decode_native(RAW)["data"])


@pytest.mark.asyncio
async def test_malformed_frames_are_retained_before_failure_and_missing_sink_never_falls_back_to_stock():
    adapter, backend = await connected(Backend())
    saved = []
    async def save(event): saved.append(event)
    adapter.set_option_event_handler(VERIFIED, save)
    for raw in (b'{"data":', b'{"data":{},"data":{}}', b'{"data":{"price":NaN}}', b'[]'):
        with pytest.raises(BrokerConnectionError, match="malformed"):
            await backend.trade_handler(CapturedTradeFrame(raw, backend.raw_handler))
        assert saved[-1].raw_payload == raw
    with pytest.raises(BrokerConnectionError, match="no verified"):
        await backend.trade_handler(CapturedTradeFrame(RAW, None))


@pytest.mark.asyncio
async def test_equity_frames_continue_through_existing_callback_without_entering_option_journal():
    adapter, backend = await connected(Backend())
    stocks = []
    async def stock(event): stocks.append(event)
    async def unexpected(raw): raise AssertionError("Stock entered option journal")
    adapter.set_trade_update_handler(stock)
    raw = json.dumps({"stream": "trade_updates", "data": {"event": "new", "order": {
        **backend.order, "asset_class": "us_equity"}}}).encode()
    await backend.trade_handler(CapturedTradeFrame(raw, unexpected))
    assert len(stocks) == 1 and stocks[0].adapter_order_id == "order-uuid"


@pytest.mark.asyncio
async def test_wrong_native_account_and_live_binding_cannot_install_event_sink():
    adapter, backend = await connected(Backend())
    async def save(event): pass
    for wrong in (replace(VERIFIED, native_account_ref="another"), replace(VERIFIED, broker="IBKR"),
                  replace(VERIFIED, scope=replace(SCOPE, environment="live"))):
        with pytest.raises(BrokerContractError, match="does not match"):
            adapter.set_option_event_handler(wrong, save)
    adapter.set_option_event_handler(VERIFIED, save)
    await adapter.disconnect()
    assert backend.raw_handler is None
    with pytest.raises(BrokerContractError, match="does not match"):
        adapter.set_option_event_handler(VERIFIED, save)


@pytest.mark.asyncio
async def test_legacy_reconciliation_excludes_options_parents_children_and_unresolved_fills():
    adapter, backend = await connected(Backend())
    stock = {**backend.order, "asset_class": "us_equity"}
    option = {**backend.order, "id": "option", "asset_class": "us_option"}
    parent = {**backend.order, "id": "combo", "asset_class": "", "order_class": "mleg"}
    unknown = {**stock, "id": "unknown", "asset_class": None}
    async def orders(**kwargs): return [stock, option, parent, unknown]
    async def fills(since):
        return [dict(id="fill-" + oid, order_id=oid, symbol="UNPROVEN", qty="1", price="2",
                     transaction_time="2026-10-05T10:00:00Z") for oid in ("order-uuid", "option", "combo", "unknown", "missing")]
    backend.get_orders, backend.get_fill_activities = orders, fills
    assert [event.adapter_order_id for event in await adapter.request_open_orders()] == ["order-uuid"]
    assert [event.adapter_order_id for event in await adapter.request_completed_orders()] == ["order-uuid"]
    for events in (await adapter.request_executions(), await adapter.request_executions_unchecked()):
        assert [event.adapter_order_id for event in events] == ["order-uuid"]


class Socket:
    def __init__(self, values): self.values, self.closed = iter(values), False
    async def recv(self):
        value = next(self.values)
        if isinstance(value, Exception): raise value
        return value
    async def close(self): self.closed = True


@pytest.mark.asyncio
async def test_pinned_sdk_receive_loop_keeps_original_bytes_and_surfaces_disconnect_without_hidden_reconnect():
    pytest.importorskip("alpaca")
    assert version("alpaca-py") == "0.43.5"
    marker = object()
    stream = create_trading_stream(AlpacaPaperSettings.from_mapping(SETTINGS), lambda: marker)
    socket = Socket([RAW, ConnectionError("fixture disconnect")])
    starts, received = [], []
    async def start():
        starts.append(1)
        stream._ws = socket
    async def receive(frame): received.append(frame)
    stream._start_ws = start
    stream.subscribe_trade_updates(receive)
    with pytest.raises(ConnectionError, match="fixture disconnect"):
        await stream._run_forever()
    assert len(starts) == 1 and socket.closed
    assert len(received) == 1 and received[0].raw == RAW and received[0].sink is marker
    assert stream._endpoint.value == "wss://paper-api.alpaca.markets/stream"


@pytest.mark.asyncio
async def test_native_receive_loop_preserves_malformed_bytes_but_excludes_authorization_frames():
    pytest.importorskip("alpaca")
    stream = create_trading_stream(AlpacaPaperSettings.from_mapping(SETTINGS), lambda: None)
    seen = []
    async def receive(frame): seen.append(frame.raw)
    stream.subscribe_trade_updates(receive)
    for raw in (b'{"data":', b'[]'):
        stream._ws = Socket([raw])
        with pytest.raises(BrokerConnectionError): await stream._consume()
        assert seen[-1] == raw
    before = list(seen)
    stream._ws = Socket([b'{"stream":"authorization","data":{"status":"unauthorized"}}'])
    with pytest.raises(BrokerConnectionError): await stream._consume()
    assert seen == before


@pytest.mark.asyncio
async def test_threaded_stream_delivers_raw_then_reports_native_connection_failure_to_owner_loop():
    pytest.importorskip("alpaca")
    from algo_trader_broker_adapter_alpaca_paper.streams import ThreadedAlpacaStream, TradeUpdateStream
    owner_loop = asyncio.get_running_loop()
    adapter, backend = await connected(Backend())
    saved, failures = [], []
    failed = asyncio.Event()
    async def save(event):
        assert asyncio.get_running_loop() is owner_loop
        saved.append(event)
    async def failure(exc):
        failures.append(exc)
        failed.set()
    adapter.set_option_event_handler(VERIFIED, save)
    native = create_trading_stream(adapter._settings, lambda: backend.raw_handler)
    socket = Socket([RAW, ConnectionError("synthetic socket failure")])
    async def start(): native._ws = socket
    native._start_ws = start
    threaded = ThreadedAlpacaStream(native, native.subscribe_trade_updates, queue_size=10, name="fixture-exact")
    managed = TradeUpdateStream(threaded, backend.trade_handler, failure)
    try:
        await managed.start()
        await asyncio.wait_for(failed.wait(), 2)
        assert len(saved) == 1 and saved[0].raw_payload == RAW
        assert len(failures) == 1 and isinstance(failures[0], BrokerConnectionError)
    finally:
        await managed.close()


@pytest.mark.asyncio
async def test_durable_sink_failure_reaches_stream_failure_handler():
    from algo_trader_broker_adapter_alpaca_paper.streams import TradeUpdateStream
    adapter, backend = await connected(Backend())
    async def save(event): raise RuntimeError("synthetic durable storage failure")
    adapter.set_option_event_handler(VERIFIED, save)
    class Threaded:
        def __aiter__(self): return self.iterate()
        async def iterate(self): yield CapturedTradeFrame(RAW, backend.raw_handler)
    seen = []
    async def failure(exc): seen.append(exc)
    await TradeUpdateStream(Threaded(), backend.trade_handler, failure)._consume()
    assert len(seen) == 1 and str(seen[0]) == "synthetic durable storage failure"
