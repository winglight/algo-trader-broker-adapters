"""Backpressure and cleanup remain explicit failures, never fresh quote proof."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from algo_trader_broker_adapter_alpaca_paper import AlpacaPaperAdapter
from algo_trader_broker_adapter_alpaca_paper.streams import AlpacaStockSubscription, source_stream_failure
from algo_trader_broker_sdk import BrokerConnectionError, BrokerError
from algo_trader_broker_sdk.errors import is_transport_failure


def adapter_for(backend):
    adapter = AlpacaPaperAdapter(
        {"alpaca_api_key_id": "fixture", "alpaca_secret_key": "fixture", "alpaca_stream_queue_size": 32},
        backend=backend,
    )
    adapter.qualify_contract = AsyncMock(return_value={"symbol": "QQQ"})
    return adapter


@pytest.mark.parametrize("kind,feed,name", [
    ("STOCK", "IEX", "alpaca-paper.stock-data"),
    ("STOCK", "SIP", "alpaca-paper.stock-data"),
    ("OPTION", "OPRA", "alpaca-paper.option-opra"),
    ("OPTION", "INDICATIVE", "alpaca-paper.option-indicative"),
])
def test_source_failure_has_fixed_diagnostics_and_preserves_retry_class(kind, feed, name):
    failure = source_stream_failure(ValueError("connection limit exceeded secret-url-query"), stream_kind=kind, feed=feed)
    assert failure.code == "MARKET_DATA_CONNECTION_LIMIT"
    assert not is_transport_failure(failure)
    assert failure.details == dict(recovery="SOURCE_CONFIGURATION_CHANGED", stream_kind=kind, feed=feed, stream_name=name)
    assert f"feed={feed}" in str(failure) and "secret-url-query" not in str(failure)
    repeated = source_stream_failure(failure, stream_kind=kind, feed=feed)
    assert repeated is failure and str(failure).count("stream_name=") == 1
    transient = source_stream_failure(ConnectionResetError("secret-url-query"), stream_kind=kind, feed=feed)
    assert is_transport_failure(transient) and "secret-url-query" not in str(transient)


def test_source_diagnostics_reject_dynamic_labels():
    failure = source_stream_failure(ValueError("connection limit exceeded"), stream_kind="https://private", feed="account-id")
    assert failure.details == {"recovery": "SOURCE_CONFIGURATION_CHANGED"}
    assert "private" not in str(failure) and "account-id" not in str(failure)


@pytest.mark.asyncio
async def test_native_option_auth_failure_includes_feed_and_closes_once():
    from algo_trader_broker_adapter_alpaca_paper.options_stream import create_option_stream
    stream = create_option_stream(SimpleNamespace(api_key_id="fixture", secret_key="fixture", request_timeout_seconds=1, stream_queue_size=32), "OPRA")
    stream._start_ws = AsyncMock(side_effect=ValueError("connection limit exceeded"))
    stream.close = AsyncMock()
    with pytest.raises(BrokerError) as caught:
        await stream._run_forever()
    assert caught.value.details['stream_kind'] == 'OPTION'
    assert caught.value.details['feed'] == 'OPRA'
    assert caught.value.code == 'MARKET_DATA_CONNECTION_LIMIT'
    stream._start_ws.assert_awaited_once()
    stream.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_overflow_is_one_terminal_failure_and_releases_consumer(caplog):
    removed = AsyncMock()
    source = AlpacaStockSubscription(queue_size=2, name="fixture-quotes", close_callback=removed)
    for value in range(1000):
        source.deliver(value)

    assert sum(record.message == "Alpaca stream queue overflow" for record in caplog.records) == 1
    assert source._queue.qsize() == 1
    with pytest.raises(BrokerConnectionError, match="queue overflow"):
        await anext(source.__aiter__())
    removed.assert_awaited_once()
    assert source._closed
    source.deliver({"bid": 100})
    await source.close()
    removed.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["quotes", "trades"])
async def test_underlying_merge_preserves_native_rejection_and_closes_both_consumers(kind):
    removed = {key: AsyncMock() for key in ("quotes", "trades")}
    sources = {key: AlpacaStockSubscription(queue_size=2, name=key, close_callback=removed[key]) for key in removed}
    error = BrokerError("native rejection", code="MARKET_DATA_CONNECTION_LIMIT", details={"recovery": "SOURCE_CONFIGURATION_CHANGED"})
    sources[kind].deliver(error)
    backend = SimpleNamespace(stream_stock=AsyncMock(side_effect=lambda symbol, source_kind: sources[source_kind]))
    stream = await adapter_for(backend).stream_real_time_price({"symbol": "QQQ"})

    with pytest.raises(BrokerError) as caught:
        await asyncio.wait_for(anext(stream), 1)
    assert caught.value is error
    for key in removed:
        removed[key].assert_awaited_once()
        assert sources[key]._closed


class BurstSource:
    def __init__(self, kind):
        self.kind, self.closed = kind, False
        self.tasks = set()
        self.reads = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.tasks.add(asyncio.current_task())
        self.reads += 1
        if self.closed:
            raise StopAsyncIteration
        common = {"timestamp": "2026-10-26T14:00:00Z"}
        return dict(common, **({"bid_price": 100, "ask_price": 101} if self.kind == "quotes" else {"price": 100.5}))

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_cancel_with_full_merge_queue_joins_pumps_and_closes_consumers():
    sources = {key: BurstSource(key) for key in ("quotes", "trades")}
    backend = SimpleNamespace(stream_stock=AsyncMock(side_effect=lambda symbol, kind: sources[kind]))
    stream = await adapter_for(backend).stream_real_time_price({"symbol": "QQQ"})
    first = await asyncio.wait_for(anext(stream), 1)
    assert first.timestamp.isoformat() == "2026-10-26T14:00:00+00:00"
    # The consumer pauses while both pumps fill the bounded merge queue.
    await asyncio.sleep(0)
    assert sum(source.reads for source in sources.values()) >= 33
    await asyncio.wait_for(stream.aclose(), 1)
    assert all(source.closed for source in sources.values())
    tasks = set().union(*(source.tasks for source in sources.values()))
    assert len(tasks) == 2 and all(task.done() for task in tasks)


@pytest.mark.asyncio
async def test_second_subscription_failure_releases_first_consumer():
    removed = AsyncMock()
    quote = AlpacaStockSubscription(queue_size=2, name="quotes", close_callback=removed)
    error = BrokerError("native rejection", code="FEED_NOT_ENTITLED")
    backend = SimpleNamespace(stream_stock=AsyncMock(side_effect=[quote, error]))
    stream = await adapter_for(backend).stream_real_time_price({"symbol": "QQQ"})
    with pytest.raises(BrokerError) as caught:
        await anext(stream)
    assert caught.value is error
    removed.assert_awaited_once()
    assert quote._closed
