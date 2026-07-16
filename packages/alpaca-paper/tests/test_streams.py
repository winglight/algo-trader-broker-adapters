from __future__ import annotations

import asyncio
import threading

import pytest

from algo_trader_broker_adapter_alpaca_paper.streams import MultiplexedAlpacaStockStream


class FakeStockDataStream:
    def __init__(self) -> None:
        self.handlers = {"bars": {}, "trades": {}, "quotes": {}}
        self.subscribe_calls: list[tuple[str, str]] = []
        self.unsubscribe_calls: list[tuple[str, str]] = []
        self.started = threading.Event()
        self.stopped = threading.Event()

    def _subscribe(self, kind, handler, symbol):
        self.handlers[kind][symbol] = handler
        self.subscribe_calls.append((kind, symbol))

    def subscribe_bars(self, handler, symbol):
        self._subscribe("bars", handler, symbol)

    def subscribe_trades(self, handler, symbol):
        self._subscribe("trades", handler, symbol)

    def subscribe_quotes(self, handler, symbol):
        self._subscribe("quotes", handler, symbol)

    def _unsubscribe(self, kind, symbol):
        self.handlers[kind].pop(symbol)
        self.unsubscribe_calls.append((kind, symbol))

    def unsubscribe_bars(self, symbol):
        self._unsubscribe("bars", symbol)

    def unsubscribe_trades(self, symbol):
        self._unsubscribe("trades", symbol)

    def unsubscribe_quotes(self, symbol):
        self._unsubscribe("quotes", symbol)

    def run(self):
        self.started.set()
        self.stopped.wait(2)

    def stop(self):
        self.stopped.set()


@pytest.mark.asyncio
async def test_stock_stream_multiplexes_consumers_over_one_connection():
    raw = FakeStockDataStream()
    managed = MultiplexedAlpacaStockStream(raw, queue_size=10, name="stock-data")

    first = await managed.subscribe("aapl", "quotes")
    second = await managed.subscribe("AAPL", "quotes")
    trades = await managed.subscribe("AAPL", "trades")
    await asyncio.to_thread(raw.started.wait, 1)

    assert raw.subscribe_calls == [("quotes", "AAPL"), ("trades", "AAPL")]
    assert managed._thread is not None

    await raw.handlers["quotes"]["AAPL"]({"bid": 1})
    await raw.handlers["trades"]["AAPL"]({"price": 2})
    first_iter = first.__aiter__()
    second_iter = second.__aiter__()
    trades_iter = trades.__aiter__()
    assert await anext(first_iter) == {"bid": 1}
    assert await anext(second_iter) == {"bid": 1}
    assert await anext(trades_iter) == {"price": 2}

    await first.close()
    assert raw.unsubscribe_calls == []
    await second.close()
    assert raw.unsubscribe_calls == [("quotes", "AAPL")]
    await trades.close()
    assert raw.unsubscribe_calls[-1] == ("trades", "AAPL")

    await managed.close()
    assert raw.stopped.is_set()
