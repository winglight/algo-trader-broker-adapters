"""Exact native trading frames on the existing alpaca-py connection.

alpaca-py 0.43.5 parses JSON before its raw_data callback and silently
reconnects inside _run_forever. Override just that receive loop: retain bytes,
use Decimal for routing, and let the adapter's existing recovery fence every
disconnect. Authentication/subscription/transport remain the official SDK's.
"""

import asyncio
from dataclasses import dataclass, field
from decimal import Decimal
import json
from typing import Awaitable, Callable

from algo_trader_broker_sdk import BrokerConnectionError


RawHandler = Callable[[bytes], Awaitable[None]]


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate native JSON key")
        result[key] = value
    return result


def _constant(_value):
    raise ValueError("Nonfinite native JSON number")


def decode_native(raw):
    if type(raw) is not bytes or not 0 < len(raw) <= 8 * 1024 * 1024:
        raise ValueError("Invalid native frame size")
    return json.loads(raw.decode("utf-8"), parse_float=Decimal,
                      object_pairs_hook=_object, parse_constant=_constant)


@dataclass(frozen=True, slots=True)
class CapturedTradeFrame:
    raw: bytes = field(repr=False)
    # Snapshot the scoped sink before leaving the native receive loop. Never
    # read the current handler after the frame has waited in a delivery queue.
    sink: RawHandler | None = field(repr=False)


def create_trading_stream(settings, capture_sink):
    from alpaca.trading.stream import TradingStream

    class ExactTradingStream(TradingStream):
        async def _consume(self):
            while self._should_run:
                if not self._stop_stream_queue.empty():
                    self._stop_stream_queue.get_nowait()
                    return
                try:
                    native = await asyncio.wait_for(self._ws.recv(), 5)
                except asyncio.TimeoutError:
                    continue
                raw = native.encode("utf-8") if type(native) is str else native
                sink = capture_sink()
                try:
                    message = decode_native(raw)
                except (ValueError, UnicodeError, RecursionError):
                    # Preserve bounded malformed frames as unresolved evidence
                    # before the adapter reports a stream gap. Never log bytes.
                    if type(raw) is bytes and 0 < len(raw) <= 8 * 1024 * 1024:
                        await self._trade_updates_handler(CapturedTradeFrame(raw, sink))
                    raise BrokerConnectionError("Alpaca trading frame is invalid") from None
                if not isinstance(message, dict):
                    await self._trade_updates_handler(CapturedTradeFrame(raw, sink))
                    raise BrokerConnectionError("Alpaca trading frame is not an object")
                if message.get("stream") == "trade_updates":
                    await self._trade_updates_handler(CapturedTradeFrame(raw, sink))
                elif message.get("action") == "error" or message.get("stream") == "authorization":
                    raise BrokerConnectionError("Alpaca trading stream requires reconnection")

        async def _run_forever(self):
            self._loop = asyncio.get_running_loop()
            if self._trade_updates_handler is None or not self._should_run or not self._stop_stream_queue.empty():
                return
            try:
                await self._start_ws()
                self._running = True
                await self._consume()
            finally:
                await self.close()

    return ExactTradingStream(settings.api_key_id, settings.secret_key, paper=True, raw_data=True)
