"""Option MsgPack acquisition on one shared official SDK stream per feed."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import math

from algo_trader_broker_sdk import BrokerConnectionError
from algo_trader_broker_sdk.options import OptionQuote, QuoteSubscription, check, timestamp

from .options_codec import _decimal, _time
from .options_reads import whole, now_wire
from .streams import MultiplexedAlpacaStockStream


@dataclass(frozen=True, slots=True)
class NativeOptionQuote:
    raw: bytes
    fields: dict
    received_at: str


def create_option_stream(settings, feed):
    import msgpack
    from alpaca.data.enums import OptionsFeed
    from alpaca.data.live.option import OptionDataStream

    class ExactOptionStream(OptionDataStream):
        async def _consume(self):
            while self._should_run:
                if not self._stop_stream_queue.empty():
                    return
                try:
                    raw = await asyncio.wait_for(self._ws.recv(), 1)
                except asyncio.TimeoutError:
                    continue
                check(type(raw) is bytes and 0 < len(raw) <= 8 * 1024 * 1024, "Invalid option MsgPack frame")
                messages = msgpack.unpackb(raw, raw=False)
                check(type(messages) is list, "Invalid native option message array")
                received = now_wire()
                for message in messages:
                    check(type(message) is dict, "Invalid native option message")
                    if message.get("T") == "error":
                        raise BrokerConnectionError("Option quote stream requires a new subscription", code="OPTION_STREAM_GAP")
                    if message.get("T") == "q":
                        handler = self._handlers["quotes"].get(message.get("S"))
                        if handler is not None:
                            await handler(NativeOptionQuote(raw, message, received))

        async def _run_forever(self):
            # Authentication and subscription stay with alpaca-py. A native
            # disconnect ends this stream; no hidden reconnect fills its gap.
            self._loop = asyncio.get_running_loop()
            try:
                if not self._should_run: return
                async with asyncio.timeout(settings.request_timeout_seconds):
                    await self._start_ws()
                    await self._send_subscribe_msg()
                self._running = True
                await self._consume()
            finally:
                await self.close()

        def stop(self):
            self._should_run = False
            if self._loop is not None and self._loop.is_running():
                super().stop()

    return ExactOptionStream(settings.api_key_id, settings.secret_key, raw_data=True,
        feed=OptionsFeed.OPRA if feed == "OPRA" else OptionsFeed.INDICATIVE,
        websocket_params={"max_size": 8 * 1024 * 1024, "max_queue": settings.stream_queue_size,
                          "ping_interval": 10, "ping_timeout": 30, "close_timeout": 5})


class SharedOptionStream(MultiplexedAlpacaStockStream):
    """Reuse existing symbol refcounts/queues, but expose option stream gaps."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failed = False

    def _notify_closed(self, failure):
        if not self._closed:
            self.failed = True
            failure = failure or BrokerConnectionError("Option quote connection ended", code="OPTION_STREAM_GAP")
        super()._notify_closed(failure)

    @property
    def unused(self):
        with self._lock:
            return not self._subscribers


async def merge_quotes(subscriptions, queue_size):
    queue = asyncio.Queue(maxsize=queue_size)
    async def forward(subscription):
        try:
            async for quote in subscription:
                await queue.put(quote)
            await queue.put(BrokerConnectionError("Option quote subscription ended", code="OPTION_STREAM_GAP"))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await queue.put(exc)
    tasks = [asyncio.create_task(forward(item)) for item in subscriptions]
    try:
        while True:
            item = await queue.get()
            if isinstance(item, Exception): raise item
            yield item
    finally:
        for task in tasks: task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for item in subscriptions: await item.close()


def normalize_quote(native, binding, request):
    import msgpack
    fields = native.fields
    at = fields.get("t")
    if isinstance(at, msgpack.Timestamp):
        # Preserve nanoseconds in raw bytes and floor only the DTO timestamp.
        at = datetime.fromtimestamp(at.seconds, timezone.utc).replace(microsecond=at.nanoseconds // 1000).isoformat().replace("+00:00", "Z")
    else:
        at = _time(at)
    def price(value):
        if type(value) is float:
            check(math.isfinite(value), "Invalid native MsgPack option price")
            # MsgPack's native price is IEEE-754, unlike REST decimal JSON.
            # Its shortest round-trip decimal is used only at this boundary.
            value = Decimal(repr(value))
        return _decimal(value)
    bid, ask = price(fields["bp"]), price(fields["ap"])
    age = (datetime.now(timezone.utc) - timestamp(at)).total_seconds() * 1000
    quality = "EXECUTABLE"
    if Decimal(bid) > Decimal(ask): quality = "CROSSED"
    elif age < 0 or age > request.max_age_ms: quality = "STALE"
    elif request.feed == "INDICATIVE": quality = "RESEARCH_ONLY"
    return OptionQuote(binding.canonical_id, bid, ask, whole(fields["bs"]), whole(fields["as"]), at,
        native.received_at, "ALPACA", request.feed, quality, request.account,
        None, None, None, None, None, None, None, None, None)


async def stream_quotes(adapter, request):
    check(type(request) is QuoteSubscription, "Expected an exact quote subscription")
    check(request.feed in {"OPRA", "INDICATIVE"}, "Options require an explicit quote feed")
    bound, _, _ = await adapter._option_bound(request)
    adapter._validate_option_bindings(bound, request.bindings)
    bindings = {binding.local_symbol: binding for binding in request.bindings}
    source = adapter._backend.stream_option_quotes(tuple(bindings), request.feed)
    try:
        async for native in source:
            adapter._option_still_bound(bound)
            check(type(native) is NativeOptionQuote and native.fields.get("S") in bindings, "Unrequested native option quote")
            adapter._remember_option_read(native.raw)
            yield normalize_quote(native, bindings[native.fields["S"]], request)
    finally:
        await source.aclose()
