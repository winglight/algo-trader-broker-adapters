"""Request-owned real-time prices; native entitlement errors never change feeds."""
from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from ib_async import Ticker

from .exceptions import IBMarketDataError


LOGGER = logging.getLogger(__name__)


class PriceStream:
    """Own one native reqId, including close before the first iteration."""

    def __init__(self, ib, contract, *, snapshot, queue_size, serialize, competing_error):
        self.ib, self.contract = ib, contract
        self.req_id = ib.client.getReqId()
        self.ticker = Ticker(contract=contract, defaults=ib.wrapper.defaults)
        self.queue = asyncio.Queue(queue_size)
        self.failure = asyncio.get_running_loop().create_future()
        self.closed = asyncio.get_running_loop().create_future()
        self.serialize, self.competing_error = serialize, competing_error
        self.ticker.updateEvent += self._update
        ib.errorEvent += self._error
        ib.wrapper.reqId2Ticker[self.req_id] = self.ticker
        ib.wrapper._reqId2Contract[self.req_id] = contract
        try:
            ib.client.reqMktData(self.req_id, contract, "", snapshot, False, [])
        except BaseException:
            self._cleanup()
            raise

    def _update(self, ticker):
        if self.closed.done() or self.failure.done():
            return
        if self.queue.full():
            self.queue.get_nowait()
        self.queue.put_nowait(self.serialize(ticker))

    def _error(self, req_id, code, message, _contract):
        if req_id != self.req_id or self.closed.done() or self.failure.done():
            return
        if code != 354 and not self.competing_error(code, message):
            return
        self.failure.set_result((code, str(message)))

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.closed.done():
            raise StopAsyncIteration
        read = asyncio.create_task(self.queue.get())
        try:
            await asyncio.wait({read, self.failure, self.closed},
                               return_when=asyncio.FIRST_COMPLETED)
            if self.closed.done():
                raise StopAsyncIteration
            # A rejection wins over a buffered tick from the same socket batch.
            if self.failure.done():
                code, message = self.failure.result()
                await self.aclose()
                raise IBMarketDataError(
                    f"Real-time market data request rejected by IB (code {code}): {message}",
                    code=("broker_market_data_not_subscribed" if code == 354
                          else "screener_market_data_competing_session"),
                    details={"nativeCode": code, "operation": "realtime_price",
                             "recovery": "WAIT_SOURCE"},
                )
            return read.result()
        except asyncio.CancelledError:
            await self.aclose()
            raise
        finally:
            if not read.done():
                read.cancel()
                with suppress(asyncio.CancelledError):
                    await read

    def _cleanup(self):
        if self.closed.done():
            return False
        self.closed.set_result(None)
        self.ticker.updateEvent -= self._update
        self.ib.errorEvent -= self._error
        self.ib.wrapper.reqId2Ticker.pop(self.req_id, None)
        self.ib.wrapper._reqId2Contract.pop(self.req_id, None)
        self.ib.wrapper.pendingTickers.discard(self.ticker)
        if not self.failure.done():
            self.failure.cancel()
        return True

    async def aclose(self):
        if self._cleanup():
            try:
                self.ib.client.cancelMktData(self.req_id)
            except Exception:
                LOGGER.warning("Failed to cancel closed IB real-time price request",
                               exc_info=True, extra={"req_id": self.req_id})
