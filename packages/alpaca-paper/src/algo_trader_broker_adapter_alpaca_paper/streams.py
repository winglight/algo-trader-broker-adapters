"""Thread-isolated Alpaca websocket streams."""

from __future__ import annotations

import asyncio
import inspect
import logging
import threading
from collections.abc import AsyncIterator, Callable
from typing import Any

from algo_trader_broker_sdk import BrokerConnectionError


LOGGER = logging.getLogger(__name__)
_END = object()


class ThreadedAlpacaStream:
    """Run an alpaca-py blocking stream without blocking the caller event loop."""

    def __init__(
        self,
        stream: Any,
        subscribe: Callable[[Any], None],
        *,
        queue_size: int,
        name: str,
    ) -> None:
        self._stream = stream
        self._subscribe = subscribe
        self._queue_size = queue_size
        self._name = name
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue: asyncio.Queue[Any] | None = None
        self._thread: threading.Thread | None = None
        self._closed = False
        self._overflow = False

    async def start(self) -> "ThreadedAlpacaStream":
        if self._thread is not None:
            return self
        self._loop = asyncio.get_running_loop()
        self._queue = asyncio.Queue(maxsize=self._queue_size)

        async def handler(item: Any) -> None:
            loop = self._loop
            if loop is not None and not self._closed:
                loop.call_soon_threadsafe(self._deliver, item)

        self._subscribe(handler)
        self._thread = threading.Thread(
            target=self._run,
            name=self._name,
            daemon=True,
        )
        self._thread.start()
        return self

    def _deliver(self, item: Any) -> None:
        queue = self._queue
        if queue is None or self._closed:
            return
        try:
            queue.put_nowait(item)
        except asyncio.QueueFull:
            self._overflow = True
            self._closed = True
            LOGGER.error(
                "Alpaca stream queue overflow",
                extra={"event": "alpaca.stream.queue_overflow", "stream": self._name},
            )
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            queue.put_nowait(_END)

    def _run(self) -> None:
        try:
            self._stream.run()
        except BaseException as exc:
            loop = self._loop
            if loop is not None and not self._closed:
                loop.call_soon_threadsafe(self._deliver, exc)
        finally:
            loop = self._loop
            if loop is not None and not self._closed:
                loop.call_soon_threadsafe(self._deliver, _END)

    def __aiter__(self) -> AsyncIterator[Any]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Any]:
        if self._thread is None:
            await self.start()
        queue = self._queue
        assert queue is not None
        try:
            while True:
                item = await queue.get()
                if item is _END:
                    if self._overflow:
                        raise BrokerConnectionError(
                            "Alpaca stream queue overflow requires reconciliation",
                            details={"stream": self._name},
                        )
                    return
                if isinstance(item, BaseException):
                    raise BrokerConnectionError(
                        "Alpaca websocket stream stopped unexpectedly",
                        details={"stream": self._name, "error_type": type(item).__name__},
                    ) from item
                yield item
        finally:
            await self.close()

    async def close(self) -> None:
        if self._closed and self._thread is None:
            return
        self._closed = True
        stop = getattr(self._stream, "stop", None)
        if callable(stop):
            result = stop()
            if inspect.isawaitable(result):
                await result
        thread = self._thread
        if thread is not None and thread.is_alive():
            await asyncio.to_thread(thread.join, 5.0)
        self._thread = None
        queue = self._queue
        if queue is not None:
            try:
                queue.put_nowait(_END)
            except asyncio.QueueFull:
                pass


class TradeUpdateStream:
    """Managed trading stream that dispatches rather than exposes an iterator."""

    def __init__(
        self,
        threaded: ThreadedAlpacaStream,
        handler: Callable[[Any], Any],
        failure_handler: Callable[[BaseException], Any] | None = None,
    ) -> None:
        self._threaded = threaded
        self._handler = handler
        self._failure_handler = failure_handler
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        await self._threaded.start()
        if self._task is None:
            self._task = asyncio.create_task(self._consume(), name="alpaca-paper.trade-updates")

    async def _consume(self) -> None:
        try:
            async for item in self._threaded:
                result = self._handler(item)
                if inspect.isawaitable(result):
                    await result
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            if self._failure_handler is not None:
                result = self._failure_handler(exc)
                if inspect.isawaitable(result):
                    await result
            else:
                raise

    async def close(self) -> None:
        await self._threaded.close()
        task = self._task
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._task = None
