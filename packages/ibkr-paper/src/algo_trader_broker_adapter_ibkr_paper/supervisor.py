"""Lifecycle manager for :class:`IBAsyncClient` instances."""


import asyncio
import logging
from contextlib import suppress
from datetime import datetime
from typing import Awaitable, Callable, Mapping

from .client import IBAsyncClient

LOGGER = logging.getLogger(__name__)


class IBClientSupervisor:
    """Coordinates startup, reconnect hooks, and shutdown for IB clients."""

    def __init__(
        self,
        client_factory: Callable[[], IBAsyncClient],
        *,
        name: str | None = None,
    ) -> None:
        self._client_factory = client_factory
        self._client: IBAsyncClient | None = None
        self._start_lock = asyncio.Lock()
        self._start_task: asyncio.Task[None] | None = None
        self._started = asyncio.Event()
        self._name = (name or "ib-client").strip() or "ib-client"
        self._connected_since: datetime | None = None
        self._reconnect_reason: str | None = None

    def _get_or_create_client(self) -> IBAsyncClient:
        client = self._client
        if client is None:
            client = self._client_factory()
            listener = getattr(client, "add_connection_listener", None)
            if callable(listener):
                listener(self._handle_connection_event)
            else:  # pragma: no cover - fallback for stubs
                LOGGER.debug("IB client does not expose add_connection_listener()")
            self._client = client
        return client

    def get_client(self) -> IBAsyncClient:
        """Return the managed client without forcing an IB connection attempt."""

        return self._get_or_create_client()

    def _handle_connection_event(self, state: str, payload: Mapping[str, object]) -> None:
        connected_since = payload.get("connected_since")
        if isinstance(connected_since, datetime):
            self._connected_since = connected_since
        else:
            self._connected_since = None
        reconnect_reason = payload.get("reconnect_reason")
        if isinstance(reconnect_reason, str):
            self._reconnect_reason = reconnect_reason
        else:
            self._reconnect_reason = None
        log_payload = {
            "event": "ib.supervisor.connection",
            "service": self._name,
            "state": state,
            "connected_since": (
                self._connected_since.isoformat() if self._connected_since else None
            ),
            "reconnect_reason": self._reconnect_reason,
        }
        LOGGER.info(
            "ib.supervisor.connection.%s.%s", self._name, state, extra=log_payload
        )

    async def start(self) -> None:
        """Ensure the underlying client has started its connection manager."""

        client = self._get_or_create_client()
        start_method = getattr(client, "start", None)
        if not callable(start_method):
            self._started.set()
            return

        async with self._start_lock:
            task = self._start_task
            if task is None or task.done():
                task = asyncio.create_task(start_method(), name=f"{self._name}.start")
                self._start_task = task
        assert task is not None
        try:
            await task
        except Exception:
            async with self._start_lock:
                if self._start_task is task:
                    self._start_task = None
            raise
        else:
            self._started.set()
            async with self._start_lock:
                if self._start_task is task:
                    self._start_task = None

    async def ensure_client(self) -> IBAsyncClient:
        """Start the client if needed and return the instance."""

        await self.start()
        return self._get_or_create_client()

    def add_resub_task(self, coro_factory: Callable[[], Awaitable[None]]) -> None:
        """Register a coroutine factory for execution after reconnect."""

        client = self._get_or_create_client()
        registrar = getattr(client, "add_resub_task", None)
        if callable(registrar):
            registrar(coro_factory)
        else:  # pragma: no cover - fallback for simplified stubs
            LOGGER.debug("IB client does not expose add_resub_task(); skipping registration")

    async def stop(self) -> None:
        """Stop the client and release resources."""

        async with self._start_lock:
            task = self._start_task
            self._start_task = None
        if task is not None and not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        self._started.clear()
        client = self._client
        self._client = None
        if client is None:
            return
        close = getattr(client, "close", None)
        if callable(close):
            await close()
            return
        disconnect = getattr(client, "disconnect", None)
        if callable(disconnect):
            await disconnect()

    def connection_state(self) -> Mapping[str, object | None]:
        """Expose the last recorded connection telemetry."""

        return {
            "service": self._name,
            "connected_since": self._connected_since,
            "reconnect_reason": self._reconnect_reason,
            "started": self._started.is_set(),
        }
