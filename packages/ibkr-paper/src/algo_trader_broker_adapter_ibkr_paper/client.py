"""Client abstraction built on top of :mod:`ib_async`."""

import atexit
import asyncio
import copy
import inspect
import logging
import math
import socket
import sys
import threading
import time
import random
import weakref
from collections import defaultdict, deque
from contextlib import suppress
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, AsyncIterator, Awaitable, Callable, Iterable, Mapping, TypeVar, Generic

from concurrent.futures import ThreadPoolExecutor
from ib_async import IB, Order
from ib_async.objects import ExecutionFilter

from .settings import IBGatewaySettings
from .exceptions import IBConnectionError, IBMarketDataError, IBOrderError
from .market_data import (
    DOMLevel,
    DOMSnapshot,
    HistoricalBar,
    HistoricalTickBidAsk,
    HistoricalTickLast,
    RealTimePrice,
    TickByTickBidAsk,
    TickByTickLast,
    TickByTickMidPoint,
)
from .orders import FutureOrderRequest, OptionOrderRequest, OrderResult, StockOrderRequest
from algo_trader_broker_sdk import TradeUpdate

try:  # pragma: no cover - optional IB scanner types
    from ib_async import ScannerSubscription
    from ib_async.objects import TagValue
except Exception:  # pragma: no cover - optional dependency missing
    ScannerSubscription = None  # type: ignore[assignment]
    TagValue = None  # type: ignore[assignment]

T = TypeVar("T")
CallRunner = Callable[[Callable[[IB], T], IB], Awaitable[T]]


@dataclass(slots=True)
class _AsyncIteratorWithReqId(Generic[T]):
    """Lightweight wrapper for async generators carrying IB request metadata.

    Provides `__aiter__`, `__anext__`, and `aclose`, and exposes a `req_id` attribute
    that downstream consumers can read. This avoids setting attributes directly on
    async generator objects, which is not supported in Python.
    """

    _iter: AsyncIterator[T]
    req_id: int | None = None

    def __aiter__(self) -> "_AsyncIteratorWithReqId[T]":
        return self

    async def __anext__(self) -> T:
        return await self._iter.__anext__()

    async def aclose(self) -> None:
        close = getattr(self._iter, "aclose", None)
        if callable(close):
            try:
                await close()
            except RuntimeError as exc:
                # aclose() can race with an in-flight __anext__ during shutdown/cancel.
                if "asynchronous generator is already running" not in str(exc):
                    raise


_EXECUTOR_THREAD_STATE = threading.local()
def _coerce_ib_datetime(value: datetime | str | None) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        token = value.strip()
        parsed: datetime | None = None
        for candidate, fmt in (
            (token, None),
            (token.replace("Z", "+00:00"), None),
            (token, "%Y%m%d-%H:%M:%S"),
            (token, "%Y%m%d %H:%M:%S"),
        ):
            try:
                parsed = (
                    datetime.fromisoformat(candidate)
                    if fmt is None
                    else datetime.strptime(candidate, fmt)
                )
                break
            except ValueError:
                continue
        if parsed is None:
            return value
        value = parsed
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    # IB documents both named-timezone and dashed UTC historical timestamps.
    # Use dashed UTC consistently so broker-facing callers do not drift across
    # local timezone conversions.
    return value.strftime("%Y%m%d-%H:%M:%S")


def _parse_ib_datetime_input(value: str) -> datetime | None:
    token = value.strip()
    for candidate, fmt in (
        (token, None),
        (token.replace("Z", "+00:00"), None),
        (token, "%Y%m%d-%H:%M:%S"),
        (token, "%Y%m%d %H:%M:%S"),
    ):
        try:
            return (
                datetime.fromisoformat(candidate)
                if fmt is None
                else datetime.strptime(candidate, fmt)
            )
        except ValueError:
            continue
    return None


@dataclass(slots=True)
class AccountSummaryItem:
    """Account summary entry returned by IB."""

    account: str
    tag: str
    value: str
    currency: str | None


@dataclass(slots=True)
class PositionItem:
    """Open position representation."""

    account: str
    contract_id: int | None
    symbol: str | None
    sec_type: str | None
    exchange: str | None
    currency: str | None
    position: float
    avg_cost: float
    local_symbol: str | None = None
    primary_exchange: str | None = None
    trading_class: str | None = None


LOGGER = logging.getLogger(__name__)

CLIENT_ID_CONFLICT_CODES = {326}
_AUTO_CLIENT_ID_RETRY_LIMIT = 12
_AUTO_CLIENT_ID_RANDOM_RETRY_LIMIT = 8
_COMPETING_LIVE_SESSION_ERROR_CODE = 10197
_DISCONNECTED_LIVE_UPDATES_ERROR_CODE = 10182
_HISTORICAL_PACING_ERROR_CODE = 162
_INVALID_HISTORICAL_DATETIME_ERROR_CODE = 10314


def _is_historical_pacing_error(code: int | None, message: Any) -> bool:
    if code != _HISTORICAL_PACING_ERROR_CODE:
        return False
    lowered = str(message or "").lower()
    return "historical data request pacing violation" in lowered


def _is_invalid_historical_datetime_error(code: int | None, message: Any) -> bool:
    if code != _INVALID_HISTORICAL_DATETIME_ERROR_CODE:
        return False
    lowered = str(message or "").lower()
    return "date, time, or time-zone" in lowered or "end date/time" in lowered

_MARKET_DEPTH_PERMISSION_ERROR_CODES = {10089, 10092, 309}
_ORDER_REJECTION_ERROR_CODES = {201}
IBKR_ADAPTER_ID = "ibkr_paper"


class IBAsyncClient:
    """High level convenience wrapper around :class:`ib_async.IB`."""

    def __init__(
        self,
        settings: IBGatewaySettings,
        *,
        ib_factory: Callable[[], IB] | None = None,
        call_runner: CallRunner | None = None,
        ping_interval: float = 30.0,
        max_backoff: float = 10.0,
        reconnect_cooldown: float = 3.0,
    ) -> None:
        self._settings = settings
        self._ib_factory = ib_factory or IB
        self._call_runner = call_runner
        self._ib: IB | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._sync_executor: ThreadPoolExecutor | None = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="ib-async-client",
        )
        self._executor_loop: asyncio.AbstractEventLoop | None = None
        self._connect_lock = asyncio.Lock()
        self._connected = asyncio.Event()
        self._client_id = settings.client_id
        self._trade_update_handler: Callable[[TradeUpdate], Awaitable[None]] | None = None
        self._position_update_handler: Callable[[list[PositionItem]], Awaitable[None]] | None = None
        self._account_update_handler: Callable[[list[AccountSummaryItem]], Awaitable[None]] | None = None
        self._order_status_listener: Callable[..., None] | None = None
        self._order_error_listener: Callable[..., None] | None = None
        self._exec_details_listener: Callable[..., None] | None = None
        self._commission_report_listener: Callable[..., None] | None = None
        self._position_listener: Callable[..., None] | None = None
        self._account_value_listener: Callable[..., None] | None = None
        self._account_updates_subscribed = False
        self._pnl_listener: Callable[[Any], None] | None = None
        self._pnl_subscription_lock = asyncio.Lock()
        self._pnl_subscriptions: dict[tuple[str, str], tuple[str, str | None]] = {}
        self._pnl_live_objects: dict[tuple[str, str], Any] = {}
        self._pnl_snapshots: dict[tuple[str, str], dict[str, float | None]] = {}
        self._pnl_waiters: defaultdict[
            tuple[str, str],
            list[asyncio.Future[dict[str, float | None] | None]],
        ] = defaultdict(list)

        # --- Reconnect, resubscribe, and keepalive management ---
        self._error_listener: Callable[..., None] | None = None
        self._reconnecting_lock = asyncio.Lock()
        self._reconnect_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._ping_task: asyncio.Task | None = None
        self._ping_interval = float(ping_interval)
        self._max_backoff = float(max_backoff)
        self._resub_tasks: list[Callable[[], Awaitable[None]]] = []
        self._reconnect_cooldown = float(reconnect_cooldown)
        self._next_reconnect_allowed = 0.0
        self._connected_since: datetime | None = None
        self._connection_state_listeners: list[
            Callable[[str, Mapping[str, Any]], Awaitable[None] | None]
        ] = []
        self._pending_reconnect_reason: str | None = None
        self._connected_host: str | None = None
        self._atexit_cleanup = self._build_atexit_cleanup()
        atexit.register(self._atexit_cleanup)

    @staticmethod
    def _call_async_with_timeout(
        func: Callable[..., Awaitable[T]],
        *args: Any,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> Awaitable[T]:
        if timeout is None:
            return func(*args, **kwargs)
        try:
            return func(*args, **kwargs, timeout=timeout)
        except TypeError as exc:
            message = str(exc)
            if "timeout" in message and (
                "unexpected keyword" in message or "got an unexpected keyword" in message
            ):
                return func(*args, **kwargs)
            raise

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    @property
    def client_id(self) -> int:
        return self._client_id

    def set_trade_update_handler(
        self, handler: Callable[[TradeUpdate], Awaitable[None]] | None
    ) -> None:
        """Register a coroutine callback to receive trade updates."""

        self._trade_update_handler = handler
        ib = self._ib
        if ib is not None and handler is not None:
            self._install_order_listeners(ib)
        elif ib is not None and handler is None:
            self._remove_order_listeners(ib)

    def set_position_update_handler(
        self, handler: Callable[[list[PositionItem]], Awaitable[None]] | None
    ) -> None:
        """Register a coroutine callback to receive full live position snapshots."""

        self._position_update_handler = handler
        ib = self._ib
        if ib is not None and handler is not None:
            self._install_position_listeners(ib)
        elif ib is not None and handler is None:
            self._remove_position_listeners(ib)

    def set_account_update_handler(
        self, handler: Callable[[list[AccountSummaryItem]], Awaitable[None]] | None
    ) -> None:
        """Register a coroutine callback to receive live account value updates."""

        self._account_update_handler = handler
        ib = self._ib
        if ib is not None and handler is not None:
            self._install_account_listeners(ib)
        elif ib is not None and handler is None:
            self._remove_account_listeners(ib)

    def add_connection_listener(
        self, listener: Callable[[str, Mapping[str, Any]], Awaitable[None] | None]
    ) -> None:
        """Register a callback that receives connection state transitions."""

        self._connection_state_listeners.append(listener)

    def remove_connection_listener(
        self, listener: Callable[[str, Mapping[str, Any]], Awaitable[None] | None]
    ) -> None:
        """Remove a previously registered connection listener."""

        with suppress(ValueError):
            self._connection_state_listeners.remove(listener)

    def connection_state_snapshot(self) -> Mapping[str, Any]:
        """Return the latest known connection telemetry."""

        return {
            "connected": self._connected.is_set(),
            "connected_since": self._connected_since,
            "reconnect_reason": self._pending_reconnect_reason,
            "host": self._connected_host or self._settings.host,
            "port": self._settings.port,
            "client_id": self._client_id,
        }

    def _notify_connection_state(self, state: str, **payload: Any) -> None:
        event: dict[str, Any] = {
            "state": state,
            "host": self._connected_host or self._settings.host,
            "port": self._settings.port,
            "client_id": self._client_id,
            "connected_since": self._connected_since,
        }
        reconnect_reason = payload.get("reconnect_reason")
        if reconnect_reason is None:
            reconnect_reason = self._pending_reconnect_reason
        event["reconnect_reason"] = reconnect_reason
        event.update(payload)

        log_payload = {
            "event": "ib.connection.state",
            "state": state,
            "host": self._connected_host or self._settings.host,
            "port": self._settings.port,
            "client_id": self._client_id,
            "reconnect_reason": reconnect_reason,
            "connected_since": (
                event["connected_since"].isoformat()
                if isinstance(event.get("connected_since"), datetime)
                else None
            ),
        }
        LOGGER.info("ib.connection.state.%s", state, extra=log_payload)

        if not self._connection_state_listeners:
            return

        snapshot = dict(event)
        for listener in list(self._connection_state_listeners):
            try:
                result = listener(state, snapshot)
                if inspect.isawaitable(result):
                    asyncio.create_task(result)  # pragma: no cover - diagnostic hook
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.exception("IB connection state listener failed")

    def _build_atexit_cleanup(self) -> Callable[[], None]:
        client_ref = weakref.ref(self)

        def _cleanup() -> None:
            client = client_ref()
            if client is None:
                return
            client._disconnect_sync_best_effort("process_exit")

        return _cleanup

    def _disconnect_sync_best_effort(self, reason: str | None = None) -> None:
        ib = self._ib
        if ib is None:
            return
        self._ib = None
        self._connected.clear()
        self._connected_since = None
        self._connected_host = None
        with suppress(Exception):
            self._notify_connection_state("disconnected", reconnect_reason=reason)
        with suppress(Exception):
            self._remove_order_listeners(ib)
        with suppress(Exception):
            self._remove_position_listeners(ib)
        with suppress(Exception):
            self._remove_account_listeners(ib)
        with suppress(Exception):
            self._remove_core_listeners(ib)
        with suppress(Exception):
            ib.disconnect()
        self._loop = None

    async def _release_conflicting_client_id(self, client_id: int) -> bool:
        released = False
        if self._reconnect_task is not None and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._reconnect_task
        if self._ib is not None:
            released = True
            await self.disconnect(reason=f"client_id_conflict:{client_id}")
            await asyncio.sleep(0.2)
        return released

    async def connect(self) -> None:
        """Connect to the configured IB gateway."""

        async with self._connect_lock:
            if self._connected.is_set():
                return

            self._settings.validate()
            base_client_id = (
                self._client_id
                if self._client_id is not None
                else self._settings.client_id
            )
            candidate_queue: deque[int] = deque()
            seen_ids: set[int] = set()
            conflict_release_attempted: set[int] = set()

            def enqueue(candidate: int) -> bool:
                if candidate < 0 or candidate in seen_ids:
                    return False
                seen_ids.add(candidate)
                candidate_queue.append(candidate)
                return True

            def enqueue_front(candidate: int) -> bool:
                if candidate < 0 or candidate in seen_ids:
                    return False
                seen_ids.add(candidate)
                candidate_queue.appendleft(candidate)
                return True

            enqueue(base_client_id)
            for fallback_client_id in self._settings.client_id_fallbacks:
                enqueue(fallback_client_id)
            host_candidates: list[str] = [self._settings.host]
            for fallback_host in self._settings.host_fallbacks:
                normalized_host = str(fallback_host).strip()
                if normalized_host and normalized_host not in host_candidates:
                    host_candidates.append(normalized_host)

            attempts: list[tuple[str, int]] = []
            last_exc: Exception | None = None
            while candidate_queue:
                client_id = candidate_queue.popleft()
                client_id_conflicted = False
                for host in host_candidates:
                    attempts.append((host, client_id))
                    LOGGER.debug(
                        "Attempting IB gateway connection: host=%s port=%s client_id=%s timeout=%s read_only=%s account=%s",
                        host,
                        self._settings.port,
                        client_id,
                        self._settings.connect_timeout,
                        self._settings.read_only,
                        self._settings.account,
                    )

                    ib = self._ib_factory()
                    error_records: list[tuple[int | None, str]] = []
                    error_handler: Callable[..., None] | None = None
                    error_event = getattr(ib, "errorEvent", None)

                    if error_event is not None:
                        def _capture_error(req_id: Any, code: Any, message: Any, misc: Any) -> None:
                            try:
                                code_int = int(code) if code is not None else None
                            except Exception:
                                code_int = None
                            error_records.append((code_int, str(message)))

                        try:
                            error_event += _capture_error  # type: ignore[operator]
                            error_handler = _capture_error
                        except Exception:  # pragma: no cover - defensive; errorEvent may be immutable
                            error_handler = None

                    try:
                        await ib.connectAsync(
                            host,
                            self._settings.port,
                            clientId=client_id,
                            timeout=self._settings.connect_timeout,
                            readonly=self._settings.read_only,
                        )
                    except Exception as exc:  # pragma: no cover - network failure
                        last_exc = exc
                        if error_handler is not None and error_event is not None:
                            with suppress(Exception):
                                error_event -= error_handler  # type: ignore[operator]
                        with suppress(Exception):
                            await self._run_in_sync_executor(lambda client: client.disconnect(), ib)

                        if self._should_retry_with_client_id_fallback(error_records, exc):
                            client_id_conflicted = True
                            released = False
                            if client_id not in conflict_release_attempted:
                                conflict_release_attempted.add(client_id)
                                released = await self._release_conflicting_client_id(client_id)
                                if released:
                                    enqueue_front(client_id)
                            fallback_client_id = client_id + 1
                            fallback_enqueued = enqueue(fallback_client_id)
                            if released:
                                LOGGER.warning(
                                    "IB client id %s conflict detected; released previous local connection and retrying same id",
                                    client_id,
                                )
                            elif fallback_enqueued:
                                LOGGER.warning(
                                    "IB client id %s appears to be in use; retrying fallback client id %s",
                                    client_id,
                                    fallback_client_id,
                                )
                            elif candidate_queue:
                                LOGGER.warning(
                                    "IB client id %s appears to be in use; retrying with alternative client id %s",
                                    client_id,
                                    candidate_queue[0],
                                )
                            else:
                                LOGGER.warning(
                                    "IB client id %s appears to be in use but no alternative client ids are available",
                                    client_id,
                                )
                            break

                        if isinstance(exc, socket.gaierror):
                            LOGGER.warning(
                                "IB host resolution failed for %s:%s (client_id=%s); trying next host candidate",
                                host,
                                self._settings.port,
                                client_id,
                            )
                            continue
                        continue

                    if error_handler is not None and error_event is not None:
                        with suppress(Exception):
                            error_event -= error_handler  # type: ignore[operator]

                    self._ib = ib
                    self._loop = asyncio.get_running_loop()
                    self._connected.set()
                    self._connected_since = datetime.now(timezone.utc)
                    self._client_id = client_id
                    self._connected_host = host
                    reconnect_reason = self._pending_reconnect_reason
                    if reconnect_reason:
                        self._notify_connection_state(
                            "restored",
                            reconnect_reason=reconnect_reason,
                        )
                    else:
                        self._notify_connection_state("connected")
                    self._pending_reconnect_reason = None
                    if self._trade_update_handler is not None:
                        self._install_order_listeners(ib)
                    if self._position_update_handler is not None:
                        self._install_position_listeners(ib)
                    if self._account_update_handler is not None:
                        self._install_account_listeners(ib)

                    # Attach core listeners once connected
                    self._install_core_listeners_once(ib)
                    LOGGER.debug(
                        "Connected to IB gateway: host=%s port=%s client_id=%s",
                        host,
                        self._settings.port,
                        client_id,
                    )
                    return

                if client_id_conflicted:
                    continue

            if last_exc is not None:
                failure_ids = ", ".join(
                    f"{host}:{client_id}" for host, client_id in attempts
                ) or "none"
                LOGGER.exception(
                    "IB gateway connection failed after exhausting host/client candidates: host=%s host_fallbacks=%s port=%s attempted=[%s]",
                    self._settings.host,
                    ",".join(self._settings.host_fallbacks) or "-",
                    self._settings.port,
                    failure_ids,
                )
                raise IBConnectionError("Unable to connect to IB gateway") from last_exc

            raise IBConnectionError("Unable to connect to IB gateway")

    async def disconnect(self, reason: str | None = None) -> None:
        """Disconnect from the gateway."""

        ib: IB | None = None
        async with self._connect_lock:
            ib = self._ib
            self._ib = None
            if self._connected.is_set() or ib is not None:
                self._connected.clear()
                self._connected_since = None
                self._connected_host = None
                self._notify_connection_state("disconnected", reconnect_reason=reason)

        if ib is not None:
            await self._reset_pnl_state(ib)
            self._remove_order_listeners(ib)
            self._remove_position_listeners(ib)
            self._remove_account_listeners(ib)
            self._remove_core_listeners(ib)
            await self._run_in_sync_executor(lambda client: client.disconnect(), ib)
            LOGGER.debug(
                "Disconnected from IB gateway: host=%s port=%s client_id=%s",
                self._settings.host,
                self._settings.port,
                self._client_id,
            )
            self._loop = None
        await self._shutdown_sync_executor()

    async def reconnect(self, *, reason: str | None = None) -> None:
        await self._schedule_reconnect(reason=reason or "manual_reconnect")

    async def ensure_connected(self) -> IB:
        """Ensure there is an active connection, with exponential backoff if needed.

        This method retries connection establishment using exponential backoff
        to handle transient connectivity issues and gateway restarts.
        """
        if self._connected.is_set():
            assert self._ib is not None
            if self._ib.isConnected():
                return self._ib

            LOGGER.warning("IB client event is set but isConnected() returned False; triggering reconnect.")
            self._connected.clear()
            reconnect_reason = "stale_connection"
            self._pending_reconnect_reason = reconnect_reason
            self._notify_connection_state("reconnecting", reconnect_reason=reconnect_reason)

        LOGGER.debug(
            "IB gateway client is not connected; establishing connection: host=%s port=%s client_id=%s",
            self._settings.host,
            self._settings.port,
            self._client_id,
        )

        # Gate rapid re-entry attempts across call sites
        now = time.monotonic()
        gate_delay = max(0.0, self._next_reconnect_allowed - now)
        if gate_delay > 0.0:
            await asyncio.sleep(gate_delay)

        backoff = 0.5
        attempt = 0
        while not self._stop_event.is_set():
            attempt += 1
            try:
                await self.connect()
            except IBConnectionError as exc:  # pragma: no cover - network failure
                LOGGER.warning("IB connect attempt %d failed: %r", attempt, exc)
            except Exception as exc:  # pragma: no cover - unexpected failure
                LOGGER.warning("IB connect attempt %d raised: %r", attempt, exc)
            else:
                if self._connected.is_set() and self._ib is not None:
                    return self._ib

            # Exponential backoff with jitter
            jitter = random.uniform(0.0, 0.3)
            delay = min(max(self._reconnect_cooldown, backoff + jitter), self._max_backoff)
            self._next_reconnect_allowed = time.monotonic() + delay
            LOGGER.info("Retrying IB connection in %.1f seconds", delay)
            await asyncio.sleep(delay)
            backoff = min(backoff * 2.0, self._max_backoff)

        raise IBConnectionError("IB connection retry loop stopped")

    async def start(self) -> None:
        """Start connection management: connect, install listeners, and keepalive ping."""
        ib = await self.ensure_connected()
        self._install_core_listeners_once(ib)
        self._start_ping_loop()

    async def close(self) -> None:
        """Stop keepalive and disconnect gracefully."""
        self._stop_event.set()
        task = self._ping_task
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        self._ping_task = None
        await self.disconnect(reason="shutdown")

    @staticmethod
    def _should_retry_with_client_id_fallback(
        error_records: Iterable[tuple[int | None, str]],
        exc: Exception,
    ) -> bool:
        for code, message in error_records:
            if code in CLIENT_ID_CONFLICT_CODES:
                return True
            lowered = message.lower()
            if "client id" in lowered and "in use" in lowered:
                return True
        text = str(exc)
        if text:
            lowered_exc = text.lower()
            if "client id" in lowered_exc and "in use" in lowered_exc:
                return True

        return False

    @staticmethod
    def _is_competing_live_session_error(code: int | None, message: Any) -> bool:
        if code == _COMPETING_LIVE_SESSION_ERROR_CODE:
            return True
        lowered = str(message or "").lower()
        if "competing live session" in lowered and "market data" in lowered:
            return True
        return "different ip address" in lowered and "trading tws session" in lowered

    async def get_account_summary(self, account: str | None = None) -> list[AccountSummaryItem]:
        """Fetch account summary information."""

        async def fetch(ib: IB) -> list[AccountSummaryItem]:
            accounts = ib.managedAccounts()
            target = account or (accounts[0] if accounts else None)
            if not target:
                return []
            summary = await ib.accountSummaryAsync(target)
            items: list[AccountSummaryItem] = []
            for entry in summary:
                items.append(
                    AccountSummaryItem(
                        account=getattr(entry, "account", target),
                        tag=getattr(entry, "tag", ""),
                        value=str(getattr(entry, "value", "")),
                        currency=getattr(entry, "currency", None),
                    )
                )
            return items

        return await self._run_with_ib_async(fetch)

    async def request_scanner_parameters(self) -> str:
        """Fetch IB scanner metadata XML."""

        async def fetch(ib: IB) -> str:
            method = getattr(ib, "reqScannerParametersAsync", None)
            if method is not None:
                return await method()  # type: ignore[misc]
            sync_method = getattr(ib, "reqScannerParameters", None)
            if sync_method is None:
                raise IBMarketDataError("IB client does not support scanner parameters")
            result = sync_method()  # type: ignore[misc]
            return result if isinstance(result, str) else str(result)

        return await self._run_with_ib_async(fetch)

    async def request_scanner_data(
        self,
        payload: Mapping[str, Any],
        *,
        tag_filters: Iterable[Mapping[str, Any]] | None = None,
        stream_seconds: float | None = None,
    ) -> list[dict[str, Any]]:
        """Execute an IB scanner request and normalize the results."""

        subscription, tags = _build_scanner_subscription(payload, tag_filters)
        try:
            LOGGER.info(
                "IB scanner request prepared | subscription=%s tags=%s",
                subscription,
                len(tags),
            )
        except Exception:
            LOGGER.debug("Failed to log IB scanner request payload", exc_info=True)

        async def fetch(ib: IB) -> list[dict[str, Any]]:
            req_stream = getattr(ib, "reqScannerSubscription", None)
            method = getattr(ib, "reqScannerDataAsync", None)
            if method is None and not callable(req_stream):
                raise IBMarketDataError("IB client does not support scanner data requests")
            if callable(req_stream):
                try:
                    sig = inspect.signature(req_stream)
                except (TypeError, ValueError):
                    sig = None
                kwargs: dict[str, Any] = {}
                if sig is not None:
                    params = sig.parameters
                    if "scannerSubscriptionFilterOptions" in params:
                        kwargs["scannerSubscriptionFilterOptions"] = tags
                    elif "filterOptions" in params:
                        kwargs["filterOptions"] = tags
                scan_data = req_stream(subscription, **kwargs)  # type: ignore[misc]
                cancel = getattr(ib, "cancelScannerSubscription", None)
                try:
                    wait_for = float(stream_seconds) if stream_seconds is not None else 5.0
                    if wait_for > 0:
                        await asyncio.sleep(wait_for)
                    data = list(scan_data)
                    if data:
                        try:
                            sample = data[0]
                            details = getattr(sample, "contractDetails", None)
                            contract = getattr(details, "contract", None) if details is not None else None
                            LOGGER.warning(
                                "IB scanner raw sample | entry=%s contractDetails=%s contract=%s",
                                sample,
                                details,
                                contract,
                            )
                        except Exception:
                            LOGGER.debug("Failed to log IB scanner raw sample", exc_info=True)
                    return [_scanner_data_to_dict(entry) for entry in data]
                finally:
                    if callable(cancel):
                        with suppress(Exception):
                            cancel(scan_data)
                return []
            try:
                data = None
                try:
                    sig = inspect.signature(method)
                except (TypeError, ValueError):
                    sig = None
                if sig is not None:
                    parameters = sig.parameters
                    kwargs: dict[str, Any] = {}
                    if "scannerSubscriptionFilterOptions" in parameters:
                        kwargs["scannerSubscriptionFilterOptions"] = tags
                    elif "filterOptions" in parameters:
                        kwargs["filterOptions"] = tags
                    elif "scannerSubscriptionOptions" in parameters:
                        # IB uses misc options for manual-only; avoid sending filters there.
                        kwargs = {}
                    if kwargs:
                        data = await method(subscription, **kwargs)  # type: ignore[misc]
                    else:
                        data = await method(subscription)  # type: ignore[misc]
                else:
                    data = await method(subscription, tags)  # type: ignore[misc]
            except TypeError:
                data = await method(subscription)  # type: ignore[misc]
            return [_scanner_data_to_dict(entry) for entry in (data or [])]

        return await self._run_with_ib_async(fetch)

    async def request_contract_details(self, contract: Any) -> list[dict[str, Any]]:
        """Fetch contract details and normalize them for downstream services."""

        contract = _require_contract(contract)

        async def fetch(ib: IB) -> list[dict[str, Any]]:
            method = getattr(ib, "reqContractDetailsAsync", None)
            if method is not None:
                details = await method(contract)  # type: ignore[misc]
            else:
                sync_method = getattr(ib, "reqContractDetails", None)
                if sync_method is None:
                    raise IBMarketDataError("IB client does not support contract detail requests")
                details = sync_method(contract)  # type: ignore[misc]
            return [_contract_details_to_dict(item) for item in list(details or [])]

        return await self._run_with_ib_async(fetch)

    async def request_option_parameters(
        self,
        *,
        symbol: str,
        con_id: int,
        sec_type: str = "STK",
        exchange: str = "",
    ) -> list[dict[str, Any]]:
        """Fetch IB option parameter sets for an underlying contract."""

        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            raise IBMarketDataError("symbol must not be empty for option parameter requests")
        if not con_id:
            raise IBMarketDataError("con_id must not be empty for option parameter requests")

        async def fetch(ib: IB) -> list[dict[str, Any]]:
            method = getattr(ib, "reqSecDefOptParamsAsync", None)
            if method is not None:
                payload = await method(normalized_symbol, exchange or "", sec_type or "STK", int(con_id))  # type: ignore[misc]
            else:
                sync_method = getattr(ib, "reqSecDefOptParams", None)
                if sync_method is None:
                    raise IBMarketDataError("IB client does not support option parameter requests")
                payload = sync_method(normalized_symbol, exchange or "", sec_type or "STK", int(con_id))  # type: ignore[misc]
            return [_option_parameter_to_dict(item) for item in list(payload or [])]

        return await self._run_with_ib_async(fetch)

    async def request_market_snapshot(
        self,
        contract: Any,
        *,
        generic_tick_list: str = "",
        snapshot_timeout: float | None = None,
    ) -> dict[str, Any]:
        """Request a one-shot market snapshot for a contract."""

        contract = _require_contract(contract)
        try:
            if not getattr(contract, "conId", None):
                contract = await self.qualify_contract(contract)
        except Exception as exc:
            raise IBMarketDataError("Failed to qualify contract for market snapshot") from exc

        async def fetch(ib: IB) -> dict[str, Any]:
            timeout = float(snapshot_timeout or getattr(self._settings, "historical_timeout", 15.0) or 15.0)
            ticker: Any | None = None
            req_tickers = getattr(ib, "reqTickersAsync", None)
            if callable(req_tickers) and not generic_tick_list:
                try:
                    tickers = await asyncio.wait_for(req_tickers(contract), timeout=timeout)  # type: ignore[misc]
                    if tickers:
                        ticker = tickers[0]
                except Exception:
                    ticker = None

            if ticker is None:
                req_mkt_data = getattr(ib, "reqMktData", None)
                if not callable(req_mkt_data):
                    raise IBMarketDataError("IB client does not support market snapshot requests")
                ticker = req_mkt_data(contract, generic_tick_list or "", True, False)  # type: ignore[misc]
                error_event = getattr(ib, "errorEvent", None)
                error_handler: Callable[..., None] | None = None
                failure_future: asyncio.Future[tuple[int | None, str]] | None = None
                target_req_id = getattr(ticker, "reqId", None)
                loop = self._loop or asyncio.get_running_loop()
                if error_event is not None:
                    future: asyncio.Future[tuple[int | None, str]] = loop.create_future()

                    def _handle_error(req_id: Any, code: Any, message: Any, misc: Any) -> None:
                        if future.done():
                            return
                        try:
                            code_int = int(code) if code is not None else None
                        except Exception:
                            code_int = None
                        if not self._is_competing_live_session_error(code_int, message):
                            return
                        req_match = target_req_id is None
                        if target_req_id is not None:
                            try:
                                req_match = int(req_id) == target_req_id
                            except Exception:
                                req_match = False
                        if not req_match:
                            return
                        future.set_result((code_int, str(message)))

                    try:
                        error_event += _handle_error  # type: ignore[attr-defined]
                    except Exception:
                        error_handler = None
                    else:
                        error_handler = _handle_error
                        failure_future = future
                deadline = time.monotonic() + max(timeout, 1.0)
                try:
                    while time.monotonic() < deadline:
                        if failure_future is not None and failure_future.done():
                            code, message = failure_future.result()
                            raise IBMarketDataError(
                                f"Market snapshot request rejected by IB (code {code}): {message}"
                            )
                        snapshot = _ticker_to_snapshot(ticker, contract=contract)
                        if _snapshot_has_market_data(snapshot):
                            break
                        await asyncio.sleep(0.25)
                finally:
                    if error_handler is not None and error_event is not None:
                        with suppress(Exception):
                            error_event -= error_handler  # type: ignore[attr-defined]
                    with suppress(Exception):
                        _cancel_market_data(ib, ticker, contract)

            return _ticker_to_snapshot(ticker, contract=contract)

        return await self._run_with_ib_async(fetch)

    async def request_option_snapshot(
        self,
        contract: Any,
        *,
        snapshot_timeout: float | None = None,
    ) -> dict[str, Any]:
        """Request a one-shot option quote and greeks snapshot."""

        return await self.request_market_snapshot(
            contract,
            generic_tick_list="100,101,104,106",
            snapshot_timeout=snapshot_timeout,
        )

    async def request_option_greeks(
        self,
        contract: Any,
        *,
        snapshot_timeout: float | None = None,
    ) -> dict[str, Any]:
        """Alias for option quote snapshots when callers only need greeks."""

        return await self.request_option_snapshot(contract, snapshot_timeout=snapshot_timeout)

    @staticmethod
    def _normalize_pnl_key(account: str, model_code: str | None) -> tuple[str, str]:
        return (account.strip(), (model_code or "").strip())

    def _publish_pnl_snapshot(
        self,
        key: tuple[str, str],
        snapshot: dict[str, float | None],
    ) -> None:
        self._pnl_snapshots[key] = snapshot
        waiters = self._pnl_waiters.pop(key, None)
        if waiters:
            for future in waiters:
                if not future.done():
                    future.set_result(snapshot)

    @staticmethod
    def _snapshot_from_pnl_object(pnl_obj: Any) -> dict[str, float | None] | None:
        snapshot = {
            "RealizedPnL": _optional_float(getattr(pnl_obj, "realizedPnL", None)),
            "UnrealizedPnL": _optional_float(getattr(pnl_obj, "unrealizedPnL", None)),
        }
        if snapshot["RealizedPnL"] is None and snapshot["UnrealizedPnL"] is None:
            return None
        return snapshot

    def _install_pnl_listener(self, ib: IB) -> bool:
        if self._pnl_listener is not None:
            return True

        pnl_event = getattr(ib, "pnlEvent", None)
        if pnl_event is None:
            LOGGER.debug("IB client does not provide pnlEvent; cannot stream account PnL")
            return False

        def _on_pnl_event(pnl_obj: Any) -> None:
            account_value = (getattr(pnl_obj, "account", "") or "").strip()
            model_value = (getattr(pnl_obj, "modelCode", "") or "").strip()
            key = (account_value, model_value)
            self._pnl_live_objects[key] = pnl_obj
            snapshot = self._snapshot_from_pnl_object(pnl_obj)
            if snapshot is None:
                return
            loop = self._loop
            if loop is not None and loop.is_running():
                loop.call_soon_threadsafe(self._publish_pnl_snapshot, key, snapshot)
            else:
                self._publish_pnl_snapshot(key, snapshot)

        try:
            pnl_event += _on_pnl_event  # type: ignore[operator]
        except Exception:
            LOGGER.exception("Failed to register pnlEvent handler")
            return False

        self._pnl_listener = _on_pnl_event
        return True

    async def _ensure_pnl_subscription(
        self,
        ib: IB,
        account: str,
        model_code: str | None,
    ) -> bool:
        normalized_account, normalized_model = self._normalize_pnl_key(account, model_code)
        key = (normalized_account, normalized_model)

        async with self._pnl_subscription_lock:
            if key in self._pnl_subscriptions:
                return True

            if not self._install_pnl_listener(ib):
                return False

            method = getattr(ib, "reqPnL", None)
            if not callable(method):
                LOGGER.debug("IB client does not provide reqPnL; skipping live PnL")
                return False

            try:
                if model_code is not None:
                    pnl_obj = method(normalized_account, model_code.strip())
                else:
                    pnl_obj = method(normalized_account)
            except Exception:  # pragma: no cover - capability/runtime issues
                LOGGER.exception("Failed to subscribe to PnL updates via reqPnL")
                return False

            stored_model = model_code.strip() if isinstance(model_code, str) else None
            self._pnl_subscriptions[key] = (normalized_account, stored_model)
            self._pnl_live_objects[key] = pnl_obj
            return True

    async def _reset_pnl_state(self, ib: IB) -> None:
        async with self._pnl_subscription_lock:
            if self._pnl_listener is not None:
                pnl_event = getattr(ib, "pnlEvent", None)
                if pnl_event is not None:
                    with suppress(Exception):
                        pnl_event -= self._pnl_listener  # type: ignore[operator]
                self._pnl_listener = None

            cancel = getattr(ib, "cancelPnL", None)
            if callable(cancel):
                for (account_key, model_key), (account_value, model_value) in list(self._pnl_subscriptions.items()):
                    variants: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
                    if model_value is not None:
                        variants.append(((account_value, model_value), {}))
                    variants.append(((account_value,), {}))
                    with suppress(Exception):
                        _call_with_variants(cancel, variants)

            for futures in self._pnl_waiters.values():
                for future in futures:
                    if not future.done():
                        future.set_result(None)

            self._pnl_subscriptions.clear()
            self._pnl_live_objects.clear()
            self._pnl_snapshots.clear()
            self._pnl_waiters = defaultdict(list)

    async def get_account_pnl(
        self,
        account: str | None = None,
        *,
        model_code: str | None = None,
        timeout: float = 5.0,
    ) -> dict[str, float | None] | None:
        """Retrieve the latest account-level PnL snapshot using ``reqPnL``.

        A persistent subscription is maintained per account/model combination so
        repeat calls return immediately with the most recent snapshot once the
        gateway has delivered at least one update. When the capability is not
        available or no update is received within ``timeout`` seconds, ``None``
        is returned to allow callers to rely on cached values.
        """

        async def fetch(ib: IB) -> dict[str, float | None] | None:
            target = account
            if not target:
                accounts = ib.managedAccounts()
                if accounts:
                    target = accounts[0]
            if not target:
                LOGGER.debug("Unable to determine account for reqPnL snapshot")
                return None

            subscribed = await self._ensure_pnl_subscription(ib, target, model_code)
            if not subscribed:
                return None

            key = self._normalize_pnl_key(target, model_code)
            live_snapshot = self._snapshot_from_pnl_object(
                self._pnl_live_objects.get(key)
            )
            if live_snapshot is not None:
                self._publish_pnl_snapshot(key, live_snapshot)
                return live_snapshot

            snapshot = self._pnl_snapshots.get(key)
            if snapshot is not None:
                return snapshot

            loop = asyncio.get_running_loop()
            deadline = loop.time() + max(0.0, timeout)
            while True:
                live_snapshot = self._snapshot_from_pnl_object(
                    self._pnl_live_objects.get(key)
                )
                if live_snapshot is not None:
                    self._publish_pnl_snapshot(key, live_snapshot)
                    return live_snapshot

                snapshot = self._pnl_snapshots.get(key)
                if snapshot is not None:
                    return snapshot

                remaining = deadline - loop.time()
                if remaining <= 0:
                    LOGGER.warning("Timed out waiting for PnL snapshot from reqPnL")
                    return None

                future: asyncio.Future[dict[str, float | None] | None] = (
                    loop.create_future()
                )
                self._pnl_waiters[key].append(future)
                try:
                    result = await asyncio.wait_for(
                        future, timeout=min(0.25, remaining)
                    )
                    if result is not None:
                        return result
                except asyncio.TimeoutError:
                    continue
                finally:
                    waiters = self._pnl_waiters.get(key)
                    if waiters and future in waiters:
                        waiters.remove(future)
                        if not waiters:
                            self._pnl_waiters.pop(key, None)

        return await self._run_with_ib_async(fetch)

    async def get_positions(self) -> list[PositionItem]:
        """Retrieve open positions for the connected account."""

        async def fetch(ib: IB) -> list[PositionItem]:
            positions = await ib.reqPositionsAsync()
            result: list[PositionItem] = []
            for pos in positions:
                contract = getattr(pos, "contract", None)
                result.append(
                    PositionItem(
                        account=getattr(pos, "account", ""),
                        contract_id=_safe_get(contract, "conId"),
                        symbol=_safe_get(contract, "symbol"),
                        sec_type=_safe_get(contract, "secType"),
                        exchange=_safe_get(contract, "exchange"),
                        currency=_safe_get(contract, "currency"),
                        position=float(getattr(pos, "position", 0.0) or 0.0),
                        avg_cost=float(getattr(pos, "avgCost", 0.0) or 0.0),
                        local_symbol=_safe_get(contract, "localSymbol"),
                        primary_exchange=_safe_get(contract, "primaryExchange"),
                        trading_class=_safe_get(contract, "tradingClass"),
                    )
                )
            self._dispatch_position_snapshot(result)
            return result

        return await self._run_with_ib_async(fetch)

    async def place_stock_order(self, request: StockOrderRequest) -> OrderResult:
        """Submit an order for a stock contract."""

        try:
            contract, order = request.build()
        except ValueError as exc:
            raise IBOrderError(str(exc)) from exc
        return await self._submit_order(contract, order)

    async def place_future_order(self, request: FutureOrderRequest) -> OrderResult:
        """Submit an order for a futures contract."""

        try:
            contract, order = request.build()
        except ValueError as exc:
            raise IBOrderError(str(exc)) from exc
        try:
            contract = await self.qualify_contract(contract)
        except Exception as exc:
            raise IBOrderError(
                f"Failed to qualify futures contract before submit: {_contract_to_dict(contract)}"
            ) from exc
        return await self._submit_order(contract, order)

    async def place_option_order(self, request: OptionOrderRequest) -> OrderResult:
        """Submit an order for an option contract."""

        try:
            contract, order = request.build()
        except ValueError as exc:
            raise IBOrderError(str(exc)) from exc
        try:
            contract = await self.qualify_contract(contract)
        except Exception as exc:
            raise IBOrderError(
                f"Failed to qualify option contract before submit: {_contract_to_dict(contract)}"
            ) from exc
        return await self._submit_order(contract, order)

    async def cancel_order(self, order_id: int | str) -> None:
        """Cancel an existing order using its IB identifier."""

        try:
            adapter_order_id = int(str(order_id))
        except (TypeError, ValueError) as exc:
            raise IBOrderError("Invalid order identifier for cancellation") from exc

        async def cancel(ib: IB) -> None:
            error_event = getattr(ib, "errorEvent", None)
            captured: list[tuple[int | None, str]] = []
            handler: Callable[..., None] | None = None
            if error_event is not None:
                def _capture(req_id: Any, code: Any, message: Any, misc: Any) -> None:
                    try:
                        code_int = int(code) if code is not None else None
                    except Exception:
                        code_int = None
                    captured.append((code_int, str(message)))
                try:
                    error_event += _capture  # type: ignore[operator]
                    handler = _capture
                except Exception:
                    handler = None
            try:
                cancel_by_id = getattr(ib, "cancelOrderById", None)
                if callable(cancel_by_id):
                    cancel_by_id(adapter_order_id)
                else:
                    trades: list[Any] = []
                    open_trades = getattr(ib, "openTrades", None)
                    if callable(open_trades):
                        trades = list(open_trades() or [])
                    matching_order = next(
                        (
                            getattr(trade, "order", None)
                            for trade in trades
                            if str(getattr(getattr(trade, "order", None), "orderId", ""))
                            == str(adapter_order_id)
                        ),
                        None,
                    )
                    if matching_order is None:
                        request_open_orders = getattr(ib, "reqOpenOrdersAsync", None)
                        if callable(request_open_orders):
                            trades = list(await request_open_orders() or [])
                            matching_order = next(
                                (
                                    getattr(trade, "order", None)
                                    for trade in trades
                                    if str(
                                        getattr(
                                            getattr(trade, "order", None),
                                            "orderId",
                                            "",
                                        )
                                    )
                                    == str(adapter_order_id)
                                ),
                                None,
                            )
                    if matching_order is None:
                        raise IBOrderError(
                            f"Open IB order {adapter_order_id} was not found for cancellation"
                        )
                    ib.cancelOrder(matching_order)
                request_open_orders = getattr(ib, "reqOpenOrdersAsync", None)
                deadline = time.monotonic() + 8.0
                absent_since: float | None = None
                last_status = ""
                while True:
                    await asyncio.sleep(0.2)
                    if callable(request_open_orders):
                        open_trades = list(await request_open_orders() or [])
                    else:
                        current_open_trades = getattr(ib, "openTrades", None)
                        if not callable(current_open_trades):
                            raise IBOrderError(
                                "IB client cannot verify cancellation because open orders are unavailable"
                            )
                        open_trades = list(current_open_trades() or [])
                    matching_trade = next(
                        (
                            trade
                            for trade in open_trades
                            if str(getattr(getattr(trade, "order", None), "orderId", ""))
                            == str(adapter_order_id)
                        ),
                        None,
                    )
                    if matching_trade is None:
                        if absent_since is None:
                            absent_since = time.monotonic()
                        if time.monotonic() - absent_since >= 2.0:
                            return
                        if time.monotonic() >= deadline:
                            raise IBOrderError(
                                f"IB order {adapter_order_id} cancellation did not remain absent"
                            )
                        continue
                    absent_since = None
                    last_status = str(
                        getattr(getattr(matching_trade, "orderStatus", None), "status", "")
                        or ""
                    )
                    if last_status.strip().lower() == "filled":
                        raise IBOrderError(
                            f"IB order {adapter_order_id} filled before cancellation was confirmed"
                        )
                    if time.monotonic() >= deadline:
                        error_summary = "; ".join(
                            f"{code or 'unknown'}:{message}" for code, message in captured
                        )
                        detail = f"; IB errors: {error_summary}" if error_summary else ""
                        raise IBOrderError(
                            f"IB order {adapter_order_id} cancellation was not confirmed "
                            f"(last_status={last_status or 'unknown'}){detail}"
                        )
            except Exception as exc:  # pragma: no cover - runtime failure
                if isinstance(exc, IBOrderError):
                    raise
                raise IBOrderError("Failed to cancel order") from exc
            finally:
                if handler is not None and error_event is not None:
                    try:
                        error_event -= handler  # type: ignore[operator]
                    except Exception:
                        pass
                if captured:
                    try:
                        LOGGER.warning(
                            "IB cancellation emitted error events",
                            extra={
                                "event": "ib.cancel.error",
                                "payload": {
                                    "order_id": adapter_order_id,
                                    "errors": [{"code": code, "message": msg} for code, msg in captured],
                                },
                            },
                        )
                    except Exception:
                        pass

        await self._run_with_ib_async(cancel)

    async def request_open_orders(self) -> list[TradeUpdate]:
        """Retrieve open orders from IB and convert them into trade updates."""

        async def fetch(ib: IB) -> list[TradeUpdate]:
            method = getattr(ib, "reqOpenOrdersAsync", None)
            if not callable(method):
                LOGGER.debug("IB client does not expose reqOpenOrdersAsync; skipping poll")
                return []
            try:
                trades = await method()
            except Exception as exc:  # pragma: no cover - network failure
                LOGGER.exception("Failed to request open orders from IB", exc_info=exc)
                return []

            updates: list[TradeUpdate] = []
            for trade in trades or []:
                try:
                    update = _trade_to_update(trade, "poll.open_orders")
                except Exception:
                    # Fallback to defensive conversion if helper fails unexpectedly
                    status = getattr(trade, "orderStatus", None)
                    order = getattr(trade, "order", None)
                    commission_report = getattr(trade, "commissionReport", None)
                    commission = _optional_float(_safe_get(status, "commission"))
                    realized_pnl = _optional_float(_get_pnl_value(status, "realized"))
                    unrealized_pnl = _optional_float(_get_pnl_value(status, "unrealized"))
                    if _commission_report_has_effective_values(commission_report):
                        commission = _add_optional_numbers(
                            commission, _optional_float(_safe_get(commission_report, "commission"))
                        )
                        realized_pnl = _add_optional_numbers(
                            realized_pnl,
                            _effective_report_pnl_value(commission_report, "realized"),
                        )
                        unrealized_pnl = _add_optional_numbers(
                            unrealized_pnl,
                            _effective_report_pnl_value(commission_report, "unrealized"),
                        )
                    update = TradeUpdate(
                        adapter_id=IBKR_ADAPTER_ID,
                        adapter_order_id=_safe_get(order, "orderId") or _safe_get(status, "orderId"),
                        adapter_order_ref=(str(_safe_get(order, "permId")) if _safe_get(order, "permId") not in (None, "") else None),
                        adapter_metadata={"schemaVersion": 1, "native": {"openClose": str(_safe_get(order, "openClose") or "")}},
                        status=getattr(status, "status", None),
                        filled=_optional_float(getattr(status, "filled", None)),
                        remaining=_optional_float(getattr(status, "remaining", None)),
                        avg_fill_price=_optional_float(getattr(status, "avgFillPrice", None)),
                        last_fill_price=_optional_float(getattr(status, "lastFillPrice", None)),
                        commission=commission,
                        realized_pnl=realized_pnl,
                        unrealized_pnl=unrealized_pnl,
                        event_time=datetime.now(timezone.utc),
                        message={
                            "source": "poll.open_orders",
                            "status": _order_status_to_dict(status),
                            "order": _order_to_dict(order),
                            "contract": _contract_to_dict(getattr(trade, "contract", None)),
                            "commissionReport": _commission_report_to_dict(commission_report),
                        },
                    )
                updates.append(update)
            return updates

        return await self._run_with_ib_async(fetch)

    async def request_executions(
        self, since: datetime | str | None = None
    ) -> list[TradeUpdate]:
        """Request the latest executions from IB."""

        async def fetch(ib: IB) -> list[TradeUpdate]:
            method = getattr(ib, "reqExecutionsAsync", None)
            if not callable(method):
                LOGGER.debug("IB client does not expose reqExecutionsAsync; skipping poll")
                return []

            execution_filter: ExecutionFilter | None = None
            if since is not None:
                execution_filter = ExecutionFilter()
                execution_filter.time = _coerce_ib_execution_filter_time(since)
                account = getattr(self._settings, "account", None)
                if account:
                    execution_filter.acctCode = account

            try:
                if execution_filter is not None:
                    LOGGER.debug(
                        "IB executions request timestamp prepared",
                        extra={
                            "event": "ib.request.executions.timestamp",
                            "raw_since": str(since),
                            "filter_time": execution_filter.time,
                            "account": getattr(execution_filter, "acctCode", None),
                        },
                    )
                if execution_filter is None:
                    executions = await method()  # type: ignore[misc]
                else:
                    executions = await method(execution_filter)  # type: ignore[misc]
            except TypeError:
                try:
                    if execution_filter is None:
                        executions = await method(None)  # type: ignore[misc]
                    else:
                        executions = await method()  # type: ignore[misc]
                except Exception as exc:  # pragma: no cover - unexpected signature
                    LOGGER.exception("Failed to request executions from IB", exc_info=exc)
                    return []
            except Exception as exc:  # pragma: no cover - network failure
                LOGGER.exception("Failed to request executions from IB", exc_info=exc)
                return []

            updates: list[TradeUpdate] = []
            for entry in executions or []:
                contract = getattr(entry, "contract", None)
                execution = getattr(entry, "execution", entry)
                commission_report = getattr(entry, "commissionReport", None) or _safe_get(
                    execution, "commissionReport"
                )
                order_id = _optional_int(_safe_get(execution, "orderId"))
                if not order_id:
                    fallback_perm = _optional_int(_safe_get(execution, "permId"))
                    order_id = fallback_perm
                if order_id is None:
                    continue
                last_price = _optional_float(_safe_get(execution, "price"))
                last_quantity = _optional_float(_safe_get(execution, "shares"))
                avg_price = _optional_float(_safe_get(execution, "avgPrice"))
                cum_quantity = _optional_float(_safe_get(execution, "cumQty"))
                commission = _optional_float(_safe_get(execution, "commission"))
                realized_pnl = _optional_float(_get_pnl_value(execution, "realized"))
                unrealized_pnl = _optional_float(_get_pnl_value(execution, "unrealized"))
                if _commission_report_has_effective_values(commission_report):
                    commission = _add_optional_numbers(
                        commission, _optional_float(_safe_get(commission_report, "commission"))
                    )
                    realized_pnl = _add_optional_numbers(
                        realized_pnl, _effective_report_pnl_value(commission_report, "realized")
                    )
                    unrealized_pnl = _add_optional_numbers(
                        unrealized_pnl,
                        _effective_report_pnl_value(commission_report, "unrealized"),
                    )
                exec_time = _safe_get(execution, "time")
                event_time = None
                if exec_time:
                    with suppress(Exception):
                        event_time = _parse_ib_datetime(exec_time)

                updates.append(
                    TradeUpdate(
                        adapter_id=IBKR_ADAPTER_ID,
                        adapter_order_id=order_id,
                        adapter_order_ref=(str(_safe_get(execution, "permId")) if _safe_get(execution, "permId") not in (None, "") else None),
                        adapter_execution_id=(str(_safe_get(execution, "execId")) if _safe_get(execution, "execId") not in (None, "") else None),
                        adapter_metadata={"schemaVersion": 1, "native": {}},
                        filled=cum_quantity,
                        avg_fill_price=avg_price,
                        last_fill_price=last_price,
                        last_fill_quantity=last_quantity,
                        commission=commission,
                        realized_pnl=realized_pnl,
                        unrealized_pnl=unrealized_pnl,
                        event_time=event_time or datetime.now(timezone.utc),
                        message={
                            "source": "poll.executions",
                            "contract": _contract_to_dict(contract),
                            "execution": _execution_to_dict(execution),
                            "commissionReport": _commission_report_to_dict(commission_report),
                        },
                    )
                )
            return updates

        return await self._run_with_ib_async(fetch)

    async def request_completed_orders(self) -> list[TradeUpdate]:
        """Retrieve completed orders from IB and convert them into trade updates."""

        async def fetch(ib: IB) -> list[TradeUpdate]:
            method = getattr(ib, "reqCompletedOrdersAsync", None)
            if not callable(method):
                LOGGER.debug("IB client does not expose reqCompletedOrdersAsync; skipping poll")
                return []

            try:
                trades = await method(apiOnly=False)
            except TypeError:
                try:
                    trades = await method()
                except Exception as exc:  # pragma: no cover - network failure
                    LOGGER.exception("Failed to request completed orders from IB", exc_info=exc)
                    return []
            except Exception as exc:  # pragma: no cover - network failure
                LOGGER.exception("Failed to request completed orders from IB", exc_info=exc)
                return []

            updates: list[TradeUpdate] = []
            for trade in trades or []:
                try:
                    updates.append(_trade_to_update(trade, "poll.completed_orders"))
                except Exception:  # pragma: no cover - defensive conversion
                    LOGGER.exception("Failed to convert completed order trade update")
            return updates

        return await self._run_with_ib_async(fetch)

    async def get_historical_data(
        self,
        contract: Any,
        *,
        end_datetime: datetime | str | None = None,
        duration: str = "1 D",
        bar_size: str = "1 min",
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ) -> list[HistoricalBar]:
        """Request historical bar data."""

        contract = _require_contract(contract)

        end_value = _coerce_ib_datetime(end_datetime)

        async def fetch(ib: IB) -> list[HistoricalBar]:
            timeout = getattr(self._settings, "historical_timeout", 15.0)
            error_event = getattr(ib, "errorEvent", None)
            captured_pacing: list[tuple[int | None, str]] = []
            captured_invalid_datetime: list[tuple[int | None, str]] = []
            error_handler: Callable[..., None] | None = None

            if error_event is not None:
                def _capture_error(*args: Any) -> None:
                    if len(args) >= 4:
                        _, code, message, _ = args[:4]
                    elif len(args) >= 2:
                        code, message = args[:2]
                    else:
                        return
                    try:
                        code_int = int(code) if code is not None else None
                    except Exception:
                        code_int = None
                    if _is_historical_pacing_error(code_int, message):
                        captured_pacing.append((code_int, str(message)))
                    if _is_invalid_historical_datetime_error(code_int, message):
                        captured_invalid_datetime.append((code_int, str(message)))

                try:
                    error_event += _capture_error  # type: ignore[operator]
                    error_handler = _capture_error
                except Exception:  # pragma: no cover - defensive; errorEvent may be immutable
                    error_handler = None

            try:
                LOGGER.debug(
                    "IB historical bars request timestamp prepared",
                    extra={
                        "event": "ib.request.historical_bars.timestamp",
                        "end_datetime": end_value,
                        "duration": duration,
                        "bar_size": bar_size,
                        "what_to_show": what_to_show,
                        "use_rth": use_rth,
                        "keep_up_to_date": False,
                    },
                )
                request = self._call_async_with_timeout(
                    ib.reqHistoricalDataAsync,
                    contract,
                    endDateTime=end_value,
                    durationStr=duration,
                    barSizeSetting=bar_size,
                    whatToShow=what_to_show,
                    useRTH=use_rth,
                    timeout=timeout,
                )
                bars = await asyncio.wait_for(request, timeout=timeout)
            except Exception as exc:
                if captured_pacing:
                    _, message = captured_pacing[-1]
                    raise IBMarketDataError(
                        f"Historical data request pacing violation: {message}"
                    ) from exc
                if captured_invalid_datetime:
                    _, message = captured_invalid_datetime[-1]
                    raise IBMarketDataError(
                        "Historical data request rejected invalid end_datetime "
                        f"{end_value!r}: {message}"
                    ) from exc
                raise
            finally:
                if error_handler is not None and error_event is not None:
                    with suppress(Exception):
                        error_event -= error_handler  # type: ignore[operator]

            if captured_pacing:
                _, message = captured_pacing[-1]
                raise IBMarketDataError(
                    f"Historical data request pacing violation: {message}"
                )
            if captured_invalid_datetime:
                _, message = captured_invalid_datetime[-1]
                raise IBMarketDataError(
                    "Historical data request rejected invalid end_datetime "
                    f"{end_value!r}: {message}"
                )

            result: list[HistoricalBar] = []
            for bar in bars:
                result.append(_build_historical_bar(bar))
            return result
        timeout_exc: asyncio.TimeoutError | None = None
        for attempt in range(2):
            try:
                return await self._run_with_ib_async(fetch)
            except asyncio.TimeoutError as exc:
                timeout_exc = exc
                if attempt == 1:
                    break
                await asyncio.sleep(0.2 * (attempt + 1))
                continue
        raise IBMarketDataError("Failed to fetch historical bars") from timeout_exc

    async def get_historical_ticks(
        self,
        contract: Any,
        *,
        start_datetime: datetime | str | None = None,
        end_datetime: datetime | str | None = None,
        number_of_ticks: int = 1000,
        what_to_show: str = "MIDPOINT",
        use_rth: bool = False,
        ignore_size: bool = False,
    ) -> list[HistoricalTickBidAsk | HistoricalTickLast]:
        """Request historical tick data for ``contract``."""

        contract = _require_contract(contract)

        start_value = _coerce_ib_datetime(start_datetime)
        end_value = _coerce_ib_datetime(end_datetime)

        async def fetch(ib: IB) -> list[HistoricalTickBidAsk | HistoricalTickLast]:
            timeout = getattr(self._settings, "historical_timeout", 15.0)
            LOGGER.debug(
                "IB historical ticks request timestamp prepared",
                extra={
                    "event": "ib.request.historical_ticks.timestamp",
                    "start_datetime": start_value,
                    "end_datetime": end_value,
                    "number_of_ticks": number_of_ticks,
                    "what_to_show": what_to_show,
                    "use_rth": use_rth,
                },
            )
            request = self._call_async_with_timeout(
                ib.reqHistoricalTicksAsync,  # type: ignore[attr-defined]
                contract,
                start_value,
                end_value,
                number_of_ticks,
                what_to_show,
                useRth=use_rth,
                ignoreSize=ignore_size,
                timeout=timeout,
            )
            ticks = await asyncio.wait_for(request, timeout=timeout)
            result: list[HistoricalTickBidAsk | HistoricalTickLast] = []
            for tick in ticks:
                payload = _build_historical_tick(tick)
                if payload is not None:
                    result.append(payload)
            return result

        try:
            return await self._run_with_ib_async(fetch)
        except Exception as exc:  # pragma: no cover - network failure
            raise IBMarketDataError("Failed to fetch historical ticks") from exc

    async def stream_historical_bars(
        self,
        contract: Any,
        *,
        end_datetime: datetime | str | None = None,
        duration: str = "1 D",
        bar_size: str = "1 min",
        what_to_show: str = "TRADES",
        use_rth: bool = True,
        keep_up_to_date: bool = True,
        emit_history: bool = False,
    ) -> AsyncIterator[HistoricalBar]:
        """Yield historical bars followed by incremental updates."""

        contract = _require_contract(contract)

        queue: asyncio.Queue[HistoricalBar | BaseException] = asyncio.Queue(
            self._settings.market_data_queue_size
        )
        await self.ensure_connected()
        loop = self._loop or asyncio.get_running_loop()

        end_value = "" if keep_up_to_date else _coerce_ib_datetime(end_datetime)

        async def subscribe(ib: IB) -> Any:
            timeout = max(
                5.0,
                float(getattr(self._settings, "historical_timeout", 60.0) or 60.0),
            )
            LOGGER.debug(
                "IB historical stream request timestamp prepared",
                extra={
                    "event": "ib.request.historical_stream.timestamp",
                    "end_datetime": end_value,
                    "duration": duration,
                    "bar_size": bar_size,
                    "what_to_show": what_to_show,
                    "use_rth": use_rth,
                    "keep_up_to_date": keep_up_to_date,
                },
            )
            request = self._call_async_with_timeout(
                ib.reqHistoricalDataAsync,
                contract,
                endDateTime=end_value,
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow=what_to_show,
                useRTH=use_rth,
                keepUpToDate=keep_up_to_date,
                timeout=timeout,
            )
            return await asyncio.wait_for(request, timeout=timeout)

        bars: Any | None = None
        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                bars = await self._run_with_ib_async(subscribe)
                break
            except asyncio.TimeoutError as exc:
                last_exc = exc
                if attempt == 2:
                    raise IBMarketDataError("Failed to subscribe to historical bars") from exc
                try:
                    await self.reconnect(reason="historical_bars_timeout")
                except Exception:
                    LOGGER.debug(
                        "Reconnect attempt after historical bar timeout failed",
                        exc_info=True,
                    )
                await asyncio.sleep(0.2 * (attempt + 1))
                continue
            except Exception as exc:  # pragma: no cover - network failure
                last_exc = exc
                if attempt == 2:
                    raise IBMarketDataError("Failed to subscribe to historical bars") from exc
                await asyncio.sleep(0.2 * (attempt + 1))
                continue

        if bars is None:
            raise IBMarketDataError("Failed to subscribe to historical bars") from last_exc

        initial_bars = list(bars)
        last_stream_bar: HistoricalBar | None = None
        last_emitted_bar_time: datetime | None = None
        if emit_history:
            for bar in initial_bars:
                built_bar = _build_historical_bar(bar)
                last_stream_bar = built_bar
                last_emitted_bar_time = _normalise_utc_datetime(built_bar.time)
                loop.call_soon_threadsafe(_queue_offer, queue, built_bar)
        elif keep_up_to_date and initial_bars:
            # IB returns the latest in-progress/completed bar together with the
            # keepUpToDate subscription. Prefer the newest closed bar so downstream
            # strategy/risk consumers do not treat an open bar as a trigger.
            initial_bar = _latest_closed_historical_bar(initial_bars, bar_size)
            if initial_bar is None and _historical_stream_allows_open_updates(bar_size):
                initial_bar = initial_bars[-1]
            if initial_bar is not None:
                loop.call_soon_threadsafe(
                    _queue_offer,
                    queue,
                    _build_historical_bar(initial_bar),
                )
                last_emitted_bar_time = _normalise_utc_datetime(_build_historical_bar(initial_bar).time)
            last_stream_bar = _build_historical_bar(initial_bars[-1])

        def handle(updated_bars: Any, has_new_bar: bool | None = None) -> None:
            nonlocal last_stream_bar, last_emitted_bar_time
            try:
                if not updated_bars:
                    return
                if has_new_bar is True:
                    latest = _latest_closed_historical_bar(updated_bars, bar_size)
                    if latest is None:
                        latest = updated_bars[-2] if len(updated_bars) >= 2 else updated_bars[-1]
                else:
                    latest = updated_bars[-1]
            except Exception:  # pragma: no cover - defensive
                latest = getattr(bars, "last", None) or getattr(updated_bars, "last", None)
            if latest is None:
                return
            payload = _build_historical_bar(latest)
            if keep_up_to_date and not _historical_stream_allows_open_updates(bar_size):
                payload_time = _normalise_utc_datetime(payload.time)
                previous = last_stream_bar
                previous_time = (
                    _normalise_utc_datetime(previous.time) if previous is not None else None
                )
                if not _historical_bar_is_closed(payload, bar_size):
                    last_stream_bar = payload
                    if (
                        previous is not None
                        and previous_time is not None
                        and payload_time > previous_time
                        and last_emitted_bar_time != previous_time
                    ):
                        last_emitted_bar_time = previous_time
                        loop.call_soon_threadsafe(_queue_offer, queue, previous)
                    return
                last_stream_bar = payload
                if last_emitted_bar_time == payload_time:
                    return
                last_emitted_bar_time = payload_time
            loop.call_soon_threadsafe(
                _queue_offer,
                queue,
                payload,
            )

        bars.updateEvent += handle  # type: ignore[attr-defined]
        req_id = _optional_int(getattr(bars, "reqId", None))
        error_event = getattr(getattr(bars, "ib", None), "errorEvent", None)
        if error_event is None:
            ib_ref = self._ib
            error_event = getattr(ib_ref, "errorEvent", None) if ib_ref is not None else None
        error_handler: Callable[..., None] | None = None

        if error_event is not None and req_id is not None:

            def _handle_stream_error(*args: Any) -> None:
                if len(args) >= 4:
                    raw_req_id, code, message, _misc = args[:4]
                elif len(args) >= 3:
                    raw_req_id, code, message = args[:3]
                else:
                    return
                try:
                    event_req_id = int(raw_req_id)
                    event_code = int(code)
                except (TypeError, ValueError):
                    return
                if event_req_id != req_id:
                    return
                if event_code != _DISCONNECTED_LIVE_UPDATES_ERROR_CODE:
                    return
                loop.call_soon_threadsafe(
                    _queue_offer,
                    queue,
                    IBMarketDataError(
                        f"IB historical bar live updates disconnected for reqId {req_id}: {message}"
                    ),
                )

            try:
                error_event += _handle_stream_error  # type: ignore[operator]
                error_handler = _handle_stream_error
            except Exception:  # pragma: no cover - defensive; errorEvent may be immutable
                LOGGER.exception("Failed to attach historical bar stream error handler")

        async def iterator() -> AsyncIterator[HistoricalBar]:
            try:
                while True:
                    item = await queue.get()
                    if isinstance(item, BaseException):
                        raise item
                    yield item
            finally:
                bars.updateEvent -= handle  # type: ignore[attr-defined]
                bound_error_event = error_event
                bound_error_handler = error_handler
                if bound_error_event is not None and bound_error_handler is not None:
                    with suppress(Exception):
                        bound_error_event -= bound_error_handler  # type: ignore[operator]
                if bars is not None:
                    async def _cancel(
                        ib: IB,
                        *,
                        _cancel_fn: Callable[[Any, Any], None] = _cancel_historical_data,
                        _bars: Any = bars,
                    ) -> None:
                        _cancel_fn(ib, _bars)

                    await self._run_with_ib_async(_cancel)

        return _AsyncIteratorWithReqId(iterator(), req_id)

    async def stream_market_depth(
        self,
        contract: Any,
        *,
        depth: int | None = None,
    ) -> AsyncIterator[DOMSnapshot]:
        """Yield depth-of-market snapshots as they arrive."""

        contract = _require_contract(contract)
        try:
            con_id = getattr(contract, "conId", None)
            if not con_id:
                contract = await self.qualify_contract(contract)
        except Exception as exc:
            raise IBMarketDataError("Failed to qualify contract for market depth") from exc
        request_contract = contract

        exchange_value = getattr(contract, "exchange", None)
        primary_exchange_value = getattr(contract, "primaryExchange", None) or getattr(
            contract, "primary_exchange", None
        )
        normalized_exchange = (
            str(exchange_value).strip() if exchange_value not in (None, "") else None
        )
        normalized_primary = (
            str(primary_exchange_value).strip()
            if primary_exchange_value not in (None, "")
            else None
        )

        is_smart_depth = False
        if (normalized_exchange or "").upper() == "SMART":
            if normalized_primary and normalized_primary.upper() != "SMART":
                request_contract = copy.copy(contract)
                setattr(request_contract, "exchange", normalized_primary)
                normalized_exchange = normalized_primary
            else:
                is_smart_depth = True

        final_exchange = getattr(request_contract, "exchange", None)
        if final_exchange in (None, ""):
            final_exchange = normalized_primary or normalized_exchange

        queue: asyncio.Queue[DOMSnapshot] = asyncio.Queue(self._settings.market_data_queue_size)
        ib = await self.ensure_connected()
        loop = self._loop or asyncio.get_running_loop()
        depth_rows = depth or self._settings.market_data_depth_rows
        contract_info = _contract_to_dict(request_contract)

        LOGGER.debug(
            "Requesting market depth stream from IB",
            extra={
                "contract": contract_info,
                "depth_rows": depth_rows,
                "exchange": final_exchange,
                "isSmartDepth": is_smart_depth,
            },
        )

        exchanges: tuple[dict[str, Any], ...] = ()
        try:
            raw_exchanges = await self._run_with_ib(lambda client: client.reqMktDepthExchanges())
        except Exception:  # pragma: no cover - best effort metadata fetch
            LOGGER.debug("Failed to fetch market depth exchanges metadata", exc_info=True)
        else:
            exchanges = _build_depth_exchanges(raw_exchanges, contract)

        ticker: Any | None = None
        try:
            ticker = await self._run_with_ib(
                lambda client: client.reqMktDepth(
                    request_contract, numRows=depth_rows, isSmartDepth=is_smart_depth
                )
            )
        except Exception as exc:  # pragma: no cover - network failure
            raise IBMarketDataError("Failed to subscribe to market depth") from exc

        def handle(_ticker: Any) -> None:
            snapshot = _build_dom_snapshot(ticker, exchanges)
            loop.call_soon_threadsafe(_queue_offer, queue, snapshot)

        ticker.updateEvent += handle  # type: ignore[attr-defined]

        error_event = getattr(ib, "errorEvent", None)
        error_handler: Callable[..., None] | None = None
        failure_future: asyncio.Future[tuple[int | None, str]] | None = None
        target_req_id = getattr(ticker, "reqId", None)

        if error_event is not None:
            future: asyncio.Future[tuple[int | None, str]] = loop.create_future()

            def _handle_error(req_id: Any, code: Any, message: Any, misc: Any) -> None:
                if future.done():
                    return

                try:
                    code_int = int(code) if code is not None else None
                except Exception:
                    code_int = None

                if (
                    code_int not in _MARKET_DEPTH_PERMISSION_ERROR_CODES
                    and not self._is_competing_live_session_error(code_int, message)
                ):
                    return

                req_match = target_req_id is None
                if target_req_id is not None:
                    try:
                        req_match = int(req_id) == target_req_id
                    except Exception:
                        req_match = False

                if not req_match:
                    return

                future.set_result((code_int, str(message)))

            try:
                error_event += _handle_error  # type: ignore[operator]
            except Exception:  # pragma: no cover - defensive; errorEvent may be immutable
                error_handler = None
            else:
                error_handler = _handle_error
                failure_future = future

        async def iterator() -> AsyncIterator[DOMSnapshot]:
            try:
                while True:
                    if failure_future is None:
                        yield await queue.get()
                        continue

                    queue_task = asyncio.create_task(queue.get())
                    try:
                        done, _ = await asyncio.wait(
                            {queue_task, failure_future},
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    except asyncio.CancelledError:
                        queue_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await queue_task
                        raise

                    if failure_future in done:
                        queue_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await queue_task

                        if (
                            failure_future.cancelled()
                        ):  # pragma: no cover - defensive cleanup path
                            raise asyncio.CancelledError()

                        code, error_message = failure_future.result()
                        log_method = LOGGER.error
                        if code in _MARKET_DEPTH_PERMISSION_ERROR_CODES:
                            log_method = LOGGER.warning

                        log_method(
                            "Market depth subscription failed",
                            extra={
                                "contract": contract_info,
                                "req_id": target_req_id,
                                "error_code": code,
                                "error_message": error_message,
                            },
                        )
                        error_code_display = code if code is not None else "unknown"
                        raise IBMarketDataError(
                            f"Market depth subscription failed: Error {error_code_display}: {error_message}"
                        )

                    yield queue_task.result()
            finally:
                ticker.updateEvent -= handle  # type: ignore[attr-defined]
                # Avoid augmented assignment on a free variable which causes UnboundLocalError
                if error_handler is not None and error_event is not None:
                    event = error_event
                    with suppress(Exception):
                        event -= error_handler  # type: ignore[operator]

                if failure_future is not None and not failure_future.done():
                    failure_future.cancel()

                def _cancel_depth(
                    client: Any,
                    *,
                    _cancel_fn: Callable[[Any, Any, Any], None] = _cancel_market_depth,
                    _ticker: Any = ticker,
                    _contract: Any = request_contract,
                ) -> None:
                    _cancel_fn(client, _ticker, _contract)

                await self._run_with_ib(_cancel_depth)

        return _AsyncIteratorWithReqId(iterator(), getattr(ticker, "reqId", None))

    async def stream_real_time_price(
        self,
        contract: Any,
        *,
        snapshot: bool = False,
    ) -> AsyncIterator[RealTimePrice]:
        """Yield real-time price ticks."""

        contract = _require_contract(contract)
        try:
            con_id = getattr(contract, "conId", None)
            if not con_id:
                contract = await self.qualify_contract(contract)
        except Exception as exc:
            LOGGER.exception(
                "Failed to request real-time price stream from IB",
                extra={
                    "contract": _contract_to_dict(contract),
                    "snapshot": snapshot,
                },
            )
            raise IBMarketDataError("Failed to qualify contract for real-time prices") from exc
        contract_info = _contract_to_dict(contract)

        queue: asyncio.Queue[RealTimePrice] = asyncio.Queue(self._settings.market_data_queue_size)
        ib = await self.ensure_connected()
        loop = self._loop or asyncio.get_running_loop()

        LOGGER.debug(
            "Requesting real-time price stream from IB",
            extra={
                "contract": contract_info,
                "snapshot": snapshot,
            },
        )
        ticker: Any | None = None
        try:
            ticker = await self._run_with_ib(
                lambda client: client.reqMktData(contract, "", snapshot, False)
            )
        except Exception as exc:  # pragma: no cover - network failure
            LOGGER.exception(
                "Failed to request real-time price stream from IB",
                extra={
                    "contract": contract_info,
                    "snapshot": snapshot,
                },
            )
            raise IBMarketDataError("Failed to subscribe to real-time prices") from exc

        reference_ticker = _resolve_reference_ticker(ib, contract, ticker)
        pending_event = getattr(ib, "pendingTickersEvent", None)
        def handle_pending(pending: Any) -> None:
            for candidate in _iter_tickers(pending):
                if _tickers_match(candidate, reference_ticker):
                    loop.call_soon_threadsafe(
                        _queue_offer,
                        queue,
                        _build_realtime_price(candidate),
                    )
                    break

        def handle_update(_ticker: Any) -> None:
            loop.call_soon_threadsafe(
                _queue_offer,
                queue,
                _build_realtime_price(_ticker),
            )

        use_pending = pending_event is not None
        if use_pending:
            pending_event += handle_pending  # type: ignore[attr-defined]
        else:  # pragma: no cover - fallback when pending event unavailable
            LOGGER.debug("pendingTickersEvent unavailable; falling back to per-ticker updates")
            ticker.updateEvent += handle_update  # type: ignore[attr-defined]

        error_event = getattr(ib, "errorEvent", None)
        error_handler: Callable[..., None] | None = None
        failure_future: asyncio.Future[tuple[int | None, str]] | None = None
        target_req_id = getattr(ticker, "reqId", None)

        if error_event is not None:
            future: asyncio.Future[tuple[int | None, str]] = loop.create_future()

            def _handle_error(req_id: Any, code: Any, message: Any, misc: Any) -> None:
                if future.done():
                    return
                try:
                    code_int = int(code) if code is not None else None
                except Exception:
                    code_int = None
                if not self._is_competing_live_session_error(code_int, message):
                    return
                req_match = target_req_id is None
                if target_req_id is not None:
                    try:
                        req_match = int(req_id) == target_req_id
                    except Exception:
                        req_match = False
                if not req_match:
                    return
                future.set_result((code_int, str(message)))

            try:
                error_event += _handle_error  # type: ignore[attr-defined]
            except Exception:
                error_handler = None
            else:
                error_handler = _handle_error
                failure_future = future

        async def iterator() -> AsyncIterator[RealTimePrice]:
            try:
                while True:
                    queue_task = asyncio.create_task(queue.get())
                    pending: set[asyncio.Task[Any] | asyncio.Future[Any]] = {
                        queue_task
                    }
                    if failure_future is not None:
                        pending.add(failure_future)
                    try:
                        done, unfinished = await asyncio.wait(
                            pending,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    except asyncio.CancelledError:
                        queue_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await queue_task
                        raise
                    for task in unfinished:
                        task.cancel()
                    for task in unfinished:
                        if isinstance(task, asyncio.Task):
                            with suppress(asyncio.CancelledError):
                                await task
                    if failure_future is not None and failure_future in done:
                        code, message = failure_future.result()
                        raise IBMarketDataError(
                            f"Real-time market data request rejected by IB (code {code}): {message}"
                        )
                    result = next(iter(done))
                    yield result.result()
            finally:
                # Avoid augmented assignment on a free variable which causes UnboundLocalError
                if use_pending:
                    event = pending_event
                    if event is not None:
                        event -= handle_pending  # type: ignore[attr-defined]
                else:  # pragma: no cover - fallback when pending event unavailable
                    ticker.updateEvent -= handle_update  # type: ignore[attr-defined]
                if error_handler is not None and error_event is not None:
                    event = error_event
                    with suppress(Exception):
                        event -= error_handler  # type: ignore[attr-defined]
                await self._run_with_ib(
                    lambda client: _cancel_market_data(client, ticker, contract)
                )

        return _AsyncIteratorWithReqId(iterator(), getattr(ticker, "reqId", None))

    async def stream_tick_by_tick_data(
        self,
        contract: Any,
        *,
        tick_type: str = "BidAsk",
        number_of_ticks: int = 0,
        ignore_size: bool = False,
    ) -> AsyncIterator[TickByTickBidAsk | TickByTickLast | TickByTickMidPoint]:
        """Yield tick-by-tick updates for ``contract``."""

        contract = _require_contract(contract)

        queue: asyncio.Queue[
            TickByTickBidAsk | TickByTickLast | TickByTickMidPoint
        ] = asyncio.Queue(self._settings.market_data_queue_size)
        ib = await self.ensure_connected()
        loop = self._loop or asyncio.get_running_loop()

        ticker: Any | None = None
        try:
            ticker = await self._run_with_ib(
                lambda client: client.reqTickByTickData(
                    contract,
                    tick_type,
                    number_of_ticks,
                    ignore_size,
                )
            )
        except Exception as exc:  # pragma: no cover - network failure
            raise IBMarketDataError("Failed to subscribe to tick-by-tick data") from exc

        reference_ticker = _resolve_reference_ticker(ib, contract, ticker)
        pending_event = getattr(ib, "pendingTickersEvent", None)

        def emit_from(source: Any) -> None:
            for tick in getattr(source, "tickByTicks", []) or []:
                payload = _build_tick_by_tick(tick)
                if payload is not None:
                    loop.call_soon_threadsafe(_queue_offer, queue, payload)

        def handle_update(_ticker: Any) -> None:
            emit_from(_ticker)

        handle_pending = None
        if pending_event is not None:

            def _handle_pending(pending: Any) -> None:
                for candidate in _iter_tickers(pending):
                    if _tickers_match(candidate, reference_ticker):
                        emit_from(candidate)
                        break

            handle_pending = _handle_pending
            pending_event += handle_pending  # type: ignore[attr-defined]

        ticker.updateEvent += handle_update  # type: ignore[attr-defined]

        try:
            while True:
                yield await queue.get()
        finally:
            ticker.updateEvent -= handle_update  # type: ignore[attr-defined]
            if handle_pending is not None and pending_event is not None:
                pending_event -= handle_pending  # type: ignore[attr-defined]
            await self._run_with_ib(
                lambda client: _cancel_tick_by_tick(client, ticker, contract, tick_type)
            )

    async def qualify_contract(self, contract: Any) -> Any:
        """Populate missing identifiers by qualifying a contract with IB."""

        async def qualify(ib: IB) -> Any:
            qualified = await ib.qualifyContractsAsync(contract)
            if not qualified:
                raise IBMarketDataError("Contract qualification returned no results")
            return qualified[0]

        try:
            return await self._run_with_ib_async(qualify)
        except IBMarketDataError:
            raise
        except IBConnectionError as exc:
            raise IBMarketDataError("Failed to qualify contract: IB connection lost") from exc
        except Exception as exc:  # pragma: no cover - network failure
            raise IBMarketDataError("Failed to qualify contract") from exc

    async def _submit_order(self, contract: Any, order: Any) -> OrderResult:
        try:
            return await self._run_with_ib_async(
                lambda ib: self._place_order_async(ib, contract, order)
            )
        except ValueError as exc:
            raise IBOrderError(str(exc)) from exc
        except Exception as exc:  # pragma: no cover - runtime failure
            raise IBOrderError("Failed to submit order") from exc

    async def _place_order_async(self, ib: IB, contract: Any, order: Any) -> OrderResult:
        place_order_async = getattr(ib, "placeOrderAsync", None)
        if callable(place_order_async):
            trade = await place_order_async(contract, order)
        else:
            trade = ib.placeOrder(contract, order)

        order_status_event = None
        trade_listener: Callable[..., None] | None = None
        if self._trade_update_handler is not None and trade is not None:
            order_status_event = getattr(trade, "orderStatusEvent", None)

            if order_status_event is not None:

                def _handle_trade_status(*args: Any) -> None:
                    candidate = args[0] if args else trade
                    self._emit_trade_snapshot(candidate, "trade.orderStatusEvent")

                try:
                    order_status_event += _handle_trade_status  # type: ignore[operator]
                except Exception:  # pragma: no cover - defensive; event may be immutable
                    LOGGER.exception("Failed to attach trade orderStatusEvent handler")
                else:
                    trade_listener = _handle_trade_status

        timeout_seconds = 2.0
        deadline = time.monotonic() + timeout_seconds
        try:
            while trade is not None and not _has_trade_submission_ack(trade, order):
                wait_async = getattr(ib, "waitOnUpdateAsync", None)
                try:
                    if callable(wait_async):
                        await wait_async()
                    else:
                        await self._await_ib_update(ib, deadline)
                except asyncio.CancelledError:
                    raise
                except Exception:  # pragma: no cover - defensive wait guard
                    LOGGER.warning("IB trade wait loop interrupted", exc_info=True)
                    break

                if time.monotonic() >= deadline:
                    break
        finally:
            if trade_listener is not None and order_status_event is not None:
                with suppress(Exception):  # pragma: no cover - defensive detach
                    order_status_event -= trade_listener  # type: ignore[operator]

        status = getattr(trade, "orderStatus", None)
        order_obj = getattr(trade, "order", order)
        if self._trade_update_handler is not None:
            self._emit_trade_snapshot(trade, "trade.placeOrder")
        perm_id_raw = getattr(order_obj, "permId", None)
        perm_id = str(perm_id_raw) if perm_id_raw not in (None, "") else None
        return OrderResult(
            order_id=int(getattr(order_obj, "orderId", 0) or 0),
            status=getattr(status, "status", ""),
            filled=float(getattr(status, "filled", 0.0) or 0.0) if status else 0.0,
            remaining=float(getattr(status, "remaining", 0.0) or 0.0) if status else 0.0,
            avg_fill_price=_optional_float(getattr(status, "avgFillPrice", None)) if status else None,
            contract=_contract_to_dict(getattr(trade, "contract", contract)),
            perm_id=perm_id,
        )

    async def _await_ib_update(self, ib: IB, deadline: float) -> None:
        remaining = max(0.0, deadline - time.monotonic())
        if remaining <= 0.0:
            await asyncio.sleep(0)
            return

        update_event = getattr(ib, "updateEvent", None)
        if update_event is None or not hasattr(update_event, "__await__"):
            await asyncio.sleep(min(remaining, 0.1))
            return

        timeout = remaining if math.isfinite(remaining) else None
        try:
            if timeout and timeout > 0.0:
                await asyncio.wait_for(update_event, timeout=timeout)
            else:
                await update_event
        except asyncio.TimeoutError:
            # Swallow timeout to let the caller enforce the deadline.
            pass

    def _install_order_listeners(self, ib: IB) -> None:
        if self._trade_update_handler is None:
            return

        if self._order_status_listener is None:
            order_status_event = getattr(ib, "orderStatusEvent", None)
            if order_status_event is not None:

                def _handle_order_status(*args: Any) -> None:
                    self._on_order_status_event(*args)

                try:
                    order_status_event += _handle_order_status  # type: ignore[operator]
                except Exception:  # pragma: no cover - event binding failures
                    LOGGER.exception("Failed to attach orderStatusEvent handler")
                else:
                    self._order_status_listener = _handle_order_status
        if self._order_error_listener is None:
            error_event = getattr(ib, "errorEvent", None)
            if error_event is not None:

                def _handle_order_error(req_id: Any, code: Any, message: Any, misc: Any) -> None:
                    try:
                        code_int = int(code) if code is not None else None
                    except Exception:
                        code_int = None
                    if code_int not in _ORDER_REJECTION_ERROR_CODES:
                        return
                    try:
                        order_id = int(req_id) if req_id is not None else None
                    except Exception:
                        order_id = None
                    if order_id in (None, 0, -1):
                        return

                    reason = str(message).strip()
                    if misc not in (None, "", {}):
                        reason = f"{reason} ({misc})" if reason else str(misc).strip()
                    update = TradeUpdate(
                        adapter_id=IBKR_ADAPTER_ID,
                        adapter_order_id=order_id,
                        adapter_metadata={"schemaVersion": 1, "native": {}},
                        status="Rejected",
                        rejection_reason=reason or None,
                        event_time=datetime.now(timezone.utc),
                        message={
                            "source": "errorEvent",
                            "error": {
                                "code": code_int,
                                "message": str(message),
                                "misc": misc,
                            },
                        },
                    )
                    self._dispatch_trade_update(update)

                try:
                    error_event += _handle_order_error  # type: ignore[operator]
                except Exception:  # pragma: no cover - event binding failures
                    LOGGER.exception("Failed to attach errorEvent handler for order errors")
                else:
                    self._order_error_listener = _handle_order_error

        if self._exec_details_listener is None:
            exec_details_event = getattr(ib, "execDetailsEvent", None)
            if exec_details_event is not None:

                def _handle_exec_details(*args: Any) -> None:
                    self._on_exec_details_event(*args)

                try:
                    exec_details_event += _handle_exec_details  # type: ignore[operator]
                except Exception:  # pragma: no cover - event binding failures
                    LOGGER.exception("Failed to attach execDetailsEvent handler")
                else:
                    self._exec_details_listener = _handle_exec_details

        if self._commission_report_listener is None:
            commission_event = getattr(ib, "commissionReportEvent", None)
            if commission_event is not None:

                def _handle_commission_report(*args: Any) -> None:
                    self._on_commission_report_event(*args)

                try:
                    commission_event += _handle_commission_report  # type: ignore[operator]
                except Exception:  # pragma: no cover - event binding failures
                    LOGGER.exception("Failed to attach commissionReportEvent handler")
                else:
                    self._commission_report_listener = _handle_commission_report

    def _remove_order_listeners(self, ib: IB) -> None:
        if self._order_status_listener is not None:
            order_status_event = getattr(ib, "orderStatusEvent", None)
            if order_status_event is not None:
                with suppress(Exception):
                    order_status_event -= self._order_status_listener  # type: ignore[operator]
            self._order_status_listener = None
        if self._order_error_listener is not None:
            error_event = getattr(ib, "errorEvent", None)
            if error_event is not None:
                with suppress(Exception):
                    error_event -= self._order_error_listener  # type: ignore[operator]
            self._order_error_listener = None

        if self._exec_details_listener is not None:
            exec_details_event = getattr(ib, "execDetailsEvent", None)
            if exec_details_event is not None:
                with suppress(Exception):
                    exec_details_event -= self._exec_details_listener  # type: ignore[operator]
            self._exec_details_listener = None

        if self._commission_report_listener is not None:
            commission_event = getattr(ib, "commissionReportEvent", None)
            if commission_event is not None:
                with suppress(Exception):
                    commission_event -= self._commission_report_listener  # type: ignore[operator]
            self._commission_report_listener = None

    def _install_position_listeners(self, ib: IB) -> None:
        if self._position_update_handler is None:
            return

        if self._position_listener is None:
            position_event = getattr(ib, "positionEvent", None)
            if position_event is not None:

                def _handle_position_event(*_args: Any) -> None:
                    self._emit_position_snapshot(ib, "positionEvent")

                try:
                    position_event += _handle_position_event  # type: ignore[operator]
                except Exception:  # pragma: no cover - event binding failures
                    LOGGER.exception("Failed to attach positionEvent handler")
                else:
                    self._position_listener = _handle_position_event

        req_positions = getattr(ib, "reqPositions", None)
        if callable(req_positions):
            with suppress(Exception):
                req_positions()
        self._schedule_position_snapshot_emit(ib, "position_listener_install", delay=0.0)
        self._schedule_position_snapshot_emit(ib, "position_listener_install", delay=0.5)

    def _remove_position_listeners(self, ib: IB) -> None:
        if self._position_listener is not None:
            position_event = getattr(ib, "positionEvent", None)
            if position_event is not None:
                with suppress(Exception):
                    position_event -= self._position_listener  # type: ignore[operator]
            self._position_listener = None

        cancel_positions = getattr(ib, "cancelPositions", None)
        if callable(cancel_positions):
            with suppress(Exception):
                cancel_positions()

    def _install_account_listeners(self, ib: IB) -> None:
        if self._account_update_handler is None:
            return
        if self._account_value_listener is None:
            account_value_event = getattr(ib, "accountValueEvent", None)
            if account_value_event is not None:

                def _handle_account_value_event(*args: Any) -> None:
                    item = self._account_summary_item_from_event(*args)
                    if item is not None:
                        self._dispatch_account_summary([item])

                try:
                    account_value_event += _handle_account_value_event  # type: ignore[operator]
                except Exception:  # pragma: no cover - event binding failures
                    LOGGER.exception("Failed to attach accountValueEvent handler")
                else:
                    self._account_value_listener = _handle_account_value_event

        req_account_updates = getattr(ib, "reqAccountUpdates", None)
        if callable(req_account_updates) and not self._account_updates_subscribed:
            account = None
            with suppress(Exception):
                accounts = ib.managedAccounts()
                account = accounts[0] if accounts else None
            with suppress(Exception):
                req_account_updates(True, account or "")
                self._account_updates_subscribed = True

    def _remove_account_listeners(self, ib: IB) -> None:
        if self._account_value_listener is not None:
            account_value_event = getattr(ib, "accountValueEvent", None)
            if account_value_event is not None:
                with suppress(Exception):
                    account_value_event -= self._account_value_listener  # type: ignore[operator]
            self._account_value_listener = None

        req_account_updates = getattr(ib, "reqAccountUpdates", None)
        if callable(req_account_updates):
            account = None
            with suppress(Exception):
                accounts = ib.managedAccounts()
                account = accounts[0] if accounts else None
            with suppress(Exception):
                req_account_updates(False, account or "")
        self._account_updates_subscribed = False

    def _account_summary_item_from_event(self, *args: Any) -> AccountSummaryItem | None:
        source = args[0] if args else None
        account = _safe_get(source, "account")
        tag = _safe_get(source, "tag")
        value = _safe_get(source, "value")
        currency = _safe_get(source, "currency")
        if tag is None and len(args) >= 2:
            tag = args[0]
            value = args[1]
            currency = args[2] if len(args) >= 3 else None
            account = args[3] if len(args) >= 4 else account
        if not tag:
            return None
        return AccountSummaryItem(
            account=str(account or ""),
            tag=str(tag),
            value=str(value if value is not None else ""),
            currency=str(currency) if currency not in (None, "") else None,
        )

    def _dispatch_account_summary(self, summary: list[AccountSummaryItem]) -> None:
        handler = self._account_update_handler
        if handler is None or not summary:
            return
        loop = self._loop
        if loop is None or loop.is_closed():
            with suppress(RuntimeError):
                loop = asyncio.get_running_loop()
        if loop is None or loop.is_closed():
            LOGGER.debug("Dropping live account summary update; event loop unavailable")
            return

        async def _invoke() -> None:
            try:
                await handler(list(summary))
            except Exception:  # pragma: no cover - consumer failures
                LOGGER.exception("Account summary update handler raised an exception")

        asyncio.run_coroutine_threadsafe(_invoke(), loop)

    def _snapshot_positions(self, ib: IB) -> list[PositionItem]:
        positions_accessor = getattr(ib, "positions", None)
        if callable(positions_accessor):
            positions = positions_accessor()
        else:
            positions = positions_accessor or []
        result: list[PositionItem] = []
        for pos in positions or []:
            contract = getattr(pos, "contract", None)
            result.append(
                PositionItem(
                    account=getattr(pos, "account", ""),
                    contract_id=_safe_get(contract, "conId"),
                    symbol=_safe_get(contract, "symbol"),
                    sec_type=_safe_get(contract, "secType"),
                    exchange=_safe_get(contract, "exchange"),
                    currency=_safe_get(contract, "currency"),
                    position=float(getattr(pos, "position", 0.0) or 0.0),
                    avg_cost=float(getattr(pos, "avgCost", 0.0) or 0.0),
                    local_symbol=_safe_get(contract, "localSymbol"),
                    primary_exchange=_safe_get(contract, "primaryExchange"),
                    trading_class=_safe_get(contract, "tradingClass"),
                )
            )
        return result

    def _dispatch_position_snapshot(self, positions: list[PositionItem]) -> None:
        handler = self._position_update_handler
        if handler is None:
            return

        loop = self._loop
        if loop is None or loop.is_closed():
            with suppress(RuntimeError):
                loop = asyncio.get_running_loop()
        if loop is None or loop.is_closed():
            LOGGER.debug("Dropping live position snapshot; event loop unavailable")
            return

        async def _invoke() -> None:
            try:
                await handler(list(positions))
            except Exception:  # pragma: no cover - consumer failures
                LOGGER.exception("Position update handler raised an exception")

        asyncio.run_coroutine_threadsafe(_invoke(), loop)

    def _emit_position_snapshot(self, ib: IB, source: str) -> None:
        if self._position_update_handler is None:
            return
        try:
            positions = self._snapshot_positions(ib)
        except Exception:
            LOGGER.exception("Failed to snapshot live positions from %s", source)
            return
        self._dispatch_position_snapshot(positions)

    def _schedule_position_snapshot_emit(self, ib: IB, source: str, *, delay: float) -> None:
        if self._position_update_handler is None:
            return
        loop = self._loop
        if loop is None or loop.is_closed():
            with suppress(RuntimeError):
                loop = asyncio.get_running_loop()
        if loop is None or loop.is_closed():
            return

        async def _emit_later() -> None:
            if delay > 0:
                await asyncio.sleep(delay)
            self._emit_position_snapshot(ib, source)

        loop.create_task(_emit_later())

    def _dispatch_trade_update(self, update: TradeUpdate) -> None:
        handler = self._trade_update_handler
        if handler is None:
            return

        loop = self._loop
        if loop is None or loop.is_closed():
            with suppress(RuntimeError):
                loop = asyncio.get_running_loop()
        if loop is None or loop.is_closed():
            LOGGER.debug("Dropping trade update; event loop unavailable")
            return

        async def _invoke() -> None:
            try:
                await handler(update)
            except Exception:  # pragma: no cover - consumer failures
                LOGGER.exception("Trade update handler raised an exception")

        asyncio.run_coroutine_threadsafe(_invoke(), loop)

    def _emit_trade_snapshot(self, trade: Any, source: str) -> None:
        if self._trade_update_handler is None or trade is None:
            return
        if hasattr(trade, "orderStatus"):
            update = _trade_to_update(trade, source)
        else:
            status = getattr(trade, "orderStatus", None)
            order = getattr(trade, "order", None)
            if status is None and order is None:
                return
            update = TradeUpdate(
                adapter_id=IBKR_ADAPTER_ID,
                adapter_order_id=_safe_get(order, "orderId") or _safe_get(status, "orderId"),
                adapter_order_ref=(str(_safe_get(order, "permId")) if _safe_get(order, "permId") not in (None, "") else None),
                adapter_metadata={"schemaVersion": 1, "native": {"openClose": str(_safe_get(order, "openClose") or "")}},
                status=getattr(status, "status", None),
                filled=_optional_float(getattr(status, "filled", None)),
                remaining=_optional_float(getattr(status, "remaining", None)),
                avg_fill_price=_optional_float(getattr(status, "avgFillPrice", None)),
                last_fill_price=_optional_float(getattr(status, "lastFillPrice", None)),
                event_time=datetime.now(timezone.utc),
                message={
                    "source": source,
                    "order": _order_to_dict(order),
                    "status": _order_status_to_dict(status),
                    "contract": _contract_to_dict(getattr(trade, "contract", None)),
                    "commissionReport": _commission_report_to_dict(
                        getattr(trade, "commissionReport", None)
                    ),
                },
            )
        self._dispatch_trade_update(update)

    def _on_order_status_event(self, *args: Any) -> None:
        if not args:
            return

        first = args[0]
        if hasattr(first, "orderStatus"):
            update = _trade_to_update(first, "orderStatusEvent")
            self._dispatch_trade_update(update)
            return

        order_id = args[0] if len(args) > 0 else None
        status = args[1] if len(args) > 1 else None
        filled = args[2] if len(args) > 2 else None
        remaining = args[3] if len(args) > 3 else None
        avg_price = args[4] if len(args) > 4 else None
        last_fill = args[7] if len(args) > 7 else None
        client_id = args[8] if len(args) > 8 else None

        message: Mapping[str, Any] = {
            "source": "orderStatusEvent",
            "client_id": client_id,
            "order": {},
            "status": {},
            "contract": {},
            "commissionReport": {},
            "raw": {
                "order_id": order_id,
                "status": status,
                "filled": filled,
                "remaining": remaining,
                "avg_fill_price": avg_price,
                "last_fill_price": last_fill,
            },
        }

        update = TradeUpdate(
            adapter_id=IBKR_ADAPTER_ID,
            adapter_order_id=order_id,
            adapter_metadata={"schemaVersion": 1, "native": {}},
            status=str(status) if status is not None else None,
            filled=_optional_float(filled),
            remaining=_optional_float(remaining),
            avg_fill_price=_optional_float(avg_price),
            last_fill_price=_optional_float(last_fill),
            event_time=datetime.now(timezone.utc),
            message=message,
        )
        self._dispatch_trade_update(update)

    def _on_commission_report_event(self, *args: Any) -> None:
        if not args:
            return

        trade: Any | None = None
        fill: Any | None = None
        standalone_report: Any | None = None

        def _looks_like_commission_report(value: Any) -> bool:
            if value is None:
                return False
            if hasattr(value, "orderStatus") or hasattr(value, "execution"):
                return False
            if _safe_get(value, "commission") is not None:
                return True
            if _get_pnl_value(value, "realized") is not None:
                return True
            if _get_pnl_value(value, "unrealized") is not None:
                return True
            return False

        for arg in args:
            if trade is None and hasattr(arg, "orderStatus"):
                trade = arg
                continue
            if fill is None and (
                hasattr(arg, "execution") or hasattr(arg, "commissionReport")
            ):
                fill = arg
                continue
            if standalone_report is None and _looks_like_commission_report(arg):
                standalone_report = arg

        if trade is None:
            execution = getattr(fill, "execution", fill) if fill is not None else None
            commission_report = standalone_report or getattr(fill, "commissionReport", None)
            update = _build_execution_update(
                execution,
                source="commissionReportEvent",
                fill=fill,
                commission_report=commission_report,
            )
            if update is None:
                return
            self._dispatch_trade_update(update)
            return

        status = getattr(trade, "orderStatus", None)
        commission_report = getattr(trade, "commissionReport", None)
        fill_report = getattr(fill, "commissionReport", None) if fill is not None else None

        if standalone_report is not None:
            if commission_report is None:
                commission_report = standalone_report
            if fill_report is None:
                fill_report = standalone_report

        update = _trade_to_update(
            trade,
            "commissionReportEvent",
            fill=fill,
            commission_report=commission_report,
            fill_commission_report=fill_report,
        )

        def _has_numeric(container: Any, attr: str) -> bool:
            if attr == "realizedPNL":
                value = _get_pnl_value(container, "realized")
            elif attr == "unrealizedPNL":
                value = _get_pnl_value(container, "unrealized")
            else:
                value = _safe_get(container, attr)
            return _optional_float(value) is not None

        if not (
            _has_numeric(status, "commission")
            or _has_numeric(commission_report, "commission")
            or _has_numeric(fill_report, "commission")
        ):
            update.commission = None

        if not (
            _has_numeric(status, "realizedPNL")
            or _has_numeric(commission_report, "realizedPNL")
            or _has_numeric(fill_report, "realizedPNL")
        ):
            update.realized_pnl = None

        if not (
            _has_numeric(status, "unrealizedPNL")
            or _has_numeric(commission_report, "unrealizedPNL")
            or _has_numeric(fill_report, "unrealizedPNL")
        ):
            update.unrealized_pnl = None

        self._dispatch_trade_update(update)

    def _on_exec_details_event(self, *args: Any) -> None:
        if not args:
            return

        first = args[0]
        if hasattr(first, "orderStatus"):
            trade = first
            fill = args[1] if len(args) > 1 else None
            update = _trade_to_update(trade, "execDetailsEvent", fill=fill)
            self._dispatch_trade_update(update)
            return

        req_id = args[0] if len(args) > 0 else None
        contract = args[1] if len(args) > 1 else None
        execution = args[2] if len(args) > 2 else None

        order_id = _safe_get(execution, "orderId")
        last_price = _optional_float(_safe_get(execution, "price"))
        last_quantity = _optional_float(_safe_get(execution, "shares"))
        cum_quantity = _optional_float(_safe_get(execution, "cumQty"))
        avg_price = _optional_float(_safe_get(execution, "avgPrice"))
        event_time: datetime | None = None
        exec_time = _safe_get(execution, "time")
        if exec_time:
            with suppress(Exception):
                event_time = _parse_ib_datetime(exec_time)

        update = _build_execution_update(
            execution,
            source="execDetailsEvent",
            contract=contract,
            req_id=req_id,
        )
        if update is None:
            return
        # IB uses reqId=-1 for unsolicited/live executions.  Non-negative
        # request ids are responses to reqExecutions and therefore replay
        # historical broker state.  Preserve the event for normal matching,
        # but tell consumers not to buffer or warn when that replay has no
        # corresponding local order (for example after an intentional schema
        # reset or for a broker-native order).
        try:
            update.suppress_retry = int(req_id) >= 0
        except (TypeError, ValueError):
            update.suppress_retry = False
        if update.adapter_order_id is None:
            update.adapter_order_id = order_id
        if update.filled is None:
            update.filled = cum_quantity
        if update.avg_fill_price is None:
            update.avg_fill_price = avg_price
        if update.last_fill_price is None:
            update.last_fill_price = last_price
        if update.last_fill_quantity is None:
            update.last_fill_quantity = last_quantity
        if update.event_time is None:
            update.event_time = event_time or datetime.now(timezone.utc)
        self._dispatch_trade_update(update)

    async def _run_with_ib(self, func: Callable[[IB], T]) -> T:
        return await self._execute_with_reconnect_sync(func)

    async def _run_with_ib_async(self, func: Callable[[IB], Awaitable[T]]) -> T:
        return await self._execute_with_reconnect_async(func)

    def _ensure_sync_executor(self) -> ThreadPoolExecutor:
        executor = self._sync_executor
        if executor is None:
            executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="ib-async-client",
            )
            self._sync_executor = executor
        return executor

    async def _run_in_sync_executor(self, func: Callable[[IB], T], ib: IB) -> T:
        loop = asyncio.get_running_loop()
        executor = self._ensure_sync_executor()
        return await loop.run_in_executor(
            executor,
            _run_ib_call_in_executor,
            func,
            ib,
            self,
        )

    async def _shutdown_sync_executor(self) -> None:
        executor = self._sync_executor
        if executor is None:
            return
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(executor, _shutdown_executor_loop, self)
        finally:
            executor.shutdown(wait=True)
            self._sync_executor = None
            self._executor_loop = None

    async def _execute_with_reconnect_sync(self, func: Callable[[IB], T]) -> T:
        last_exc: Exception | None = None
        for attempt in range(5):
            ib = await self.ensure_connected()
            if self._trade_update_handler is not None:
                self._install_order_listeners(ib)
            if self._account_update_handler is not None:
                self._install_account_listeners(ib)
            try:
                if self._call_runner is not None:
                    return await self._call_runner(func, ib)
                return await self._run_in_sync_executor(func, ib)
            except ConnectionError as exc:
                last_exc = exc
                LOGGER.warning(
                    "IB call failed due to connection error; scheduling reconnect (attempt %s)",
                    attempt + 1,
                )
                reason = f"sync_call_failure:{getattr(func, '__name__', 'sync_call')}"
                await self._schedule_reconnect(reason=reason)
        assert last_exc is not None
        raise IBConnectionError("Lost connection to IB gateway") from last_exc

    async def _execute_with_reconnect_async(self, func: Callable[[IB], Awaitable[T]]) -> T:
        last_exc: Exception | None = None
        for attempt in range(5):
            ib = await self.ensure_connected()
            if self._trade_update_handler is not None:
                self._install_order_listeners(ib)
            if self._account_update_handler is not None:
                self._install_account_listeners(ib)
            try:
                # For async functions, we can call them directly since we're already in an async context
                return await func(ib)
            except ConnectionError as exc:
                last_exc = exc
                LOGGER.warning(
                    "IB async call failed; scheduling reconnect (attempt %s, error=%s)",
                    attempt + 1,
                    type(exc).__name__,
                )
                reason = f"async_call_failure:{getattr(func, '__name__', 'async_call')}"
                await self._schedule_reconnect(reason=reason)
            except asyncio.TimeoutError as exc:
                last_exc = exc
                connected = False
                with suppress(Exception):
                    connected = bool(self._connected.is_set() and ib.isConnected())
                if connected:
                    LOGGER.warning(
                        "IB async call timed out while connection is still healthy; not reconnecting (attempt %s, call=%s)",
                        attempt + 1,
                        getattr(func, "__name__", "async_call"),
                    )
                    raise
                LOGGER.warning(
                    "IB async call timed out with disconnected client; scheduling reconnect (attempt %s, call=%s)",
                    attempt + 1,
                    getattr(func, "__name__", "async_call"),
                )
                reason = f"async_call_timeout:{getattr(func, '__name__', 'async_call')}"
                await self._schedule_reconnect(reason=reason)
        assert last_exc is not None
        raise IBConnectionError("Lost connection to IB gateway") from last_exc

    # -------------------- Core listeners & keepalive --------------------
    def _install_core_listeners_once(self, ib: IB) -> None:
        """Install core error listeners once to detect connectivity changes."""
        if self._error_listener is not None:
            return

        error_event = getattr(ib, "errorEvent", None)
        if error_event is None:
            return

        def _handle_error(*args: Any) -> None:
            # Support both (code, msg, obj) and (reqId, code, msg, obj)
            if len(args) >= 4:
                _, code, msg, _ = args[:4]
            elif len(args) >= 2:
                code, msg = args[:2]
            else:
                return
            # Connectivity-related codes:
            # 1100 = connectivity lost
            # 1101 = connectivity restored, data lost and requests should be recovered
            # 1102 = connectivity restored, data maintained
            if code in {1100, 1101}:
                LOGGER.warning("IB disconnected or data lost (code %s): %s", code, msg)
                asyncio.create_task(
                    self._schedule_reconnect(reason=f"ib_error_{code}")
                )
            elif code == _DISCONNECTED_LIVE_UPDATES_ERROR_CODE:
                LOGGER.warning("IB live update request disconnected (code %s): %s", code, msg)
                asyncio.create_task(
                    self._schedule_reconnect(reason=f"ib_error_{code}")
                )
            elif self._is_competing_live_session_error(code, msg):
                LOGGER.error(
                    "IB market data request rejected by IB (code %s): %s; keeping current session connected",
                    code,
                    msg,
                )
            elif code == 1102:
                LOGGER.info("IB connectivity restored with data maintained (code 1102): %s", msg)
                self._notify_connection_state(
                    "restored",
                    reconnect_reason="ib_error_1102",
                    code=code,
                    message=str(msg),
                )
            else:
                LOGGER.debug("IB error %s: %s", code, msg)

        try:
            error_event += _handle_error  # type: ignore[operator]
        except Exception:  # pragma: no cover - event binding failures
            LOGGER.exception("Failed to attach errorEvent handler")
        else:
            self._error_listener = _handle_error

    def _remove_core_listeners(self, ib: IB) -> None:
        error_event = getattr(ib, "errorEvent", None)
        handler = self._error_listener
        if error_event is not None and handler is not None:
            try:
                error_event -= handler  # type: ignore[operator]
            except Exception:  # pragma: no cover - event removal failures
                LOGGER.exception("Failed to detach errorEvent handler")
        self._error_listener = None

    def _start_ping_loop(self) -> None:
        if self._ping_task is not None and not self._ping_task.done():
            return
        self._stop_event.clear()
        self._ping_task = asyncio.create_task(self._ping_loop(), name="ib_keepalive")

    async def _ping_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(self._ping_interval)
            except asyncio.CancelledError:  # pragma: no cover - cooperative cancellation
                return

            if not self._connected.is_set():
                LOGGER.debug("IB client disconnected in ping loop; triggering reconnect")
                await self._schedule_reconnect(reason="ping_loop_retry")
                continue

            ib = self._ib
            if ib is None:
                continue
            try:
                # Wrap the entire ping logic in a timeout to detect hanging connections
                ping_timeout = self._settings.connect_timeout + 5.0
                async with asyncio.timeout(ping_timeout):
                    method = getattr(ib, "reqCurrentTimeAsync", None)
                    if callable(method):
                        await method()
                    else:
                        sync_method = getattr(ib, "reqCurrentTime", None)
                        if callable(sync_method):
                            await self._run_in_sync_executor(lambda client: client.reqCurrentTime(), ib)
                        else:
                            # Fallback: perform a lightweight wait on updates
                            wait_async = getattr(ib, "waitOnUpdateAsync", None)
                            if callable(wait_async):
                                await wait_async(timeout=self._settings.connect_timeout)
                            else:
                                LOGGER.debug("No suitable keepalive method available on IB client")
            except Exception as exc:  # pragma: no cover - network failure
                LOGGER.warning("IB keepalive ping failed: %r; scheduling reconnect", exc)
                await self._schedule_reconnect(reason="keepalive_ping_failed")

    async def _schedule_reconnect(self, *, reason: str | None = None) -> None:
        reconnect_reason = reason or "scheduled_reconnect"

        task = self._reconnect_task
        if task is None or task.done():
            async with self._reconnecting_lock:
                task = self._reconnect_task
                if task is None or task.done():
                    task = asyncio.create_task(
                        self._run_reconnect(reconnect_reason),
                        name="ib_reconnect",
                    )
                    self._reconnect_task = task
        else:
            LOGGER.debug(
                "Reconnect already in progress; coalescing request (reason=%s current_reason=%s)",
                reconnect_reason,
                self._pending_reconnect_reason,
            )

        assert task is not None
        try:
            await asyncio.shield(task)
        except Exception:
            # Keep reconnect failures observable in logs but avoid cascading
            # failures across every caller that coalesced into this task.
            LOGGER.debug("Reconnect task ended with error", exc_info=True)

    async def _run_reconnect(self, reconnect_reason: str) -> None:
        self._pending_reconnect_reason = reconnect_reason
        self._notify_connection_state("reconnecting", reconnect_reason=reconnect_reason)
        LOGGER.debug(
            "Reconnecting to IB gateway: host=%s port=%s client_id=%s reason=%s",
            self._settings.host,
            self._settings.port,
            self._client_id,
            reconnect_reason,
        )

        try:
            # Apply a small cooldown before attempting reconnect to avoid tight loops
            now = time.monotonic()
            delay = max(self._reconnect_cooldown, self._next_reconnect_allowed - now)
            if delay > 0:
                await asyncio.sleep(delay)

            if self._connected.is_set():
                self._connected.clear()
            ib = self._ib
            if ib is not None:
                with suppress(Exception):
                    await self.disconnect(reason=reconnect_reason)

            # Use exponential backoff ensure_connected
            ib2 = await self.ensure_connected()

            # Reinstall listeners and resubscribe
            self._install_core_listeners_once(ib2)
            await self._resubscribe_after_reconnect()
        except Exception as exc:  # pragma: no cover - persistent failure
            LOGGER.error("Reconnect failed: %r", exc)
            raise
        finally:
            current_task = asyncio.current_task()
            if self._reconnect_task is current_task:
                self._reconnect_task = None

    # -------------------- Resubscription registry --------------------
    def add_resub_task(self, coro_factory: Callable[[], Awaitable[None]]) -> None:
        """Register a coroutine factory to be executed after reconnect.

        Example:
            client.add_resub_task(lambda: client.req_mkt_data(contract))
        """
        self._resub_tasks.append(coro_factory)

    async def _resubscribe_after_reconnect(self) -> None:
        for fn in list(self._resub_tasks):
            try:
                await fn()
            except Exception as exc:  # pragma: no cover - subscription failure
                LOGGER.error("Resubscribe task failed: %r", exc)


def _cancel_historical_data(client: Any, bars: Any) -> None:
    cancel = getattr(client, "cancelHistoricalData", None)
    if not callable(cancel) or bars is None:
        return
    
    # According to ib_async docs, cancelHistoricalData requires bars parameter
    try:
        cancel(bars)
        LOGGER.debug(
            "Successfully cancelled historical data subscription",
            extra={"req_id": getattr(bars, "reqId", None)},
        )
    except Exception as e:
        LOGGER.debug(
            "Unable to cancel historical data subscription cleanly",
            extra={
                "req_id": getattr(bars, "reqId", None),
                "error": str(e)
            },
        )


def _cancel_market_data(client: Any, ticker: Any, contract: Any) -> None:
    cancel = getattr(client, "cancelMktData", None)
    if not callable(cancel):
        return

    req_id = getattr(ticker, "reqId", None)
    variants: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    if contract is not None:
        variants.append(((contract,), {}))
    if ticker is not None:
        variants.append(((ticker,), {}))
    if req_id is not None:
        variants.append(((req_id,), {}))

    if not variants:
        LOGGER.debug("No handle available for market data cancellation")
        return

    try:
        success = _call_with_variants(cancel, variants)
    except Exception as exc:  # pragma: no cover - defensive logging
        LOGGER.debug(
            "Unable to cancel market data stream cleanly",
            extra={
                "req_id": req_id,
                "contract": getattr(contract, "conId", None),
                "error": str(exc),
            },
        )
        return

    LOGGER.debug(
        "Market data cancellation attempted",
        extra={
            "req_id": req_id,
            "contract": getattr(contract, "conId", None),
            "success": success,
        },
    )


def _add_optional_numbers(base: float | None, delta: float | None) -> float | None:
    if delta is None:
        return base
    if base is None:
        return delta
    return base + delta


def _commission_report_has_effective_values(report: Any) -> bool:
    if report is None:
        return False
    exec_id = _safe_get(report, "execId") or _safe_get(report, "exec_id")
    if exec_id not in (None, "") and str(exec_id).strip():
        return True
    numeric_values = [
        _safe_get(report, "commission"),
        _get_pnl_value(report, "realized"),
        _get_pnl_value(report, "unrealized"),
    ]
    return any(
        value is not None and abs(value) > 1e-9
        for value in (_optional_float(item) for item in numeric_values)
    )


def _commission_report_has_nonzero_pnl(report: Any) -> bool:
    if report is None:
        return False
    return any(
        value is not None and abs(value) > 1e-9
        for value in (
            _optional_float(_get_pnl_value(report, "realized")),
            _optional_float(_get_pnl_value(report, "unrealized")),
        )
    )


def _effective_report_pnl_value(report: Any, component: str) -> float | None:
    value = _optional_float(_get_pnl_value(report, component))
    if value is None:
        return None
    if abs(value) > 1e-9:
        return value
    if _commission_report_has_nonzero_pnl(report):
        return value
    return None


def _is_trade_done(trade: Any) -> bool:
    if trade is None:
        return True

    is_done = getattr(trade, "isDone", None)
    if callable(is_done):
        try:
            return bool(is_done())
        except Exception:  # pragma: no cover - defensive safeguard
            return False

    status = getattr(trade, "orderStatus", None)
    state = getattr(status, "status", None)
    if isinstance(state, str):
        lowered = state.lower()
        if lowered in {"filled", "inactive", "cancelled", "pendingcancel", "pendinginactive"}:
            return True

    return False


def _has_trade_submission_ack(trade: Any, fallback_order: Any | None = None) -> bool:
    if trade is None:
        return True

    order = getattr(trade, "order", None) or fallback_order
    order_id = getattr(order, "orderId", None) if order is not None else None
    try:
        if order_id is not None and int(order_id) > 0:
            return True
    except Exception:
        pass

    perm_id = getattr(order, "permId", None) if order is not None else None
    if perm_id not in (None, "", 0, "0"):
        return True

    status = getattr(trade, "orderStatus", None)
    state = getattr(status, "status", None)
    if isinstance(state, str) and state.strip():
        return True

    return _is_trade_done(trade)


def _trade_to_update(
    trade: Any,
    source: str,
    *,
    fill: Any | None = None,
    commission_report: Any | None = None,
    fill_commission_report: Any | None = None,
) -> TradeUpdate:
    status = getattr(trade, "orderStatus", None)
    order = getattr(trade, "order", None)
    contract = getattr(trade, "contract", None)
    commission_report_data = (
        commission_report if commission_report is not None else getattr(trade, "commissionReport", None)
    )

    adapter_order_id_raw = _safe_get(order, "orderId") or _safe_get(status, "orderId")
    adapter_order_id_int = _optional_int(adapter_order_id_raw)
    adapter_order_id = adapter_order_id_int if adapter_order_id_int is not None else adapter_order_id_raw

    client_order_id: str | None = None
    client_ref = _safe_get(order, "orderRef") or _safe_get(order, "clientOrderId")
    if client_ref not in (None, ""):
        client_order_id = str(client_ref)

    update = TradeUpdate(
        adapter_id=IBKR_ADAPTER_ID,
        adapter_order_id=adapter_order_id,
        adapter_order_ref=(str(_safe_get(order, "permId")) if _safe_get(order, "permId") not in (None, "") else None),
        adapter_execution_id=(str(_safe_get(getattr(fill, "execution", None), "execId")) if fill is not None and _safe_get(getattr(fill, "execution", None), "execId") not in (None, "") else None),
        adapter_metadata={
            "schemaVersion": 1,
            "native": {"openClose": str(_safe_get(order, "openClose") or "")},
        },
        status=_safe_get(status, "status"),
        filled=_optional_float(_safe_get(status, "filled")),
        remaining=_optional_float(_safe_get(status, "remaining")),
        avg_fill_price=_optional_float(_safe_get(status, "avgFillPrice")),
        last_fill_price=_optional_float(_safe_get(status, "lastFillPrice")),
        last_fill_quantity=_optional_float(
            _safe_get(status, "lastFillQty") or _safe_get(status, "lastFillQuantity")
        ),
        commission=_optional_float(_safe_get(status, "commission")),
        realized_pnl=_optional_float(_get_pnl_value(status, "realized")),
        unrealized_pnl=_optional_float(_get_pnl_value(status, "unrealized")),
        event_time=datetime.now(timezone.utc),
        message=None,
        client_order_id=client_order_id,
    )

    message: dict[str, Any] = {
        "source": source,
        "order": _order_to_dict(order),
        "status": _order_status_to_dict(status),
        "contract": _contract_to_dict(contract),
        "commissionReport": _commission_report_to_dict(commission_report_data),
    }

    perm_id = _safe_get(order, "permId")
    if perm_id not in (None, ""):
        message["order_identifier"] = str(perm_id)

    rejection: dict[str, Any] = {}
    rejection_reasons: list[str] = []
    advanced_error = getattr(trade, "advancedError", None)
    if advanced_error not in (None, ""):
        rejection["advancedError"] = advanced_error
        rejection_reasons.append(str(advanced_error).strip())
    why_held = _safe_get(status, "whyHeld")
    if why_held not in (None, ""):
        rejection["whyHeld"] = why_held
        rejection_reasons.append(str(why_held).strip())
    warning_text = _safe_get(status, "warningText") or _safe_get(trade, "warningText")
    if warning_text not in (None, ""):
        warning_text_str = str(warning_text).strip()
        if warning_text_str:
            rejection["warningText"] = warning_text_str
            rejection_reasons.append(warning_text_str)
    log_entries = getattr(trade, "log", None)
    if log_entries:
        last_entry = log_entries[-1]
        log_info: dict[str, Any] = {}
        log_message = getattr(last_entry, "message", None)
        if log_message not in (None, ""):
            log_info["message"] = log_message
            rejection_reasons.append(str(log_message).strip())
        log_time = getattr(last_entry, "time", None)
        if log_time not in (None, ""):
            log_info["time"] = log_time
        if log_info:
            rejection["log"] = log_info
    if rejection:
        message["rejection"] = rejection
    if rejection_reasons:
        status_value = update.status
        status_text = str(status_value).strip().lower() if status_value is not None else ""
        rejection_states = {
            "rejected",
            "inactive",
            "cancelled",
            "apicancelled",
            "apicancel",
            "pendingcancel",
            "pendinginactive",
        }
        if status_text not in rejection_states:
            rejection_reasons = []
    if rejection_reasons:
        deduped = []
        seen = set()
        for reason in rejection_reasons:
            text = reason.strip()
            if not text:
                continue
            marker = text.lower()
            if marker in seen:
                continue
            seen.add(marker)
            deduped.append(text)
        if deduped:
            update.rejection_reason = "; ".join(deduped)

    status_text = str(update.status or "").strip().lower()
    if (
        source in {"trade.placeOrder", "trade.orderStatusEvent", "orderStatusEvent"}
        and status_text in {"", "inactive"}
        and update.rejection_reason is None
        and abs(float(update.filled or 0.0)) <= 1e-12
        and abs(float(update.remaining or 0.0)) <= 1e-12
        and fill is None
    ):
        update.status = "PendingSubmit"
        metadata = dict(update.adapter_metadata or {})
        diagnostics = dict(metadata.get("diagnostics") or {})
        diagnostics["initialNativeStatus"] = str(_safe_get(status, "status") or "")
        metadata["diagnostics"] = diagnostics
        metadata.setdefault("extensions", {})
        update.adapter_metadata = metadata

    if _commission_report_has_effective_values(commission_report_data):
        update.commission = _add_optional_numbers(
            update.commission, _optional_float(_safe_get(commission_report_data, "commission"))
        )
        update.realized_pnl = _add_optional_numbers(
            update.realized_pnl, _effective_report_pnl_value(commission_report_data, "realized")
        )
        update.unrealized_pnl = _add_optional_numbers(
            update.unrealized_pnl, _effective_report_pnl_value(commission_report_data, "unrealized")
        )
    if (
        fill_commission_report is not None
        and fill_commission_report is not commission_report_data
        and _commission_report_has_effective_values(fill_commission_report)
    ):
        update.commission = _add_optional_numbers(
            update.commission, _optional_float(_safe_get(fill_commission_report, "commission"))
        )
        update.realized_pnl = _add_optional_numbers(
            update.realized_pnl, _effective_report_pnl_value(fill_commission_report, "realized")
        )
        update.unrealized_pnl = _add_optional_numbers(
            update.unrealized_pnl, _effective_report_pnl_value(fill_commission_report, "unrealized")
        )

    if fill is not None:
        update = _fill_to_update(
            trade,
            fill,
            source=source,
            base_update=update,
            message=message,
            commission_report=fill_commission_report,
            base_commission_report=commission_report_data,
        )
    else:
        update.message = message

    return update


def _fill_to_update(
    trade: Any,
    fill: Any,
    *,
    source: str,
    base_update: TradeUpdate | None = None,
    message: dict[str, Any] | None = None,
    commission_report: Any | None = None,
    base_commission_report: Any | None = None,
) -> TradeUpdate:
    update = base_update or _trade_to_update(trade, source)
    payload = message or (dict(update.message) if update.message else {})

    execution = getattr(fill, "execution", fill)

    last_price = _optional_float(_safe_get(execution, "price"))
    if last_price is not None:
        update.last_fill_price = last_price

    last_quantity = _optional_float(_safe_get(execution, "shares"))
    if last_quantity is not None:
        update.last_fill_quantity = last_quantity

    cum_quantity = _optional_float(_safe_get(execution, "cumQty"))
    if cum_quantity is not None:
        update.filled = cum_quantity

    avg_price = _optional_float(_safe_get(execution, "avgPrice"))
    if avg_price is not None:
        update.avg_fill_price = avg_price

    timestamp = getattr(fill, "time", None) or _safe_get(execution, "time")
    if timestamp:
        with suppress(Exception):
            update.event_time = _parse_ib_datetime(timestamp)

    fill_report = commission_report if commission_report is not None else getattr(fill, "commissionReport", None)
    if fill_report is not None and fill_report is base_commission_report:
        pass
    else:
        pass

    payload["fill"] = {
        "time": timestamp,
        "execution": _execution_to_dict(execution),
        "commissionReport": _commission_report_to_dict(fill_report),
    }

    update.message = payload
    return update


def _build_execution_update(
    execution: Any,
    *,
    source: str,
    contract: Any | None = None,
    fill: Any | None = None,
    commission_report: Any | None = None,
    req_id: int | None = None,
) -> TradeUpdate | None:
    if execution is None and commission_report is None:
        return None

    exec_time = _safe_get(execution, "time")
    event_time: datetime | None = None
    if exec_time:
        with suppress(Exception):
            event_time = _parse_ib_datetime(exec_time)

    order_id = _safe_get(execution, "orderId")
    last_price = _optional_float(_safe_get(execution, "price"))
    last_quantity = _optional_float(_safe_get(execution, "shares"))
    cum_quantity = _optional_float(_safe_get(execution, "cumQty"))
    avg_price = _optional_float(_safe_get(execution, "avgPrice"))
    commission = _optional_float(_safe_get(execution, "commission"))
    realized_pnl = _optional_float(_get_pnl_value(execution, "realized"))
    unrealized_pnl = _optional_float(_get_pnl_value(execution, "unrealized"))

    if _commission_report_has_effective_values(commission_report):
        commission = _add_optional_numbers(
            commission, _optional_float(_safe_get(commission_report, "commission"))
        )
        realized_pnl = _add_optional_numbers(
            realized_pnl, _effective_report_pnl_value(commission_report, "realized")
        )
        unrealized_pnl = _add_optional_numbers(
            unrealized_pnl, _effective_report_pnl_value(commission_report, "unrealized")
        )

    message: dict[str, Any] = {
        "source": source,
        "contract": _contract_to_dict(contract),
        "order": {},
        "status": {},
        "commissionReport": _commission_report_to_dict(commission_report),
        "execution": _execution_to_dict(execution),
    }
    if req_id is not None:
        message["req_id"] = req_id

    if fill is not None:
        timestamp = getattr(fill, "time", None) or exec_time
        message["fill"] = {
            "time": timestamp,
            "execution": _execution_to_dict(execution),
            "commissionReport": _commission_report_to_dict(
                commission_report if commission_report is not None else getattr(fill, "commissionReport", None)
            ),
        }

    return TradeUpdate(
        adapter_id=IBKR_ADAPTER_ID,
        adapter_order_id=order_id,
        adapter_order_ref=(str(_safe_get(execution, "permId")) if _safe_get(execution, "permId") not in (None, "") else None),
        adapter_execution_id=(str(_safe_get(execution, "execId")) if _safe_get(execution, "execId") not in (None, "") else None),
        adapter_metadata={"schemaVersion": 1, "native": {}},
        filled=cum_quantity,
        avg_fill_price=avg_price,
        last_fill_price=last_price,
        last_fill_quantity=last_quantity,
        commission=commission,
        realized_pnl=realized_pnl,
        unrealized_pnl=unrealized_pnl,
        event_time=event_time or datetime.now(timezone.utc),
        message=message,
    )


def _cancel_market_depth(client: Any, ticker: Any, contract: Any) -> None:
    cancel = getattr(client, "cancelMktDepth", None)
    if not callable(cancel):
        return

    req_id = getattr(ticker, "reqId", None)
    variants: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    
    # ib_insync library expects contract object as primary parameter
    # Priority order: contract > ticker.contract > ticker object > req_id
    if contract is not None:
        variants.append(((contract,), {}))
        variants.append(((contract, True), {}))
        variants.append(((contract, False), {}))
    
    if ticker is not None and hasattr(ticker, "contract"):
        ticker_contract = getattr(ticker, "contract")
        if ticker_contract is not None:
            variants.append(((ticker_contract,), {}))
            variants.append(((ticker_contract, True), {}))
            variants.append(((ticker_contract, False), {}))
    
    if ticker is not None:
        variants.append(((ticker,), {}))
        variants.append(((ticker, True), {}))
        variants.append(((ticker, False), {}))
    
    # Fallback to req_id for direct IB API compatibility
    if req_id is not None:
        variants.append(((req_id,), {}))

    if not variants:
        LOGGER.debug("No handle available for market depth cancellation")
        return

    try:
        success = _call_with_variants(cancel, variants)
    except Exception as exc:  # pragma: no cover - defensive logging
        LOGGER.debug(
            "Unable to cancel market depth stream cleanly",
            extra={
                "req_id": req_id,
                "contract": getattr(contract, "conId", None),
                "error": str(exc),
            },
        )
        return

    LOGGER.debug(
        "Market depth cancellation attempted",
        extra={
            "req_id": req_id,
            "contract": getattr(contract, "conId", None),
            "success": success,
        },
    )


def _cancel_tick_by_tick(client: Any, ticker: Any, contract: Any, tick_type: str) -> None:
    cancel = getattr(client, "cancelTickByTickData", None)
    if not callable(cancel):
        return

    req_id = getattr(ticker, "reqId", None)
    variants: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    if contract is not None:
        variants.append(((contract,), {"tickType": tick_type}))
        variants.append(((contract,), {}))
    if ticker is not None:
        variants.append(((ticker,), {}))
        variants.append(((ticker,), {"tickType": tick_type}))
    if req_id is not None:
        variants.append(((req_id,), {}))
        variants.append(((req_id,), {"tickType": tick_type}))

    if not variants:
        LOGGER.debug(
            "No handle available for tick-by-tick cancellation",
            extra={"tick_type": tick_type},
        )
        return

    try:
        success = _call_with_variants(cancel, variants)
    except Exception as exc:  # pragma: no cover - defensive logging
        LOGGER.debug(
            "Unable to cancel tick-by-tick stream cleanly",
            extra={
                "req_id": req_id,
                "contract": getattr(contract, "conId", None),
                "tick_type": tick_type,
                "error": str(exc),
            },
        )
        return

    LOGGER.debug(
        "Tick-by-tick cancellation attempted",
        extra={
            "req_id": req_id,
            "contract": getattr(contract, "conId", None),
            "tick_type": tick_type,
            "success": success,
        },
    )


def _iter_unique_candidates(*candidates: Any) -> Iterable[Any]:
    seen: set[Any] = set()
    for candidate in candidates:
        if candidate is None:
            continue
        if isinstance(candidate, (str, bytes, int)):
            marker: Any = (type(candidate), candidate)
        else:
            marker = id(candidate)
        if marker in seen:
            continue
        seen.add(marker)
        yield candidate


def _call_with_variants(
    func: Callable[..., Any],
    variants: list[tuple[tuple[Any, ...], dict[str, Any]]],
) -> bool:
    attempted: set[tuple[tuple[Any, ...], tuple[tuple[str, Any], ...]]] = set()
    for args, kwargs in variants:
        key = (
            tuple(_hashable_marker(arg) for arg in args),
            tuple(sorted((name, _hashable_marker(value)) for name, value in kwargs.items())),
        )
        if key in attempted:
            continue
        attempted.add(key)
        try:
            func(*args, **kwargs)
        except TypeError:
            continue
        return True
    return False


def _hashable_marker(value: Any) -> Any:
    try:
        hash(value)
    except TypeError:
        return id(value)
    else:
        return value


def _run_ib_call_in_executor(
    func: Callable[[IB], T], ib: IB, client: "IBAsyncClient"
) -> T:
    loop = getattr(_EXECUTOR_THREAD_STATE, "loop", None)
    if loop is None or loop.is_closed():
        loop = asyncio.new_event_loop()
        _EXECUTOR_THREAD_STATE.loop = loop
        client._executor_loop = loop
    asyncio.set_event_loop(loop)
    try:
        return func(ib)
    finally:
        asyncio.set_event_loop(None)


def _shutdown_executor_loop(client: "IBAsyncClient") -> None:
    loop = getattr(_EXECUTOR_THREAD_STATE, "loop", None)
    if loop is None:
        loop = client._executor_loop
    if loop is None:
        return
    if loop.is_closed():
        _EXECUTOR_THREAD_STATE.loop = None
        if client._executor_loop is loop:
            client._executor_loop = None
        return
    try:
        asyncio.set_event_loop(loop)
        if sys.version_info >= (3, 7):
            loop.run_until_complete(loop.shutdown_asyncgens())
    except RuntimeError:
        pass
    finally:
        asyncio.set_event_loop(None)
        loop.close()
    _EXECUTOR_THREAD_STATE.loop = None
    if client._executor_loop is loop:
        client._executor_loop = None


def _safe_get(obj: Any, attr: str) -> Any:
    if obj is None:
        return None
    if isinstance(obj, Mapping):
        return obj.get(attr)
    return getattr(obj, attr, None)


def _get_pnl_value(obj: Any, base_name: str) -> Any:
    """Retrieve PnL value from various IB field variants.

    IB sometimes reports PnL metrics using different field names depending on
    context and asset class. In addition to the canonical
    `realizedPNL`/`unrealizedPNL` (or mixed-case `PnL`), certain commission
    reports expose `mktRealizedPNL` and `mktUnrealizedPNL`.

    This helper normalizes access across these variants.
    """
    if obj is None:
        return None
    # Canonical key variants like realizedPNL / realizedPnL
    for suffix in ("PNL", "PnL"):
        value = _safe_get(obj, f"{base_name}{suffix}")
        if value is not None:
            return value
    # Market-to-market variants: mktRealizedPNL / mktUnrealizedPNL
    capitalized = base_name[:1].upper() + base_name[1:]
    for suffix in ("PNL", "PnL"):
        value = _safe_get(obj, f"mkt{capitalized}{suffix}")
        if value is not None:
            return value
    return None


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result


def _optional_price(value: Any) -> float | None:
    result = _optional_float(value)
    if result is None:
        return None
    if not math.isfinite(result):
        return None
    if result < 0:
        return None
    return result


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _require_contract(contract: Any) -> Any:
    if contract is None:
        raise IBMarketDataError("Contract must not be None")
    return contract


_IB_TIMESTAMP_ZONE = ZoneInfo("America/New_York")


def _coerce_ib_execution_filter_time(value: datetime | str) -> str:
    """Format IB execution filter time without a timezone suffix."""

    if isinstance(value, str):
        parsed = _parse_ib_datetime_input(value)
        if parsed is None:
            return value
        value = parsed
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(_IB_TIMESTAMP_ZONE).strftime("%Y%m%d %H:%M:%S")


def _localize_ib_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=_IB_TIMESTAMP_ZONE)
    return value.astimezone(timezone.utc)


def _parse_ib_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _localize_ib_datetime(value)
    if isinstance(value, date):
        return _localize_ib_datetime(datetime(value.year, value.month, value.day))
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)

    text = str(value)
    for fmt in ("%Y%m%d %H:%M:%S", "%Y%m%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text, fmt)
            return _localize_ib_datetime(dt)
        except ValueError:
            continue
    return datetime.now(timezone.utc)


def _option_greeks_to_dict(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    payload = {
        "impliedVol": _optional_float(getattr(value, "impliedVol", None)),
        "delta": _optional_float(getattr(value, "delta", None)),
        "gamma": _optional_float(getattr(value, "gamma", None)),
        "vega": _optional_float(getattr(value, "vega", None)),
        "theta": _optional_float(getattr(value, "theta", None)),
        "optPrice": _optional_price(getattr(value, "optPrice", None)),
        "pvDividend": _optional_float(getattr(value, "pvDividend", None)),
        "undPrice": _optional_price(getattr(value, "undPrice", None)),
    }
    if any(item is not None for item in payload.values()):
        return payload
    return None


def _ticker_to_snapshot(
    ticker: Any,
    *,
    contract: Any | None = None,
) -> dict[str, Any]:
    active_contract = getattr(ticker, "contract", None) or contract
    timestamp = getattr(ticker, "time", None)
    return {
        "contract": _contract_to_dict(active_contract),
        "timestamp": _parse_ib_datetime(timestamp).isoformat() if timestamp is not None else None,
        "bid": _optional_price(getattr(ticker, "bid", None)),
        "ask": _optional_price(getattr(ticker, "ask", None)),
        "last": _optional_price(getattr(ticker, "last", None)),
        "close": _optional_price(getattr(ticker, "close", None)),
        "open": _optional_price(getattr(ticker, "open", None)),
        "high": _optional_price(getattr(ticker, "high", None)),
        "low": _optional_price(getattr(ticker, "low", None)),
        "midpoint": _optional_price(getattr(ticker, "midpoint", lambda: None)()),
        "bidSize": _optional_float(getattr(ticker, "bidSize", None)),
        "askSize": _optional_float(getattr(ticker, "askSize", None)),
        "lastSize": _optional_float(getattr(ticker, "lastSize", None)),
        "volume": _optional_float(getattr(ticker, "volume", None)),
        "callOpenInterest": _optional_float(getattr(ticker, "callOpenInterest", None)),
        "putOpenInterest": _optional_float(getattr(ticker, "putOpenInterest", None)),
        "histVolatility": _optional_float(getattr(ticker, "histVolatility", None)),
        "impliedVolatility": _optional_float(getattr(ticker, "impliedVolatility", None)),
        "markPrice": _optional_price(getattr(ticker, "marketPrice", lambda: None)()),
        "modelGreeks": _option_greeks_to_dict(getattr(ticker, "modelGreeks", None)),
        "bidGreeks": _option_greeks_to_dict(getattr(ticker, "bidGreeks", None)),
        "askGreeks": _option_greeks_to_dict(getattr(ticker, "askGreeks", None)),
        "lastGreeks": _option_greeks_to_dict(getattr(ticker, "lastGreeks", None)),
    }


def _snapshot_has_market_data(snapshot: Mapping[str, Any]) -> bool:
    direct_fields = (
        snapshot.get("bid"),
        snapshot.get("ask"),
        snapshot.get("last"),
        snapshot.get("close"),
        snapshot.get("volume"),
        snapshot.get("callOpenInterest"),
        snapshot.get("putOpenInterest"),
        snapshot.get("impliedVolatility"),
    )
    if any(value is not None for value in direct_fields):
        return True
    for key in ("modelGreeks", "bidGreeks", "askGreeks", "lastGreeks"):
        payload = snapshot.get(key)
        if isinstance(payload, Mapping) and any(item is not None for item in payload.values()):
            return True
    return False


def _contract_details_to_dict(item: Any) -> dict[str, Any]:
    contract = getattr(item, "contract", None) or getattr(item, "summary", None)
    result = {
        "contract": _contract_to_dict(contract),
        "marketName": _safe_get(item, "marketName"),
        "longName": _safe_get(item, "longName"),
        "minTick": _optional_float(_safe_get(item, "minTick")),
        "orderTypes": _safe_get(item, "orderTypes"),
        "validExchanges": _safe_get(item, "validExchanges"),
        "priceMagnifier": _optional_int(_safe_get(item, "priceMagnifier")),
        "underConId": _optional_int(_safe_get(item, "underConId")),
        "tradingHours": _safe_get(item, "tradingHours"),
        "liquidHours": _safe_get(item, "liquidHours"),
        "timeZoneId": _safe_get(item, "timeZoneId"),
        "industry": _safe_get(item, "industry"),
        "category": _safe_get(item, "category"),
        "subcategory": _safe_get(item, "subcategory"),
        "minSize": _optional_float(_safe_get(item, "minSize")),
        "sizeIncrement": _optional_float(_safe_get(item, "sizeIncrement")),
        "suggestedSizeIncrement": _optional_float(_safe_get(item, "suggestedSizeIncrement")),
        "contractMonth": _safe_get(item, "contractMonth"),
        "realExpirationDate": _safe_get(item, "realExpirationDate"),
    }
    return {key: value for key, value in result.items() if value not in (None, "", [], {})}


def _option_parameter_to_dict(item: Any) -> dict[str, Any]:
    expirations = sorted(str(value) for value in list(getattr(item, "expirations", []) or []) if value)
    strikes: list[float] = []
    for value in list(getattr(item, "strikes", []) or []):
        parsed = _optional_float(value)
        if parsed is not None:
            strikes.append(parsed)
    return {
        "exchange": _safe_get(item, "exchange"),
        "underlyingConId": _optional_int(_safe_get(item, "underlyingConId")),
        "tradingClass": _safe_get(item, "tradingClass"),
        "multiplier": _safe_get(item, "multiplier"),
        "expirations": expirations,
        "strikes": sorted(set(strikes)),
    }


def _contract_to_dict(contract: Any) -> dict[str, Any]:
    if contract is None:
        return {}
    fields = [
        "conId",
        "symbol",
        "secType",
        "currency",
        "exchange",
        "primaryExchange",
        "localSymbol",
        "tradingClass",
        "lastTradeDateOrContractMonth",
    ]
    result: dict[str, Any] = {}
    for field in fields:
        value = getattr(contract, field, None)
        if value not in (None, ""):
            result[field] = value
    return result


def _order_to_dict(order: Any) -> dict[str, Any]:
    if order is None:
        return {}
    fields = [
        "orderId",
        "clientId",
        "permId",
        "action",
        "openClose",
        "orderType",
        "totalQuantity",
        "lmtPrice",
        "auxPrice",
        "tif",
        "account",
    ]
    result: dict[str, Any] = {}
    for field in fields:
        value = getattr(order, field, None)
        if value not in (None, ""):
            result[field] = value
    return result


def _order_status_to_dict(status: Any) -> dict[str, Any]:
    if status is None:
        return {}
    fields = [
        "status",
        "filled",
        "remaining",
        "avgFillPrice",
        "lastFillPrice",
        "permId",
        "parentId",
        "clientId",
        "whyHeld",
        "warningText",
    ]
    result: dict[str, Any] = {}
    for field in fields:
        value = getattr(status, field, None)
        if value not in (None, ""):
            result[field] = value
    return result


def _commission_report_to_dict(report: Any) -> dict[str, Any]:
    if report is None:
        return {}
    fields = [
        "execId",
        "commission",
        "currency",
        "realizedPNL",
        "unrealizedPNL",
        # Include market-to-market variants to surface details used by downstream
        "mktRealizedPNL",
        "mktUnrealizedPNL",
        "yield",
        "yieldRedemptionDate",
    ]
    result: dict[str, Any] = {}
    for field in fields:
        attr = field
        if field == "yield":
            attr = "yield_"
        value = _safe_get(report, attr)
        if value not in (None, ""):
            result[field] = value
    return result


def _execution_to_dict(execution: Any) -> dict[str, Any]:
    if execution is None:
        return {}
    fields = [
        "execId",
        "orderId",
        "price",
        "shares",
        "cumQty",
        "avgPrice",
        "permId",
        "clientId",
        "time",
        "side",
        "lastLiquidity",
    ]
    result: dict[str, Any] = {}
    for field in fields:
        value = _safe_get(execution, field)
        if value not in (None, ""):
            result[field] = value
    return result


def _queue_offer(queue: "asyncio.Queue[T]", item: T) -> None:
    try:
        queue.put_nowait(item)
    except asyncio.QueueFull:
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
        try:
            queue.put_nowait(item)
        except asyncio.QueueFull:  # pragma: no cover - defensive
            pass


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"", "none", "null"}:
            return None
        if lowered in {"true", "t", "yes", "y", "1"}:
            return True
        if lowered in {"false", "f", "no", "n", "0"}:
            return False
    return bool(value)


def _build_historical_bar(bar: Any) -> HistoricalBar:
    timestamp = getattr(bar, "date", None)
    if timestamp in (None, ""):
        timestamp = getattr(bar, "time", None)
    if timestamp in (None, ""):
        timestamp = datetime.now(timezone.utc)
    return HistoricalBar(
        time=_parse_ib_datetime(timestamp),
        open=float(getattr(bar, "open", 0.0) or 0.0),
        high=float(getattr(bar, "high", 0.0) or 0.0),
        low=float(getattr(bar, "low", 0.0) or 0.0),
        close=float(getattr(bar, "close", 0.0) or 0.0),
        volume=float(getattr(bar, "volume", 0.0) or 0.0),
        wap=_optional_float(getattr(bar, "wap", None)),
        count=_optional_int(getattr(bar, "barCount", None)),
    )


def _bar_size_delta(bar_size: str | None) -> timedelta | None:
    if not bar_size:
        return None
    parts = str(bar_size).strip().lower().split()
    if not parts:
        return None
    try:
        amount = int(float(parts[0]))
    except (TypeError, ValueError):
        return None
    if amount <= 0:
        return None
    unit = parts[1] if len(parts) > 1 else ""
    if unit.startswith("sec"):
        return timedelta(seconds=amount)
    if unit.startswith("min"):
        return timedelta(minutes=amount)
    if unit.startswith("hour"):
        return timedelta(hours=amount)
    if unit.startswith("day"):
        return timedelta(days=amount)
    if unit.startswith("week"):
        return timedelta(weeks=amount)
    return None


def _historical_bar_is_closed(
    bar: HistoricalBar,
    bar_size: str | None,
    *,
    now: datetime | None = None,
) -> bool:
    delta = _bar_size_delta(bar_size)
    if delta is None:
        return True
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    else:
        current = current.astimezone(timezone.utc)
    timestamp = bar.time
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    else:
        timestamp = timestamp.astimezone(timezone.utc)
    return timestamp + delta <= current + timedelta(seconds=1)


def _normalise_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _historical_stream_allows_open_updates(bar_size: str | None) -> bool:
    delta = _bar_size_delta(bar_size)
    if delta is None:
        return False
    return delta < timedelta(seconds=60)


def _latest_closed_historical_bar(
    bars: Any,
    bar_size: str | None,
    *,
    now: datetime | None = None,
) -> Any | None:
    for raw_bar in reversed(list(bars)):
        try:
            bar = _build_historical_bar(raw_bar)
        except Exception:  # pragma: no cover - defensive
            continue
        if _historical_bar_is_closed(bar, bar_size, now=now):
            return raw_bar
    return None


def _build_dom_snapshot(
    ticker: Any, exchanges: tuple[dict[str, Any], ...] | None = None
) -> DOMSnapshot:
    bids = tuple(
        DOMLevel(
            price=float(getattr(level, "price", 0.0) or 0.0),
            size=float(getattr(level, "size", 0.0) or 0.0),
            market_maker=getattr(level, "marketMaker", None),
            position=int(getattr(level, "position", 0) or 0),
        )
        for level in getattr(ticker, "domBids", [])
    )
    asks = tuple(
        DOMLevel(
            price=float(getattr(level, "price", 0.0) or 0.0),
            size=float(getattr(level, "size", 0.0) or 0.0),
            market_maker=getattr(level, "marketMaker", None),
            position=int(getattr(level, "position", 0) or 0),
        )
        for level in getattr(ticker, "domAsks", [])
    )
    return DOMSnapshot(
        bids=bids,
        asks=asks,
        timestamp=datetime.now(timezone.utc),
        exchanges=tuple(exchanges or ()),
    )


def _resolve_reference_ticker(ib: Any, contract: Any, fallback: Any) -> Any:
    ticker_func = getattr(ib, "ticker", None)
    if callable(ticker_func):
        try:
            reference = ticker_func(contract)
        except Exception:  # pragma: no cover - defensive
            LOGGER.debug("Failed to resolve ticker() reference", exc_info=True)
        else:
            if reference is not None:
                return reference
    return fallback


def _iter_tickers(container: Any) -> tuple[Any, ...]:
    if container is None:
        return ()
    if isinstance(container, tuple):
        return container
    if isinstance(container, list):
        return tuple(container)
    if isinstance(container, set):
        return tuple(container)
    return (container,)


def _tickers_match(left: Any, right: Any) -> bool:
    if left is right:
        return True
    left_contract = getattr(left, "contract", None)
    right_contract = getattr(right, "contract", None)
    if left_contract is right_contract and left_contract is not None:
        return True
    left_id = _optional_int(getattr(left_contract, "conId", None))
    right_id = _optional_int(getattr(right_contract, "conId", None))
    if left_id is not None and right_id is not None and left_id == right_id:
        return True
    left_symbol = getattr(left_contract, "symbol", None)
    right_symbol = getattr(right_contract, "symbol", None)
    left_sec_type = getattr(left_contract, "secType", None)
    right_sec_type = getattr(right_contract, "secType", None)
    left_exchange = getattr(left_contract, "exchange", None)
    right_exchange = getattr(right_contract, "exchange", None)
    if left_symbol and right_symbol and left_symbol == right_symbol:
        if not left_sec_type or not right_sec_type or left_sec_type == right_sec_type:
            if not left_exchange or not right_exchange or left_exchange == right_exchange:
                return True
    return False


def _build_depth_exchanges(raw: Any, contract: Any) -> tuple[dict[str, Any], ...]:
    result: list[dict[str, Any]] = []
    iterable = raw if isinstance(raw, (list, tuple)) else list(raw or [])
    for entry in iterable:
        payload: dict[str, Any] = {}
        for attr in (
            "exchange",
            "secType",
            "listingExchange",
            "description",
            "dataType",
        ):
            value = getattr(entry, attr, None)
            if value not in (None, ""):
                payload[attr] = value
        agg_group = _optional_int(getattr(entry, "aggGroup", None))
        if agg_group is not None:
            payload["aggGroup"] = agg_group
        if not payload:
            continue
        result.append(payload)
    if not result:
        return tuple()
    contract_exchange = getattr(contract, "exchange", None)
    if contract_exchange:
        filtered = [item for item in result if item.get("exchange") in (contract_exchange, None)]
        if filtered:
            result = filtered
    return tuple(result)


def _build_realtime_price(ticker: Any) -> RealTimePrice:
    contract = getattr(ticker, "contract", None)
    symbol = getattr(contract, "symbol", None) if contract else None
    return RealTimePrice(
        symbol=symbol or "",
        bid=_optional_price(getattr(ticker, "bid", None)),
        ask=_optional_price(getattr(ticker, "ask", None)),
        last=_optional_price(getattr(ticker, "last", None)),
        last_size=_optional_price(getattr(ticker, "lastSize", None)),
        close=_optional_price(getattr(ticker, "close", None)),
        timestamp=datetime.now(timezone.utc),
        bid_size=_optional_price(getattr(ticker, "bidSize", None)),
        ask_size=_optional_price(getattr(ticker, "askSize", None)),
        open=_optional_price(getattr(ticker, "open", None)),
        high=_optional_price(getattr(ticker, "high", None)),
        low=_optional_price(getattr(ticker, "low", None)),
        volume=_optional_price(getattr(ticker, "volume", None)),
    )


def _build_tick_by_tick(
    tick: Any,
) -> TickByTickBidAsk | TickByTickLast | TickByTickMidPoint | None:
    timestamp = _parse_ib_datetime(getattr(tick, "time", datetime.now(timezone.utc)))
    if hasattr(tick, "bidPrice") and hasattr(tick, "askPrice"):
        attrib = getattr(tick, "tickAttribBidAsk", None)
        return TickByTickBidAsk(
            time=timestamp,
            bid_price=float(getattr(tick, "bidPrice", 0.0) or 0.0),
            ask_price=float(getattr(tick, "askPrice", 0.0) or 0.0),
            bid_size=float(getattr(tick, "bidSize", 0.0) or 0.0),
            ask_size=float(getattr(tick, "askSize", 0.0) or 0.0),
            bid_past_low=_optional_bool(getattr(attrib, "bidPastLow", None)),
            ask_past_high=_optional_bool(getattr(attrib, "askPastHigh", None)),
        )
    if hasattr(tick, "midPoint"):
        return TickByTickMidPoint(
            time=timestamp,
            mid_price=float(getattr(tick, "midPoint", 0.0) or 0.0),
        )
    if hasattr(tick, "price"):
        attrib = getattr(tick, "tickAttribLast", None)
        return TickByTickLast(
            time=timestamp,
            price=float(getattr(tick, "price", 0.0) or 0.0),
            size=float(getattr(tick, "size", 0.0) or 0.0),
            exchange=getattr(tick, "exchange", None),
            special_conditions=getattr(tick, "specialConditions", None),
            past_limit=_optional_bool(getattr(attrib, "pastLimit", None)),
            unreported=_optional_bool(getattr(attrib, "unreported", None)),
        )
    return None


def _build_historical_tick(
    tick: Any,
) -> HistoricalTickBidAsk | HistoricalTickLast | None:
    timestamp = _parse_ib_datetime(getattr(tick, "time", datetime.now(timezone.utc)))
    if hasattr(tick, "priceBid") and hasattr(tick, "priceAsk"):
        attrib = getattr(tick, "tickAttribBidAsk", None)
        return HistoricalTickBidAsk(
            time=timestamp,
            bid_price=float(getattr(tick, "priceBid", 0.0) or 0.0),
            ask_price=float(getattr(tick, "priceAsk", 0.0) or 0.0),
            bid_size=float(getattr(tick, "sizeBid", 0.0) or 0.0),
            ask_size=float(getattr(tick, "sizeAsk", 0.0) or 0.0),
            bid_past_low=_optional_bool(getattr(attrib, "bidPastLow", None)),
            ask_past_high=_optional_bool(getattr(attrib, "askPastHigh", None)),
        )
    if hasattr(tick, "price"):
        attrib = getattr(tick, "tickAttribLast", None)
        return HistoricalTickLast(
            time=timestamp,
            price=float(getattr(tick, "price", 0.0) or 0.0),
            size=float(getattr(tick, "size", 0.0) or 0.0),
            exchange=getattr(tick, "exchange", None),
            special_conditions=getattr(tick, "specialConditions", None),
            past_limit=_optional_bool(getattr(attrib, "pastLimit", None)),
            unreported=_optional_bool(getattr(attrib, "unreported", None)),
        )
    return None


def _build_scanner_subscription(
    payload: Mapping[str, Any],
    tag_filters: Iterable[Mapping[str, Any]] | None = None,
) -> tuple[Any, list[Any]]:
    if ScannerSubscription is None:
        subscription: Any = {}
    else:
        subscription = ScannerSubscription()

    key_map = {
        "scan_code": "scanCode",
        "scanCode": "scanCode",
        "location_code": "locationCode",
        "locationCode": "locationCode",
        "number_of_rows": "numberOfRows",
        "numberOfRows": "numberOfRows",
        "instrument": "instrument",
    }

    for raw_key, value in payload.items():
        if raw_key in {"filters", "tag_filters", "tagFilters"}:
            continue
        key = key_map.get(raw_key, raw_key)
        if isinstance(subscription, dict):
            subscription[key] = value
            continue
        try:
            setattr(subscription, key, value)
        except Exception:
            continue

    tags: list[Any] = []
    filters = payload.get("filters") or payload.get("tag_filters") or payload.get("tagFilters")
    if filters is None:
        filters = tag_filters
    if filters:
        for entry in filters:
            if not isinstance(entry, Mapping):
                continue
            tag = entry.get("tag") or entry.get("name")
            value = entry.get("value") if "value" in entry else entry.get("tagValue")
            if tag is None or value is None:
                continue
            if TagValue is not None:
                try:
                    tags.append(TagValue(str(tag), str(value)))
                    continue
                except Exception:
                    pass
            tags.append({"tag": str(tag), "value": str(value)})

    return subscription, tags


def _scanner_data_to_dict(entry: Any) -> dict[str, Any]:
    if isinstance(entry, Mapping):
        return dict(entry)
    payload: dict[str, Any] = {}
    rank = getattr(entry, "rank", None)
    if rank is not None:
        payload["rank"] = rank
    contract = getattr(entry, "contractDetails", None) or getattr(entry, "contract", None)
    if contract is not None:
        base_contract = getattr(contract, "contract", None) or contract
        raw_symbol = getattr(base_contract, "symbol", None)
        local_symbol = getattr(base_contract, "localSymbol", None)
        payload["symbol_raw"] = raw_symbol
        payload["localSymbol"] = local_symbol
        payload["symbol"] = local_symbol or raw_symbol
        payload["exchange"] = getattr(base_contract, "exchange", None)
        payload["primaryExchange"] = getattr(base_contract, "primaryExchange", None)
        payload["currency"] = getattr(base_contract, "currency", None)
        payload["secType"] = getattr(base_contract, "secType", None)
        payload["conId"] = getattr(base_contract, "conId", None)
        if not payload.get("symbol"):
            try:
                LOGGER.warning(
                    "Scanner entry missing symbol | contractDetails=%s base_contract=%s",
                    contract,
                    base_contract,
                )
            except Exception:
                LOGGER.debug("Failed to log scanner contractDetails snapshot", exc_info=True)
    else:
        payload["symbol"] = getattr(entry, "symbol", None)
    if hasattr(entry, "details") and not contract:
        payload["details"] = getattr(entry, "details", None)
    return payload


__all__ = [
    "IBAsyncClient",
    "AccountSummaryItem",
    "PositionItem",
]
