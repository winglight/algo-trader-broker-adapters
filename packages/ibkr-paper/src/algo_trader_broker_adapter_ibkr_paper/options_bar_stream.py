"""Shared, read-only IB historicalDataUpdate subscriptions. No polling/aggregation."""
import asyncio
from collections import OrderedDict
from dataclasses import asdict, fields, replace
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from algo_trader_broker_sdk import BrokerConnectionError
from algo_trader_broker_sdk.options import check, timestamp
from algo_trader_broker_sdk.options_bars import (
    OptionBarStreamCapability,
    OptionBarSubscription,
    OptionBarUpdate,
    bar_scope,
)
from algo_trader_broker_sdk.options_calendar import OptionCalendarQuery

from .options_history import normalize
from .options_reads import now_wire

SNAPSHOT_LIMIT = 2048
MAX_BINDINGS = 32
MAX_OWNERS = 32
MAX_NATIVE_STREAMS = 32


async def capability(adapter, scope):
    async def read(ib, bound):
        observed = now_wire()
        # Use the vendor client's tested handshake boundary, not a guessed
        # protocol threshold. ib_async 2.0.1 requires MinClientVersion 157.
        supported = ib.client.serverVersion() >= ib.client.MinClientVersion
        return OptionBarStreamCapability(bound.scope,
            'IMPLEMENTED' if supported else 'UNSUPPORTED',
            ('provider_native',) if supported else (), ('1Min',) if supported else (),
            observed, (timestamp(observed) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z"),
            ('NATIVE_ENTITLEMENT_REQUIRED',) if supported else ('IB_SERVER_TOO_OLD',))
    return await adapter._option_read(scope, read)


class NativeBars:
    def __init__(self, adapter, ib, bound, connection, request, binding, contract):
        self.adapter, self.ib, self.bound = adapter, ib, bound
        self.connection, self.request = connection, request
        self.binding, self.contract = binding, contract
        self.owners = {}
        self.owner_errors = {}
        self.ready = asyncio.Event()
        self.error = None
        self.snapshots = OrderedDict()
        self.bootstrap_captured = False
        self.snapshot_view = None
        self.initialized = asyncio.Event()
        self.calendar = None
        self.rows = None
        self.req_id = None
        self.future = None
        self.task = None

    def fail(self, exc):
        if self.error is None:
            self.error = exc
        self.ready.set()
        self.initialized.set()
        if self.future is not None and not self.future.done():
            self.future.set_exception(self.error)
        for queue, _, _ in self.owners.values():
            if queue is None:
                continue
            while not queue.empty():
                queue.get_nowait()
            queue.put_nowait(self.error)

    async def guard(self, request=None):
        async def current(ib, bound):
            check(ib is self.ib and bound == self.bound
                  and self.adapter._option_connection == self.connection,
                  'IB option bar connection changed')
            self.adapter._option_bound_contracts(request or self.request)
        await self.adapter._option_read(self.request, current)

    async def publish(self, row, snapshot):
        await self.guard()
        check(isinstance(row.date, datetime) and row.date.tzinfo is not None,
              'IB native bar requires an aware timestamp')
        day = (datetime.now(UTC) if snapshot else row.date).astimezone(
            ZoneInfo('America/New_York')).date().isoformat()
        if (self.calendar is None or self.calendar.trade_date != day
                or timestamp(now_wire()) >= timestamp(self.calendar.expires_at)):
            self.calendar = await self.adapter.option_calendar(OptionCalendarQuery(
                **asdict(bar_scope(self.request)), bindings=(self.binding,), trade_date=day))
        # normalize only maps native OHLCV; no new intervals or finality flags.
        from types import SimpleNamespace
        bar = normalize(row, SimpleNamespace(data_kind='BAR'), self.binding.canonical_id, row.date)
        await self.guard()
        update = OptionBarUpdate(bar_scope(self.request), self.binding, self.request.feed,
            self.request.timeframe, bar, 'IBKR_NATIVE_HISTORICAL_UPDATE', now_wire(), self.calendar, snapshot)
        self.snapshots[bar.time] = update
        self.snapshot_view = None
        while len(self.snapshots) > SNAPSHOT_LIMIT:
            self.snapshots.popitem(last=False)
        for token, (queue, binding, request) in tuple(self.owners.items()):
            if snapshot or queue is None:
                continue
            await self.guard(request)
            if queue.full():
                error = BrokerConnectionError('IB native bar consumer exceeded bounded queue')
                self.owner_errors[token] = error
                self.owners.pop(token, None)
                while not queue.empty():
                    queue.get_nowait()
                queue.put_nowait(error)
                continue
            queue.put_nowait((update, binding))
        if not self.owners:
            raise BrokerConnectionError('IB native bar has no remaining owners')

    async def run(self):
        from ib_async.objects import BarDataList
        ib = self.ib
        def changed(rows, has_new_bar):
            # Copy before ib_async mutates its last row on the next callback.
            if rows and self.error is None:
                self.pending.append(replace(rows[-1]))
                if self.bootstrap_captured:
                    del rows[:-2]  # Retain initial history until it has been copied.
                if len(self.pending) > 256:
                    self.fail(BrokerConnectionError('IB native bar input exceeded bound'))
                self.ready.set()
        def error(req_id, code, message, *args):
            if req_id == self.req_id:
                self.fail(BrokerConnectionError(f'IB native option bar request failed: {code}'))
        def disconnected(*args):
            self.fail(BrokerConnectionError('IB native option bar connection closed'))
        self.pending = []
        ib.errorEvent += error
        ib.disconnectedEvent += disconnected
        try:
            await self.guard()
            self.req_id = ib.client.getReqId()
            self.rows = BarDataList()
            self.rows.reqId = self.req_id
            self.rows.contract = self.contract
            self.future = ib.wrapper.startReq(self.req_id, self.contract, container=self.rows)
            ib.wrapper.startSubscription(self.req_id, self.rows, self.contract)
            self.rows.updateEvent += changed
            ib.client.reqHistoricalData(self.req_id, self.contract, '', '1 D', '1 min',
                                        'TRADES', False, 2, True, [])
            await asyncio.wait_for(self.future, self.adapter._qualification_timeout)
            if self.error:
                raise self.error
            # Reuse this native subscription's history for reconnect catch-up.
            # Copy before yielding so concurrent native revisions stay ordered.
            if not self.rows:
                raise BrokerConnectionError("IB native option bar bootstrap returned no bars")
            initial = tuple(replace(row) for row in self.rows[-SNAPSHOT_LIMIT:])
            self.bootstrap_captured = True
            del self.rows[:-2]
            for row in initial:
                await self.publish(row, True)
            initial = ()
            self.initialized.set()
            while True:
                await self.ready.wait()
                self.ready.clear()
                if self.error:
                    raise self.error
                pending, self.pending = self.pending, []
                for row in pending:
                    await self.publish(row, False)
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            self.fail(BrokerConnectionError("IB native option bar initialization timed out"))
        except Exception as exc:  # noqa: BLE001 - publish terminal failures to every owner
            self.fail(exc)
        finally:
            ib.errorEvent -= error
            ib.disconnectedEvent -= disconnected
            if self.rows is not None:
                self.rows.updateEvent -= changed
            if self.req_id is not None:
                try:
                    ib.client.cancelHistoricalData(self.req_id)
                except Exception as exc:
                    raise BrokerConnectionError('IB native bar cancellation failed') from exc
                finally:
                    if self.rows is not None:
                        ib.wrapper.endSubscription(self.rows)
                    ib.wrapper._endReq(self.req_id)



async def stream(adapter, request):
    check(type(request) is OptionBarSubscription and request.feed == 'provider_native'
          and request.timeframe == '1Min', 'IB native bars require provider_native 1Min')
    check(len(request.bindings) <= MAX_BINDINGS, 'IB native bars allow at most 32 bindings per owner')
    (await capability(adapter, bar_scope(request))).require_subscription(request, datetime.now(UTC))
    async def open_read(ib, bound):
        return ib, bound, adapter._option_bound_contracts(request)
    ib, bound, contracts = await adapter._option_read(request, open_read)
    registry = adapter._option_bar_registry
    queue = asyncio.Queue(maxsize=256)
    token = object()  # Per-iterator identity preserves two leases of one owner.
    owned = []
    check(len(adapter._option_bar_owner_tokens) < MAX_OWNERS, 'IB native bar owner limit reached')
    adapter._option_bar_owner_tokens.add(token)
    try:
        for binding, contract in zip(request.bindings, contracts, strict=True):
            single = replace(request, bindings=(binding,))
            key = (id(ib), adapter._option_connection, bar_scope(request), bound,
                   tuple(getattr(binding, f.name) for f in fields(binding) if f.name != "qualified_at"),
                   request.feed, request.timeframe)
            entry = registry.get(key)
            if entry is None:
                check(len(registry) < MAX_NATIVE_STREAMS, 'IB native bar stream limit reached')
                entry = NativeBars(adapter, ib, bound, adapter._option_connection, single, binding, contract)
                registry[key] = entry
                entry.task = asyncio.create_task(entry.run())
            entry.owners[token] = (None, binding, single)
            owned.append((key, entry))
            if entry.error:
                raise entry.error
        # Reserve upstream references during bootstrap, but do not queue live
        # data until the immutable replay view is captured. This prevents a
        # newer snapshot from being followed by an older queued correction.
        for _, entry in owned:
            await entry.initialized.wait()
            if entry.error:
                raise entry.error
            await entry.guard(request)
        replay = []
        for _, entry in owned:
            _, binding, single = entry.owners[token]
            if entry.snapshot_view is None:
                entry.snapshot_view = tuple(entry.snapshots.values())
            replay.append((entry, binding, entry.snapshot_view))
            entry.owners[token] = (queue, binding, single)
        # Views contain shared immutable DTO references, not per-owner copies.
        # Only the cursor and one yielded DTO are owner-specific; live backlog
        # remains at 256, independent of bootstrap size or binding count.
        for entry, binding, view in replay:
            for update in view:
                for _, active in owned:
                    if token in active.owner_errors:
                        raise active.owner_errors[token]
                    if active.error:
                        raise active.error
                await entry.guard(request)
                yield replace(update, binding=binding, is_snapshot=True)
        replay.clear()
        view = ()
        while True:
            value = await queue.get()
            if isinstance(value, Exception):
                raise value
            for _, entry in owned:
                if token in entry.owner_errors:
                    raise entry.owner_errors[token]
                await entry.guard(request)
            update, binding = value
            yield replace(update, binding=binding)
    finally:
        adapter._option_bar_owner_tokens.discard(token)
        cleanup_error = None
        for key, entry in owned:
            entry.owners.pop(token, None)
            entry.owner_errors.pop(token, None)
            if not entry.owners:
                if registry.get(key) is entry:
                    registry.pop(key)
                entry.task.cancel()
                try:
                    await entry.task
                except asyncio.CancelledError:
                    pass
                except Exception as exc:  # noqa: BLE001 - release every owned request first
                    cleanup_error = exc
        if cleanup_error is not None:
            raise cleanup_error
