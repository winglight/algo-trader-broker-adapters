"""Bounded native intraday bars and historical ticks; never archive coverage."""

import asyncio
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from zoneinfo import ZoneInfo

from algo_trader_broker_sdk import BrokerCapabilityError
from algo_trader_broker_sdk.options import (
    OptionHistoryRequest, OptionHistoryPage, OptionBar, OptionTrade, OptionQuote, check, timestamp,
)

from .options_reads import native_decimal, now_wire
from .options_quotes import size

BAR_SIZES = {**{f"{n}Min": f"{n} min" + ("s" if n != 1 else "") for n in (1, 2, 3, 5, 10, 15, 20, 30)},
             **{f"{n}Hour": f"{n} hour" + ("s" if n != 1 else "") for n in (1, 2, 3, 4, 8)}}


def wire(at):
    return at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def next_day(at):
    local = at.astimezone(ZoneInfo("America/New_York"))
    return (local.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)).astimezone(timezone.utc)


async def native_page(adapter, ib, contract, request, start, end):
    # Serialize option history across owners on this connection. The existing
    # IB client also applies its global outbound-message rate limit.
    loop = asyncio.get_running_loop()
    await asyncio.sleep(max(0, adapter._option_history_next - loop.time()))
    adapter._option_history_next = loop.time() + 1
    req_id = ib.client.getReqId()
    future = ib.wrapper.startReq(req_id, contract)
    errors = []
    def error(received_id, code, message, *args):
        if received_id == req_id:
            errors.append(code)
    ib.errorEvent += error
    try:
        if request.data_kind == "BAR":
            until = end.replace(microsecond=0) + (timedelta(seconds=1) if end.microsecond else timedelta())
            ib.client.reqHistoricalData(req_id, contract, until.strftime("%Y%m%d-%H:%M:%S"),
                "1 D", BAR_SIZES[request.timeframe], "TRADES", False, 2, False, [])
        else:
            ib.client.reqHistoricalTicks(req_id, contract, start.strftime("%Y%m%d-%H:%M:%S"), "", 1000,
                "BID_ASK" if request.data_kind == "QUOTE" else "TRADES", False, False, [])
        rows = await asyncio.wait_for(future, adapter._qualification_timeout)
        check(not errors, f"IB option history failed: {errors[0]}" if errors else "Invalid history response")
        check(len(rows) <= 100000, "Narrow the IB history query: native batch exceeded bound")
        return rows
    finally:
        ib.errorEvent -= error
        try:
            if request.data_kind == "BAR" and req_id in ib.wrapper._futures:
                ib.client.cancelHistoricalData(req_id)
        finally:
            ib.wrapper._endReq(req_id)


def normalize(row, request, canonical_id, at):
    if request.data_kind == "BAR":
        volume = size(row.volume)
        check(volume is not None, "IB option bar volume must be whole contracts")
        return OptionBar(canonical_id, wire(at), *(native_decimal(getattr(row, name))
                         for name in ("open", "high", "low", "close")), volume)
    if request.data_kind == "TRADE":
        count = size(row.size)
        check(count is not None and count > 0, "IB option trade size must be positive whole contracts")
        return OptionTrade(canonical_id, wire(at), native_decimal(row.price), count)
    bid, ask = native_decimal(row.priceBid), native_decimal(row.priceAsk)
    bs, az = size(row.sizeBid), size(row.sizeAsk)
    check(bs is not None and az is not None, "IB historical quote sizes must be whole contracts")
    return OptionQuote(canonical_id, bid, ask, bs, az, wire(at), now_wire(), "IBKR", request.feed,
        "RESEARCH_ONLY", request.account, None, None, None, None, None, None, None, None, None)


async def read_history(adapter, request):
    check(type(request) is OptionHistoryRequest and request.feed == "provider_native",
          "IB historical data requires explicit provider_native feed")
    if request.data_kind == "BAR" and request.timeframe not in BAR_SIZES:
        raise BrokerCapabilityError("IB option history supports native intraday bar sizes only", code="HISTORY_TIMEFRAME_UNSUPPORTED")
    query = asdict(request)
    query.pop("cursor")
    start, end = timestamp(request.time_from), timestamp(request.time_to)

    async def read(ib, bound):
        async with adapter._option_history_lock:
            contracts = adapter._option_bound_contracts(request)
            if request.cursor is None:
                index, position, pending = 0, start, ()
            else:
                cached = adapter._option_history_pages.get(request.cursor)
                check(cached is not None and cached[0] == query and timestamp(now_wire()) < cached[1],
                      "Invalid or expired IB history cursor")
                _, _, index, position, pending = cached
            if not pending and index < len(contracts):
                contract = contracts[index]
                expiry = datetime.strptime(contract.lastTradeDateOrContractMonth, "%Y%m%d").date()
                if expiry < datetime.now(ZoneInfo("America/New_York")).date():
                    raise BrokerCapabilityError("IB does not provide expired option history", code="EXPIRED_OPTION_HISTORY_UNAVAILABLE")
                chunk_end = min(position + timedelta(days=1), end)
                rows = await native_page(adapter, ib, contract, request, position, chunk_end)
                observed = []
                previous = None
                for row in rows:
                    at = row.date if request.data_kind == "BAR" else row.time
                    check(isinstance(at, datetime) and at.tzinfo is not None, "IB intraday history needs UTC timestamps")
                    check(previous is None or previous <= at, "IB historical response is out of order")
                    previous = at
                    if position <= at < (chunk_end if request.data_kind == "BAR" else end):
                        observed.append(normalize(row, request, request.bindings[index].canonical_id, at))
                pending = tuple(observed)
                if request.data_kind == "BAR":
                    position = chunk_end
                elif rows:
                    # IB completes the final second even when it exceeds 1000
                    # records. Keep all records in the cursor before advancing.
                    position = max(position, rows[-1].time) + timedelta(seconds=1)
                else:
                    position = next_day(position)
                if position >= end:
                    index, position = index + 1, start
            output, pending = pending[:request.limit], pending[request.limit:]
            cursor = None
            if pending or index < len(contracts):
                cursor = str(uuid4())
                adapter._option_history_pages[cursor] = (query, timestamp(now_wire()) + timedelta(minutes=5),
                                                         index, position, pending)
                while len(adapter._option_history_pages) > 32:
                    adapter._option_history_pages.popitem(last=False)
            return OptionHistoryPage(request.data_kind,
                output if request.data_kind == "BAR" else (), output if request.data_kind == "QUOTE" else (),
                output if request.data_kind == "TRADE" else (), cursor, cursor is None,
                "PARTIAL" if output else "UNAVAILABLE", "IBKR_NATIVE_HISTORICAL", now_wire())
    return await adapter._option_read(request, read)
