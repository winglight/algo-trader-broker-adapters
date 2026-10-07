"""Fresh native contract sessions, with broker cutoffs kept explicitly unknown."""

import asyncio
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from zoneinfo import ZoneInfo

from algo_trader_broker_sdk.options import check, timestamp
from algo_trader_broker_sdk.options_calendar import (
    OptionCalendarQuery, OptionCalendar, OptionCalendarSource, OptionTradingSession,
)

from .options_reads import now_wire

SOURCE = "https://interactivebrokers.github.io/tws-api/classIBApi_1_1ContractDetails.html"


def session(binding, detail, request):
    check(detail.timeZoneId in {"US/Eastern", "America/New_York", "EST5EDT"},
          "IB option calendar requires explicit Eastern session timezone")
    key = request.trade_date.replace("-", "")
    rows = [row for row in detail.liquidHours.split(";") if row.startswith(key + ":")]
    check(len(rows) == 1, "Requested date is absent or ambiguous in IB liquidHours")
    if rows[0] == key + ":CLOSED":
        return OptionTradingSession(binding.canonical_id, request.trade_date, False,
            None, None, None, None, None, ("MARKET_CLOSED",))
    # P0 has one continuous RTH interval. Do not combine split sessions across
    # a break into a tradable interval or substitute stock market hours.
    values = rows[0].split("-")
    check(len(values) == 2 and "," not in rows[0], "Unsupported split IB option session")
    if ":" not in values[1]:
        values[1] = key + ":" + values[1]
    zone = ZoneInfo("America/New_York")
    times = [datetime.strptime(value, "%Y%m%d:%H%M").replace(tzinfo=zone)
             .astimezone(timezone.utc).isoformat().replace("+00:00", "Z") for value in values]
    return OptionTradingSession(binding.canonical_id, request.trade_date, True, *times,
        None, None, None, ("BROKER_ENTRY_CUTOFF_UNKNOWN", "BROKER_CLOSE_CUTOFF_UNKNOWN", "EXERCISE_CUTOFF_UNKNOWN"))


async def read_calendar(adapter, request):
    check(type(request) is OptionCalendarQuery, "Expected an exact option calendar query")
    async def read(ib, bound):
        contracts = adapter._option_bound_contracts(request)
        sessions, evidence = [], []
        for binding, contract in zip(request.bindings, contracts):
            details = await asyncio.wait_for(ib.reqContractDetailsAsync(contract), adapter._qualification_timeout)
            check(len(details) == 1 and details[0].contract == contract, "IB calendar contract changed")
            detail = details[0]
            sessions.append(session(binding, detail, request))
            evidence.append(dict(conId=contract.conId, tradingHours=detail.tradingHours,
                                 liquidHours=detail.liquidHours, timeZoneId=detail.timeZoneId))
        observed = now_wire()
        expires = (timestamp(observed) + timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
        revision = "ib-native-sessions-v1:" + sha256(json.dumps(evidence, sort_keys=True).encode()).hexdigest()
        return OptionCalendar(bound.scope, request.trade_date, "America/New_York", observed, expires,
            revision, (OptionCalendarSource(SOURCE, observed, observed, expires, revision),), tuple(sessions))
    return await adapter._option_read(request, read)
