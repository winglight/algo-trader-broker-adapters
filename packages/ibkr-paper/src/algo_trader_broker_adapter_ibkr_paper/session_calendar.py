"""IB dated session-hours normalization; only the Runner adapter sees native fields."""
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from algo_trader_broker_sdk.errors import BrokerContractError

class BrokerSessionCalendarError(BrokerContractError):
    pass


def _local_boundary(value: str, day: str, zone: ZoneInfo) -> datetime:
    token = value.strip()
    if ":" in token:
        raw_day, clock = token.split(":", 1)
    else:
        raw_day, clock = day, token
    if len(raw_day) != 8 or len(clock) != 4 or not (raw_day + clock).isdigit():
        raise BrokerSessionCalendarError("BROKER_SESSION_BOUNDARY_INVALID")
    naive = datetime.strptime(raw_day + clock.replace("2400", "0000"), "%Y%m%d%H%M")
    if clock == "2400":
        naive += timedelta(days=1)
    local = naive.replace(tzinfo=zone)
    # Reject ambiguous/nonexistent local boundaries rather than choosing a
    # fold or silently normalizing a DST gap without broker evidence.
    if local.astimezone(UTC).astimezone(zone).replace(tzinfo=None) != naive:
        raise BrokerSessionCalendarError("BROKER_SESSION_DST_GAP")
    if local.utcoffset() != naive.replace(tzinfo=zone, fold=1).utcoffset():
        raise BrokerSessionCalendarError("BROKER_SESSION_DST_AMBIGUOUS")
    return local.astimezone(UTC)


def parse_broker_session_windows(detail: Mapping[str, Any], *, session: str, symbol: str) -> list[dict[str, str]]:
    """Parse IB-style explicit dated hours, including CLOSED/early-close rows."""
    if session not in {"RTH", "ETH"}:
        raise BrokerSessionCalendarError("BROKER_SESSION_SELECTION_MISSING")
    zone_name = str(detail.get("timeZoneId") or "").strip()
    # Reject ambiguous US fixed-offset abbreviations, but accept valid IANA
    # aliases (for example CET/EST5EDT/GB-Eire) as well as Area/Location keys.
    if not zone_name or zone_name in {"EST", "MST", "HST"}:
        raise BrokerSessionCalendarError("BROKER_SESSION_TIMEZONE_INVALID")
    try:
        zone = ZoneInfo(zone_name)
    except ZoneInfoNotFoundError as exc:
        raise BrokerSessionCalendarError("BROKER_SESSION_TIMEZONE_INVALID") from exc
    raw = detail.get("liquidHours" if session == "RTH" else "tradingHours")
    if not isinstance(raw, str) or not raw.strip():
        raise BrokerSessionCalendarError("BROKER_SESSION_HOURS_MISSING")
    rows: list[dict[str, str]] = []
    for segment in raw.split(";"):
        if not segment.strip():
            continue
        try:
            day, hours = segment.strip().split(":", 1)
            datetime.strptime(day, "%Y%m%d")
            if hours == "CLOSED":
                continue
            intervals = []
            for interval in hours.split(","):
                left, right = interval.split("-", 1)
                opened = _local_boundary(left, day, zone)
                closed = _local_boundary(right, day, zone)
                if opened >= closed:
                    raise BrokerSessionCalendarError("BROKER_SESSION_BOUNDARY_INVALID")
                intervals.append((opened, closed))
            intervals.sort()
            if not intervals or any(a[1] > b[0] for a, b in zip(intervals, intervals[1:])):
                raise BrokerSessionCalendarError("BROKER_SESSION_BOUNDARY_INVALID")
            # Intraday trading pauses do not create a new overnight boundary.
            opened, closed = intervals[0][0], intervals[-1][1]
            rows.append({"symbol": symbol.upper(), "date": day,
                         "startTime": opened.isoformat(), "sessionOpen": opened.isoformat(),
                         "sessionClose": closed.isoformat(), "session": session,
                         "timezone": zone_name, "calendarSource": "broker_contract_details",
                         "sessionProfile": "BROKER_LIQUID_HOURS" if session == "RTH" else "BROKER_TRADING_HOURS"})
        except (ValueError, TypeError) as exc:
            if isinstance(exc, BrokerSessionCalendarError):
                raise
            raise BrokerSessionCalendarError("BROKER_SESSION_HOURS_INVALID") from exc
    rows.sort(key=lambda row: row["sessionOpen"])
    if any(a["sessionClose"] > b["sessionOpen"] for a, b in zip(rows, rows[1:])):
        raise BrokerSessionCalendarError("BROKER_SESSION_OVERLAP")
    if not rows:
        raise BrokerSessionCalendarError("BROKER_SESSION_CALENDAR_EMPTY")
    return rows


async def request_calendar(adapter, identity, *, symbol, session, session_profile=None, timestamp):
    if session not in {'RTH','ETH'}:
        raise BrokerSessionCalendarError('BROKER_SESSION_SELECTION_MISSING')
    expected = 'BROKER_LIQUID_HOURS' if session == 'RTH' else 'BROKER_TRADING_HOURS'
    if session_profile not in (None,'',expected):
        raise BrokerSessionCalendarError('BROKER_SESSION_PROFILE_MISMATCH')
    identity = dict(identity)
    con_id = identity.get('conId',identity.get('con_id',identity.get('contractId',identity.get('contract_id'))))
    if con_id is not None:
        if isinstance(con_id,bool) or not str(con_id).isdigit() or int(con_id) <= 0:
            raise BrokerSessionCalendarError('BROKER_SESSION_CONTRACT_INVALID')
        identity['conId'] = int(con_id)
        for alias in ('con_id','contractId','contract_id'):
            identity.pop(alias,None)
    elif not (identity.get('localSymbol') and identity.get('exchange') and identity.get('secType')):
        raise BrokerSessionCalendarError('BROKER_SESSION_QUALIFIED_CONTRACT_REQUIRED')
    matches = []
    for detail in await adapter.request_contract_details(identity):
        actual = detail.get('contract') if isinstance(detail,Mapping) else None
        if not isinstance(actual,Mapping) or symbol.upper() not in {str(actual.get(k) or '').upper() for k in ('symbol','localSymbol')}:
            continue
        if con_id is not None:
            if str(actual.get('conId')) != str(int(con_id)):
                continue
        elif any(str(actual.get(k) or '') != str(identity[k]) for k in ('localSymbol','exchange','secType')):
            continue
        matches.append(detail)
    if len(matches) != 1:
        raise BrokerSessionCalendarError('BROKER_SESSION_CONTRACT_AMBIGUOUS')
    return parse_broker_session_windows(matches[0],session=session,symbol=symbol)
