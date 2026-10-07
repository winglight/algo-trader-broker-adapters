"""Native calendar plus versioned Alpaca Trading API rules for P0 SPY/QQQ.

Rule validity is ATI's review window, not a promise that the broker cannot
change its policy. Unknown special-session cutoffs remain explicitly missing.
"""

from datetime import datetime, timedelta, timezone
from hashlib import sha256
import re
from zoneinfo import ZoneInfo

from algo_trader_broker_sdk.options import check, timestamp
from algo_trader_broker_sdk.options_calendar import (
    OptionCalendar, OptionCalendarQuery, OptionCalendarSource, OptionTradingSession,
)
from .options_reads import decode_native, now_wire

NY = ZoneInfo("America/New_York")
RULE_REVISION = "alpaca-spy-qqq-calendar-2026-10-07"
RULE_CHECKED = "2026-10-07T00:00:00Z"
RULE_VALID_UNTIL = "2026-11-07T00:00:00Z"
RULE_URLS = (
    "https://alpaca.markets/support/when-do-options-trade",
    "https://alpaca.markets/support/what-are-the-cutoff-times-for-trading-0dte-options-on-alpaca",
    "https://docs.alpaca.markets/us/docs/options-trading",
)


def wire_time(day, value):
    check(type(value) is str and bool(re.fullmatch(r"\d{2}:\d{2}(?::\d{2})?", value)), "Invalid native calendar time")
    try:
        return datetime.fromisoformat(day + "T" + value).replace(tzinfo=NY).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except ValueError:
        check(False, "Invalid native calendar time")


async def read_calendar(adapter, request):
    check(type(request) is OptionCalendarQuery, "Expected an exact option calendar query")
    check("1970-01-01" <= request.trade_date <= "2029-12-31", "Date is outside the documented native calendar coverage")
    bound, _, _ = await adapter._option_bound(request)
    adapter._validate_option_bindings(bound, request.bindings)
    raw = await adapter._backend.get_option_resource("/v2/calendar",
        params=dict(start=request.trade_date, end=request.trade_date, date_type="TRADING"))
    data = decode_native(raw)
    check(type(data) is list and len(data) <= 1 and all(type(row) is dict and row.get("date") == request.trade_date for row in data), "Native calendar changed the requested day")
    raw_hash = adapter._remember_option_read(raw)
    observed = now_wire()
    valid_rule = timestamp(RULE_CHECKED) <= timestamp(observed) < timestamp(RULE_VALID_UNTIL) and RULE_CHECKED[:10] <= request.trade_date < RULE_VALID_UNTIL[:10]
    sessions = []
    for binding in request.bindings:
        _, contract, _ = adapter._option_catalog[binding.broker_contract_id]
        reasons = []
        opened = closed = entry = close = exercise = None
        trading = bool(data)
        if not trading:
            reasons.append("MARKET_CLOSED")
        else:
            opened, closed = (wire_time(request.trade_date, data[0][name]) for name in ("open", "close"))
            check(opened < closed, "Native session interval is invalid")
            if contract.key.underlying not in {"SPY", "QQQ"}:
                reasons.append("CALENDAR_PRODUCT_UNSUPPORTED")
            elif not valid_rule:
                reasons.append("CALENDAR_RULE_REVIEW_REQUIRED")
            elif contract.key.expiry < request.trade_date:
                reasons.append("CONTRACT_EXPIRED")
            else:
                # The Trading API supports regular hours only. These are the
                # broker's effective limits, not the exchange's last trade.
                close = exercise = closed
                entry = closed
                if contract.key.expiry == request.trade_date:
                    if data[0]["open"] in {"09:30", "09:30:00"} and data[0]["close"] in {"16:00", "16:00:00"}:
                        # Official support explicitly names SPY/QQQ and says
                        # orders after 15:30 are rejected; apply to both intents.
                        entry = close = wire_time(request.trade_date, "15:30")
                    else:
                        entry = close = None
                        reasons.append("EXPIRY_SPECIAL_SESSION_CUTOFF_UNKNOWN")
        sessions.append(OptionTradingSession(binding.canonical_id, request.trade_date, trading,
            opened, closed, entry, close, exercise, tuple(reasons)))
    expiry = timestamp(observed) + timedelta(minutes=5)
    if valid_rule: expiry = min(expiry, timestamp(RULE_VALID_UNTIL))
    expires = expiry.isoformat().replace("+00:00", "Z")
    sources = (OptionCalendarSource("https://docs.alpaca.markets/us/reference/legacycalendar", observed, observed, expires,
        "native-sha256:" + raw_hash),) + tuple(OptionCalendarSource(url, RULE_CHECKED, RULE_CHECKED, RULE_VALID_UNTIL, RULE_REVISION) for url in RULE_URLS)
    adapter._option_still_bound(bound)
    revision = sha256((raw_hash + RULE_REVISION).encode()).hexdigest()
    return OptionCalendar(bound.scope, request.trade_date, "America/New_York", observed, expires, revision, sources, tuple(sessions))
