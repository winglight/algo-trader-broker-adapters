"""Exact native historical bars/trades; neither dataset is historical quotes."""

from datetime import timedelta

from algo_trader_broker_sdk import BrokerCapabilityError
from algo_trader_broker_sdk.options import OptionBar, OptionTrade, OptionHistoryRequest, OptionHistoryPage, check, timestamp

from .options_codec import _decimal, _time
from .options_reads import now_wire, whole
from .raw_stream import decode_native


async def read_history(adapter, request):
    check(type(request) is OptionHistoryRequest, "Expected an exact option history request")
    if request.data_kind == "QUOTE":
        raise BrokerCapabilityError("Alpaca historical option quotes are not available", code="UNSUPPORTED_CAPABILITY")
    check(request.feed == "provider_native", "Native history must not imply an OPRA or indicative quote feed")
    bound, _, _ = await adapter._option_bound(request)
    adapter._validate_option_bindings(bound, request.bindings)
    dataset = "bars" if request.data_kind == "BAR" else "trades"
    start, end = timestamp(request.time_from), timestamp(request.time_to)
    # Domain ranges are half-open; the vendor's end parameter is inclusive.
    last_microsecond = (end - timedelta(microseconds=1)).isoformat(timespec="microseconds").removesuffix("+00:00")
    params = dict(symbols=",".join(binding.local_symbol for binding in request.bindings), start=request.time_from,
        end=last_microsecond + "999Z", limit=request.limit, sort="asc")
    if request.data_kind == "BAR": params["timeframe"] = request.timeframe
    if request.cursor is not None: params["page_token"] = request.cursor
    raw = await adapter._backend.get_option_resource("/v1beta1/options/" + dataset, params=params, data=True)
    adapter._remember_option_read(raw)
    body = decode_native(raw)
    check(type(body) is dict and type(body.get(dataset)) is dict, "Malformed native option history")
    cursor = body.get("next_page_token")
    check(cursor is None or (type(cursor) is str and 0 < len(cursor) <= 4096 and cursor != request.cursor),
          "Native history pagination did not advance")
    bindings = {binding.local_symbol: binding for binding in request.bindings}
    check(set(body[dataset]) <= set(bindings), "History contains an unrequested native contract")
    bars, trades, count = [], [], 0
    for symbol, rows in body[dataset].items():
        check(type(rows) is list, "Malformed native history rows")
        count += len(rows)
        check(count <= request.limit, "Native history exceeds the total page limit")
        previous = None
        for item in rows:
            at = _time(item["t"])
            current = timestamp(at)
            check(start <= current < end and (previous is None or previous <= current), "Native history time differs from requested range/order")
            previous = current
            identity = bindings[symbol].canonical_id
            if request.data_kind == "BAR":
                bars.append(OptionBar(identity, at, *(_decimal(item[key]) for key in ("o", "h", "l", "c")), whole(item["v"])))
            else:
                trades.append(OptionTrade(identity, at, _decimal(item["p"]), whole(item["s"])))
    adapter._option_still_bound(bound)
    # Exhausted acquisition is distinct from a certified time-series coverage
    # manifest. That independent coverage producer remains responsible for it.
    return OptionHistoryPage(request.data_kind, tuple(bars), (), tuple(trades), cursor, cursor is None,
        "PARTIAL" if count or cursor else "UNAVAILABLE", "ALPACA:provider_native", now_wire())
