"""Bounded IB live snapshots with request-owned tickers and subscriptions."""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import OptionMarketSnapshot, OptionQuote, timestamp

from .options_reads import native_decimal, now_wire
from .options_greeks import GreekCapture


def amount(value):
    try:
        return native_decimal(value)
    except BrokerContractError:
        return None


def size(value):
    number = amount(value)
    if number is None:
        return None
    number = Decimal(number)
    return int(number) if number == number.to_integral_value() else None


async def read_snapshot(ib, request, contracts, *, timeout):
    from ib_async import Ticker

    subscriptions, ticks, changed = [], {}, asyncio.Event()
    started = datetime.now(timezone.utc)
    greeks = GreekCapture(ib)

    def update(ticker):
        sides = ticks[id(ticker)]
        for tick in ticker.ticks:
            # These are IB socket receipt timestamps, not exchange timestamps.
            # A model/last update must not refresh an older bid or ask.
            if tick.tickType in {0, 1, 2, 3} and tick.time.tzinfo is not None and tick.time >= started:
                side = "bid" if tick.tickType in {0, 1} else "ask"
                sides[side] = (amount(tick.price), size(tick.size), tick.time)
        changed.set()

    try:
        for contract in contracts:
            req_id = ib.client.getReqId()
            # ib.reqMktData/startTicker reuses a ticker by contract hash. Give
            # this finite read its own request ID and ticker so cleanup cannot
            # cancel an existing strategy's subscription for the same conId.
            ticker = Ticker(contract=contract, defaults=ib.wrapper.defaults)
            ticker.marketDataType = 0  # Require a received native type callback.
            ticks[id(ticker)] = {}
            ticker.updateEvent += update
            ib.wrapper.reqId2Ticker[req_id] = ticker
            ib.wrapper._reqId2Contract[req_id] = contract
            subscriptions.append((req_id, ticker))
            greeks.register(req_id)
            ib.client.reqMktData(req_id, contract, "", False, False, [])
        deadline = asyncio.get_running_loop().time() + timeout
        while not all(len(ticks[id(ticker)]) == 2 for _, ticker in subscriptions):
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            changed.clear()
            try:
                await asyncio.wait_for(changed.wait(), remaining)
            except asyncio.TimeoutError:
                break
        received, quotes, missing = now_wire(), [], []
        all_side_times = []
        for binding, (req_id, ticker) in zip(request.bindings, subscriptions):
            sides = ticks[id(ticker)]
            if len(sides) != 2:
                missing.append(binding.canonical_id)
                continue
            all_side_times.extend((sides["bid"][2], sides["ask"][2]))
            quotes.append(quote_from_sides(binding, ticker, sides, request, received=received,
                native_greeks=greeks.observation(req_id, binding, ticker.marketDataType)))
        skew = int((max(all_side_times) - min(all_side_times)).total_seconds() * 1000) if all_side_times else 0
        return OptionMarketSnapshot(str(uuid4()), tuple(quotes), tuple(missing), not missing, skew, received)
    finally:
        greeks.close()
        cleanup_error = None
        for req_id, ticker in subscriptions:
            ticker.updateEvent -= update
            try:
                ib.client.cancelMktData(req_id)
            except Exception as exc:
                cleanup_error = exc
            finally:
                ib.wrapper.reqId2Ticker.pop(req_id, None)
                ib.wrapper._reqId2Contract.pop(req_id, None)
                ib.wrapper.pendingTickers.discard(ticker)
        if cleanup_error is not None:
            raise cleanup_error


def quote_from_sides(binding, ticker, sides, request, *, received=None, native_greeks=None):
    received = received or now_wire()
    bid, bs, bid_time = sides["bid"]
    ask, az, ask_time = sides["ask"]
    at = min(bid_time, ask_time)
    age = (timestamp(received) - at).total_seconds() * 1000
    quality = "EXECUTABLE"
    if None in (bid, ask, bs, az):
        quality = "MISSING"
    elif Decimal(bid) > Decimal(ask):
        quality = "CROSSED"
    elif age < 0 or age > request.max_age_ms:
        quality = "STALE"
    elif ticker.marketDataType != 1:
        quality = "RESEARCH_ONLY"
    # Native model Greeks have no certified input timestamp/unit here.
    return OptionQuote(binding.canonical_id, bid, ask, bs, az,
        at.isoformat().replace("+00:00", "Z"), received, "IBKR", request.feed,
        quality, request.account, None, None, None, None, None, None, None, None, None, native_greeks)
