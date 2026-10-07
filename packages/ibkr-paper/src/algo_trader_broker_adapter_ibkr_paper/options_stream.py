"""Request-owned option tickers on the bound IB connection and Runner lease."""

import asyncio
from datetime import datetime, timezone

from algo_trader_broker_sdk import BrokerConnectionError
from algo_trader_broker_sdk.options import QuoteSubscription, check

from .options_quotes import amount, size, quote_from_sides
from .options_reads import FEED


async def stream_quotes(adapter, request):
    from ib_async import Ticker
    check(type(request) is QuoteSubscription and request.feed == FEED,
          "IB quote subscription requires explicit IBKR_LIVE feed")
    async def open_connection(ib, bound):
        return ib, bound, adapter._option_bound_contracts(request, fresh=True)
    ib, bound, contracts = await adapter._option_read(request, open_connection)
    connection = adapter._option_connection
    subscriptions, sides, dirty, errors = {}, {}, set(), []
    changed = asyncio.Event()
    started = datetime.now(timezone.utc)

    def update(ticker):
        row = sides[id(ticker)]
        for tick in ticker.ticks:
            if tick.tickType in {0, 1, 2, 3} and tick.time.tzinfo is not None and tick.time >= started:
                row["bid" if tick.tickType in {0, 1} else "ask"] = (amount(tick.price), size(tick.size), tick.time)
                dirty.add(id(ticker))
        changed.set()

    def error(req_id, code, message, *args):
        if req_id in subscriptions:
            errors.append(code)
            changed.set()

    async def current(native, verified):
        check(native is ib and verified == bound and adapter._option_connection == connection,
              "IB option subscription connection changed")
        if errors:
            raise BrokerConnectionError(f"IB option quote subscription failed: {errors[0]}")

    ib.errorEvent += error
    try:
        for binding, contract in zip(request.bindings, contracts):
            req_id = ib.client.getReqId()
            ticker = Ticker(contract=contract, defaults=ib.wrapper.defaults)
            ticker.marketDataType = 0
            sides[id(ticker)] = {}
            ticker.updateEvent += update
            subscriptions[req_id] = (binding, ticker)
            ib.wrapper.reqId2Ticker[req_id] = ticker
            ib.wrapper._reqId2Contract[req_id] = contract
            ib.client.reqMktData(req_id, contract, "", False, False, [])
        while True:
            await adapter._option_read(request, current)
            changed.clear()
            # Latest bid/ask per contract: bounded by the qualified universe.
            ready = dirty.copy()
            dirty.difference_update(ready)
            for binding, ticker in subscriptions.values():
                if id(ticker) in ready and len(sides[id(ticker)]) == 2:
                    await adapter._option_read(request, current)
                    yield quote_from_sides(binding, ticker, sides[id(ticker)], request)
            try:
                await asyncio.wait_for(changed.wait(), 1)
            except asyncio.TimeoutError:
                pass
    finally:
        ib.errorEvent -= error
        cleanup_error = None
        for req_id, (_, ticker) in subscriptions.items():
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
