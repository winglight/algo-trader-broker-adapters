"""Finite stock BBO reads owned by one native IB request, not a shared ticker."""
import asyncio
from datetime import datetime, timezone

from ib_async import Ticker

from .options_quotes import amount


async def read_stock_snapshot(ib, contract, *, timeout, serialize):
    # Snapshot-end arrives later than the bid/ask ticks. A short live request
    # avoids that delay, without borrowing or canceling a chart subscription.
    req_id = ib.client.getReqId()
    ticker = Ticker(contract=contract, defaults=ib.wrapper.defaults)
    ticker.marketDataType = 0
    started = datetime.now(timezone.utc)
    sides, changed = {}, asyncio.Event()

    def update(value):
        for tick in value.ticks:
            if (tick.tickType in {0, 1, 2, 3, 66, 67, 69, 70}
                    and tick.time.tzinfo is not None and tick.time >= started):
                side = 'bid' if tick.tickType in {0, 1, 66, 69} else 'ask'
                sides[side] = (amount(tick.price), amount(tick.size), tick.time)
        changed.set()

    ticker.updateEvent += update
    ib.wrapper.reqId2Ticker[req_id] = ticker
    ib.wrapper._reqId2Contract[req_id] = contract
    try:
        ib.client.reqMktData(req_id, contract, '', False, False, [])
        deadline = asyncio.get_running_loop().time() + timeout
        while not (ticker.marketDataType in {1, 2, 3, 4} and len(sides) == 2
                   and all(price is not None and size is not None for price, size, _ in sides.values())):
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            changed.clear()
            try:
                await asyncio.wait_for(changed.wait(), remaining)
            except asyncio.TimeoutError:
                break
        result = serialize(ticker, contract=contract)
        result.update(symbol=contract.symbol, dataFeed={1: 'IBKR_LIVE', 2: 'IBKR_FROZEN',
            3: 'IBKR_DELAYED', 4: 'IBKR_DELAYED_FROZEN'}.get(ticker.marketDataType, 'UNKNOWN'))
        # IB supplies socket receipt times. Never label them exchange quote
        # times, or let a later last/model tick refresh an older BBO side.
        result['timestamp'] = min(side[2] for side in sides.values()).isoformat() if sides else None
        for side in ('bid', 'ask'):
            result[side], result[side + 'Size'] = sides.get(side, (None, None, None))[:2]
        return result
    finally:
        ticker.updateEvent -= update
        try:
            ib.client.cancelMktData(req_id)
        finally:
            ib.wrapper.reqId2Ticker.pop(req_id, None)
            ib.wrapper._reqId2Contract.pop(req_id, None)
            ib.wrapper.pendingTickers.discard(ticker)
