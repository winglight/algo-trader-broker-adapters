"""Last native BID_ASK ticks for closed-session research, never execution."""
import asyncio
import logging
from dataclasses import replace
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import OptionQuote, timestamp
from .options_calendar import session
from .options_quotes import amount, size
from .options_reads import now_wire

log = logging.getLogger(__name__)


def closed_bindings(adapter, request, now):
    if request.purpose != 'RESEARCH':
        return set()
    result = set()
    day = now.astimezone(ZoneInfo('America/New_York')).date().isoformat()
    for binding in request.bindings:
        detail = adapter._option_catalog[binding.broker_contract_id][2]
        try:
            native = session(binding, detail, SimpleNamespace(trade_date=day))
            if not native.is_trading_day or not timestamp(native.session_open_at) <= now < timestamp(native.session_close_at):
                result.add(binding.canonical_id)
        except (BrokerContractError, ValueError, AttributeError):
            # Unknown calendar is not permission to substitute old prices.
            continue
    return result


async def last_bbo(adapter, ib, contract, deadline):
    cached = adapter._option_reference_quotes.get(contract.conId)
    now = datetime.now(timezone.utc)
    if (cached and asyncio.get_running_loop().time() - cached[0] < 300
            and timedelta(0) <= now - cached[1][4] <= timedelta(days=7)):
        adapter._option_reference_quotes.move_to_end(contract.conId)
        return cached[1]
    async with adapter._option_reference_slots:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return None
        req_id = ib.client.getReqId()
        future = ib.wrapper.startReq(req_id, contract)
        errors = []
        def error(received_id, code, message, *args):
            if received_id == req_id:
                errors.append(code)
        ib.errorEvent += error
        try:
            # A backwards native tick request gives actual paired prices and
            # their source time. OHLC/last trade values are not bid/ask.
            ib.client.reqHistoricalTicks(req_id, contract, '', now.strftime('%Y%m%d-%H:%M:%S'),
                1000, 'BID_ASK', False, False, [])
            rows = await asyncio.wait_for(asyncio.shield(future), remaining)
            if errors:
                log.warning('Closed-session option BID_ASK unavailable; native codes=%s', errors)
                return None
            valid = []
            for row in rows:
                at = row.time
                bid, ask, bs, az = amount(row.priceBid), amount(row.priceAsk), size(row.sizeBid), size(row.sizeAsk)
                if (isinstance(at, datetime) and at.tzinfo is not None
                        and timedelta(0) <= now - at <= timedelta(days=7)
                        and None not in (bid, ask, bs, az) and 0 <= Decimal(bid) <= Decimal(ask) and Decimal(ask) > 0):
                    valid.append((bid, ask, bs, az, at))
            latest = max(valid, key=lambda value: value[4]) if valid else None
            if latest:
                adapter._option_reference_quotes[contract.conId] = (asyncio.get_running_loop().time(), latest)
                while len(adapter._option_reference_quotes) > 20000:
                    adapter._option_reference_quotes.popitem(last=False)
            return latest
        except (TimeoutError, BrokerContractError):
            return None
        finally:
            ib.errorEvent -= error
            if ib.wrapper._futures.get(req_id) is future:
                ib.wrapper._endReq(req_id)


async def with_references(adapter, ib, request, contracts, snapshot, closed, deadline):
    if request.purpose != 'RESEARCH' or not closed:
        return snapshot
    selected = [(binding, contract) for binding, contract in zip(request.bindings, contracts)
                if binding.canonical_id in closed]
    tasks = [asyncio.create_task(last_bbo(adapter, ib, contract, deadline)) for _, contract in selected]
    try:
        async with asyncio.timeout(max(0, deadline - asyncio.get_running_loop().time())):
            results = await asyncio.gather(*tasks, return_exceptions=True)
    except TimeoutError:
        results = [task.result() if task.done() and not task.cancelled() and task.exception() is None else None for task in tasks]
    finally:
        for task in tasks:
            if not task.done(): task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    references = {binding.canonical_id: value for (binding, _), value in zip(selected, results)
                  if value is not None and not isinstance(value, BaseException)}
    received = now_wire()
    quotes = {quote.canonical_id: quote for quote in snapshot.quotes}
    for cid, value in references.items():
        fields = dict(bid=value[0], ask=value[1], bid_size=value[2], ask_size=value[3],
            quote_at=value[4].astimezone(timezone.utc).isoformat().replace('+00:00', 'Z'), received_at=received,
            quality='RESEARCH_ONLY', reference_pricing_allowed=True)
        quotes[cid] = (replace(quotes[cid], **fields) if cid in quotes else OptionQuote(
            canonical_id=cid, provider='IBKR', feed=request.feed, entitlement_scope=request.account,
            delta=None, gamma=None, vega_per_vol_point=None, theta_per_day=None, iv_decimal=None,
            greeks_source=None, greeks_as_of=None, oi=None, oi_date=None, **fields))
    quotes = tuple(quotes.values())
    missing = tuple(cid for cid in snapshot.missing_contract_ids if cid not in references)
    times = [timestamp(quote.quote_at) for quote in quotes]
    skew = int((max(times) - min(times)).total_seconds() * 1000) if times else 0
    return replace(snapshot, quotes=quotes, missing_contract_ids=missing, complete=not missing,
        max_leg_skew_ms=skew, observed_at=received)
