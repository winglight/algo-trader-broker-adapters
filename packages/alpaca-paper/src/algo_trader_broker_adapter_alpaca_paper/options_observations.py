"""Native chain observations, with independent OI/day-volume/trade clocks."""
import asyncio
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from algo_trader_broker_sdk import BrokerError, BrokerContractError
from algo_trader_broker_sdk.options import check, timestamp
from .options_codec import _time, _decimal
from .raw_stream import decode_native

NY=ZoneInfo('America/New_York')


def integer(value):
    from decimal import Decimal
    number=Decimal(_decimal(value))
    check(number==number.to_integral_value() and 0<=number<=2**53-1,'Observation must be a nonnegative whole number')
    return int(number)


def catalog_oi(item):
    if not isinstance(item,dict):return None,None
    if item.get('open_interest') is None or item.get('open_interest_date') is None:return None,None
    try:
        day=date.fromisoformat(item['open_interest_date']).isoformat()
        return integer(item['open_interest']),day
    except (ValueError,TypeError,BrokerContractError):return None,None


def last_trade(item):
    trade=item.get('latestTrade')
    if not isinstance(trade,dict) or trade.get('p') is None or not trade.get('t'):return {}
    try:return dict(last_trade_price=_decimal(trade['p']),last_trade_at=_time(trade['t']))
    except (ValueError,TypeError,BrokerContractError):return {}


async def day_volumes(adapter,bindings):
    """Fetch once before the quote read; a volume observation cannot refresh BBO."""
    if not hasattr(adapter,'_option_day_volumes'):adapter._option_day_volumes={}
    cache=adapter._option_day_volumes
    now=datetime.now(timezone.utc)
    wanted=[b.local_symbol for b in bindings]
    missing=[symbol for symbol in wanted if symbol not in cache or (now-cache[symbol][0]).total_seconds()>=30]
    if missing:
        start=(now-timedelta(days=8)).isoformat().replace('+00:00','Z')
        try:
            async with asyncio.timeout(2):
                raw=await adapter._backend.get_option_resource('/v1beta1/options/bars',data=True,
                    params=dict(symbols=','.join(missing),start=start,end=now.isoformat().replace('+00:00','Z'),
                        timeframe='1Day',limit=10000,sort='asc'))
            body=decode_native(raw)
            check(isinstance(body,dict) and isinstance(body.get('bars'),dict) and not body.get('next_page_token'),
                'Complete bounded daily bars are required for volume observations')
            check(set(body['bars'])<=set(missing),'Daily volume contains an unrequested symbol')
            observed=datetime.now(timezone.utc)
            adapter._remember_option_read(raw)
            for symbol in missing:
                days={}
                for bar in body['bars'].get(symbol,[]):
                    at=_time(bar['t']);day=timestamp(at).astimezone(NY).date().isoformat()
                    check(day not in days,'Daily volume repeats a session')
                    days[day]=dict(volume=integer(bar['v']),volume_date=day,
                        volume_as_of=observed.isoformat().replace('+00:00','Z'))
                cache[symbol]=(observed,days)
        except (BrokerError,ValueError,TypeError,KeyError,TimeoutError):
            # Volume is independent from quotes. Missing bars stay unknown;
            # neither a permission failure nor a partial page becomes zero.
            for symbol in missing:cache[symbol]=(datetime.now(timezone.utc),{})
        while len(cache)>2500:cache.pop(next(iter(cache)))
    return {symbol:cache[symbol][1] for symbol in wanted if symbol in cache}


def quote_volume(volumes,symbol,quote_at):
    day=timestamp(quote_at).astimezone(NY).date().isoformat()
    return volumes.get(symbol,{}).get(day,{})
