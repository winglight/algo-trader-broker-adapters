"""Normalize exact native conId and market-rule increments for whole shares."""
from decimal import Decimal
from hashlib import sha256
import json
from algo_trader_broker_sdk.cash_control import CashStockQualification
from algo_trader_broker_sdk.options import check
from .account_identity import stock_identity


def number(value):
    try:
        result = Decimal(str(value))
    except Exception as exc:
        from algo_trader_broker_sdk import BrokerContractError
        raise BrokerContractError('Native stock increment is invalid') from exc
    check(result.is_finite(), 'Native stock increment must be finite')
    return result


def qualify(rows, contract, limit_price):
    check(type(rows) is list and len(rows) == 1 and type(rows[0]) is dict, 'Stock must resolve exactly one native contract')
    details = rows[0]
    native = details.get('contract', details)
    check(type(native) is dict and (native.get('symbol'), native.get('secType'), native.get('currency')) ==
          (contract.get('symbol'), 'STK', 'USD') and contract.get('currency') == 'USD', 'Native stock identity differs')
    con_id = native.get('conId')
    check(type(con_id) is int and con_id > 0, 'Stock requires its exact native conId')
    instrument = 'IBKR:' + str(con_id)
    stock_identity(instrument)
    step, minimum = number(details.get('sizeIncrement', '1')), number(details.get('minSize', '1'))
    check(step > 0 and 0 < minimum <= 1 and Decimal(1) % step == 0, 'Native size rules do not support whole shares')
    limit = number(limit_price)
    check(limit > 0, 'Stock limit must be positive')
    increments = details.get('priceIncrements')
    if increments is not None:
        check(type(increments) is list and bool(increments), 'Native price increments are unavailable')
        check(all(type(item) is dict and 'lowEdge' in item and 'increment' in item for item in increments), 'Native price rule is invalid')
        bands = [(number(item['lowEdge']), number(item['increment'])) for item in increments]
        check(all(edge >= 0 and tick > 0 for edge,tick in bands) and len({edge for edge,_ in bands}) == len(bands), 'Native price increments are invalid')
        applicable = sorted((edge,tick) for edge,tick in bands if edge <= limit)
        check(bool(applicable), 'Native price rules do not cover this limit')
        tick = applicable[-1][1]
    else:
        tick = number(details.get('minTick'))
    check(tick > 0, 'Native minimum tick must be positive')
    price_step = format(tick, 'f').rstrip('0').rstrip('.') if '.' in format(tick, 'f') else format(tick, 'f')
    raw_hash = sha256(json.dumps(details,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False,default=str).encode()).hexdigest()
    return CashStockQualification('IBKR', instrument, native['symbol'], 'USD', '1', price_step, raw_hash)
