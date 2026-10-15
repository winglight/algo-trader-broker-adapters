"""Normalize native tradable equity identity and limit increments."""
from decimal import Decimal
from hashlib import sha256
import json
from algo_trader_broker_sdk.cash_control import CashStockQualification
from algo_trader_broker_sdk.options import check
from .account_identity import stock_identity


def qualify(rows, contract, limit_price):
    check(type(rows) is list and len(rows) == 1 and type(rows[0]) is dict, 'Stock must resolve exactly one native asset')
    details = rows[0]
    native = details.get('contract', details)
    check(type(native) is dict and (native.get('symbol'), native.get('secType'), native.get('currency')) ==
          (contract.get('symbol'), 'STK', 'USD') and contract.get('currency') == 'USD', 'Native stock identity differs')
    check(native.get('tradable') is True, 'Native asset is not tradable')
    instrument = 'ALPACA:' + str(native.get('assetId'))
    stock_identity(instrument)
    price = Decimal(limit_price)
    check(price.is_finite() and price > 0, 'Stock limit must be positive')
    # Native equity limit precision changes at one dollar.
    tick = '0.01' if price >= 1 else '0.0001'
    raw_hash = sha256(json.dumps(details,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False,default=str).encode()).hexdigest()
    return CashStockQualification('ALPACA', instrument, native['symbol'], 'USD', '1', tick, raw_hash)
