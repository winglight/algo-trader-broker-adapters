from copy import deepcopy
import pytest
from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_adapter_alpaca_paper import cash_qualification as native

CONTRACT=dict(symbol='SPY',secType='STK',currency='USD')
DETAILS={**CONTRACT,'assetId':'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa','tradable':True}


@pytest.mark.parametrize('price,tick',[('0.99','0.0001'),('1','0.01'),('600.25','0.01')])
def test_qualified_identity_uses_native_price_precision(price,tick):
    value=native.qualify([DETAILS],CONTRACT,price)
    assert value.provider_id=='ALPACA' and value.instrument_id=='ALPACA:'+DETAILS['assetId']
    assert value.price_step==tick and value.quantity_step=='1'


@pytest.mark.parametrize('change',[dict(assetId='bad'),dict(assetId=None),dict(tradable=False),dict(symbol='QQQ'),dict(secType='OPT'),dict(currency='EUR')])
def test_invalid_native_stock_never_reaches_normalized_transport(monkeypatch,change):
    def forbidden(*a,**k):raise AssertionError('Invalid native identity reached transport')
    monkeypatch.setattr(native,'CashStockQualification',forbidden)
    with pytest.raises(BrokerContractError):native.qualify([{**DETAILS,**change}],CONTRACT,'600')


@pytest.mark.parametrize('rows',[[],[DETAILS,DETAILS],[None]])
def test_ambiguous_or_missing_assets_are_rejected(rows):
    with pytest.raises(BrokerContractError):native.qualify(rows,CONTRACT,'600')
