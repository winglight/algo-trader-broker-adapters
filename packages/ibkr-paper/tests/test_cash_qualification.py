import pytest
from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_adapter_ibkr_paper import cash_qualification as native

CONTRACT=dict(symbol='SPY',secType='STK',currency='USD')
DETAILS=dict(contract={**CONTRACT,'conId':12345},minTick='0.01',sizeIncrement='1',minSize='1')


def test_market_rules_choose_exact_applicable_increment():
    details={**DETAILS,'priceIncrements':[dict(lowEdge='0',increment='0.0001'),dict(lowEdge='1',increment='0.0100')]}
    for limit,tick in [('0.99','0.0001'),('1','0.01'),('600','0.01')]:
        result=native.qualify([details],CONTRACT,limit)
        assert result.provider_id=='IBKR' and result.instrument_id=='IBKR:12345'
        assert result.price_step==tick and result.quantity_step=='1'


@pytest.mark.parametrize('change',[dict(conId=0),dict(conId=True),dict(conId='12345'),dict(symbol='QQQ'),dict(secType='OPT'),dict(currency='EUR')])
def test_invalid_native_identity_never_reaches_transport(monkeypatch,change):
    def forbidden(*a,**k):raise AssertionError('Invalid native identity reached transport')
    monkeypatch.setattr(native,'CashStockQualification',forbidden)
    with pytest.raises(BrokerContractError):native.qualify([{**DETAILS,'contract':{**DETAILS['contract'],**change}}],CONTRACT,'600')


@pytest.mark.parametrize('change',[dict(sizeIncrement='0'),dict(sizeIncrement='3'),dict(minSize='2'),dict(minTick='0'),dict(minTick=None),dict(minTick='NaN'),
    dict(priceIncrements=[]),dict(priceIncrements=[dict(lowEdge='1',increment='0.01')]),dict(priceIncrements=[dict(lowEdge='0',increment='-1')]),
    dict(priceIncrements=[dict(lowEdge='0',increment='0.01'),dict(lowEdge='0',increment='0.1')])])
def test_invalid_size_or_market_rules_never_reach_transport(monkeypatch,change):
    def forbidden(*a,**k):raise AssertionError('Invalid native increment reached transport')
    monkeypatch.setattr(native,'CashStockQualification',forbidden)
    with pytest.raises(BrokerContractError):native.qualify([{**DETAILS,**change}],CONTRACT,'0.99')
