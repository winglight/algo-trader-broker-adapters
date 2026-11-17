from types import SimpleNamespace as N

import pytest

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_adapter_ibkr_paper.options_orders import acknowledgement


def fixture(combo=False):
    order = dict(account='DU-OPTIONS-FIXTURE', orderRef='fixture-command', orderId=42,
        clientId=3, totalQuantity=1, lmtPrice=1.25, action='BUY', openClose='O',
        orderType='LMT', tif='DAY', outsideRth=False, permId=123)
    request = N(command_id='fixture-command',client_order_id='fixture-command',groups=1,
        legs=(N(leg_id='L1',binding=N(broker_contract_id='456',canonical_id='fixture-option',exchange='SMART'),ratio=1,side='BUY'),))
    record = dict(kind='openOrder',order=order.copy(),contract=dict(secType='OPT',conId=456),
        state=dict(status='Filled'))
    if combo:
        request.legs += (N(leg_id='L2',binding=N(broker_contract_id='789',canonical_id='fixture-option-2',exchange='SMART'),ratio=1,side='SELL'),)
        record['contract'] = dict(secType='BAG',comboLegs=[dict(conId=456,ratio=1,action='BUY',exchange='SMART',openClose=0),dict(conId=789,ratio=1,action='SELL',exchange='SMART',openClose=0)])
    return request, N(**order), record


@pytest.mark.parametrize('combo',[False,True])
def test_fast_native_filled_parent_proves_acceptance_without_inventing_financial_fills(combo):
    request, order, record = fixture(combo)
    result = acknowledgement(request,order,[record])
    assert result.status == 'ACKNOWLEDGED' and result.parent_order_ref == '123'
    assert result.filled_groups == 0 and result.remaining_groups == 1
    assert all(leg.filled_contracts == 0 for leg in result.legs)
    assert record['state']['status'] == 'Filled'


@pytest.mark.parametrize('field,value',[('account','OTHER'),('totalQuantity',2),('lmtPrice',2.25),('permId',0)])
def test_filled_label_never_overrides_exact_command_or_native_identity(field,value):
    request, order, record = fixture()
    record['order'][field] = value
    if field == 'permId':
        result = acknowledgement(request,order,[record])
        assert result.status == 'SUBMISSION_UNKNOWN' and result.parent_order_ref is None
    else:
        with pytest.raises(BrokerContractError):acknowledgement(request,order,[record])
