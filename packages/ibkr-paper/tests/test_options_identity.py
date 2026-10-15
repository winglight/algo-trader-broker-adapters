import json
from dataclasses import replace
import pytest
from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import OptionScope
from algo_trader_broker_sdk.options_events import OptionRawEvent
from algo_trader_broker_adapter_ibkr_paper import options_identity as native


def event(environment='paper',account='DU12345',**changes):
    value=dict(kind='orderStatus',native_account_ref=account,state={'status':'ApiPending'},
        order=dict(account=account,clientId=1,orderId=2390,permId=0,orderRef='atiopt_'+'a'*40,
                   scalePriceAdjustInterval=9223372036854775807))
    value.update(changes)
    scope=OptionScope('options/1.0','a'*64,'profile',11,'account',environment)
    return OptionRawEvent(scope,'IB_OPTION_CALLBACK',json.dumps(value).encode(),None)


@pytest.mark.parametrize('environment',['paper','live'])
@pytest.mark.parametrize('account',['DU12345','U12345','F12345'])
def test_pending_identity_preserves_scoped_native_id_without_financial_status(environment,account):
    original=event(environment,account)
    notice=native.pending_identity(original)
    assert notice.scope==original.scope and notice.provider_id=='IBKR'
    assert notice.order_id=='api:1:2390' and notice.session_key=='IBKR_API_GENERATION_11'
    assert not hasattr(notice,'status') and not hasattr(notice,'contracts')
    wire=json.loads(original.raw_payload)
    wire['order']['permId']=123
    notice=native.pending_identity(replace(original,raw_payload=json.dumps(wire).encode()))
    assert notice.order_id=='123' and notice.session_key=='IBKR_PERM_ID'


@pytest.mark.parametrize('change',[dict(account='OTHER'),dict(clientId=-1),dict(orderId=0),dict(permId=-1),dict(permId=True),dict(orderRef='other')])
def test_invalid_native_pending_identity_never_reaches_transport(monkeypatch,change):
    original=event()
    wire=json.loads(original.raw_payload)
    wire['order'].update(change)
    def forbidden(*a,**k):raise AssertionError('Invalid native identity reached transport')
    monkeypatch.setattr(native,'OptionNativeOrderIdentity',forbidden)
    with pytest.raises(BrokerContractError):native.pending_identity(replace(original,raw_payload=json.dumps(wire).encode()))


def test_other_callbacks_do_not_acquire_pending_identity_and_duplicate_fields_are_rejected():
    assert native.pending_identity(event(kind='execDetails')) is None
    assert native.pending_identity(event(state=dict(status='Filled'))) is None
    original=event()
    with pytest.raises(BrokerContractError):native.pending_identity(replace(original,raw_payload=b'{"kind":1,"kind":2}'))


def test_empty_submission_outcome_retains_prepared_identity_without_acknowledging():
    original=event()
    value=json.loads(original.raw_payload)
    value.update(source='IB_OPTION_PREPARATION_V1')
    submission=replace(original,source='IB_OPTION_SUBMISSION',raw_payload=json.dumps(dict(preparation=json.dumps(value),observations=[])).encode())
    assert native.pending_identity(submission).order_id=='api:1:2390'
    assert native.pending_identity(replace(submission,raw_payload=json.dumps(dict(observations=[dict(kind='openOrder')])).encode())) is None
