import pytest
from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_adapter_ibkr_paper import options_events as native


@pytest.mark.parametrize('suffix,revision',[('01',1),('02',2),('100',100)])
def test_revision_attests_exact_native_source_and_original_family(suffix,revision):
    full='0001.fixture.980.'+suffix
    value=native.execution_revision(full)
    assert value.original_execution_id=='0001.fixture.980.01' and value.revision==revision
    assert value.provider_id=='IBKR' and value.order_namespace=='IBKR_PERM_ID' and value.native_execution_id==full


@pytest.mark.parametrize('native_id',['','opaque','native.0','native.00','native.1','native.001','native.bad',' native.01','native..01','native.01 ',
    'a.b.1','a.b.001','a.b.00','a.b.9007199254740992'])
def test_invalid_native_revision_never_reaches_transport(monkeypatch,native_id):
    def forbidden(*a,**k):raise AssertionError('Invalid native revision reached transport')
    monkeypatch.setattr(native,'OptionExecutionRevision',forbidden)
    with pytest.raises(BrokerContractError):native.execution_revision(native_id)
