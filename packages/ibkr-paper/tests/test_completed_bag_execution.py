import json
from copy import deepcopy

import pytest

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import OptionScope
from algo_trader_broker_sdk.options_events import OptionRawEvent
from algo_trader_broker_adapter_ibkr_paper.options_events import decode


@pytest.mark.asyncio
async def test_completed_bag_missing_total_keeps_identity_without_financial_status():
    scope = OptionScope('options/1.0', 'a' * 64, 'ibkr_paper', 11, 'paper-account', 'paper')
    account, reference, at = 'DU-OPTIONS-FIXTURE', 'atiopt_fixture', '2026-10-13T17:48:58Z'
    raw = dict(kind='execDetails', native_account_ref=account, observed_at=at, contract=dict(secType='BAG', conId=0),
        execution=dict(acctNumber=account, modelCode='', permId=123, execId='0001.fixture.bag.01',
            pendingPriceRevision=False, orderRef=reference, time=at, cumQty=1.0),
        parent=dict(contract=dict(secType='BAG'), order=dict(account=account, permId=123,
            orderRef=reference, orderId=0, clientId=0, totalQuantity=0.0)))
    result = await decode(OptionRawEvent(scope, 'IB_OPTION_CALLBACK', json.dumps(raw).encode(), None), None)
    assert result.order_id == '123'
    assert result.client_order_ref == reference
    assert result.status is None and not result.legs and not result.executions and not result.fees
    raw['parent']['order']['totalQuantity'] = 1
    raw['execution']['cumQty'] = 2
    with pytest.raises(BrokerContractError, match='parent quantity is unresolved'):
        await decode(OptionRawEvent(scope, 'IB_OPTION_CALLBACK', json.dumps(raw).encode(), None), None)


@pytest.mark.asyncio
@pytest.mark.parametrize('environment', ['paper', 'live'])
async def test_bag_execution_before_parent_cache_retains_identity_only(environment):
    scope = OptionScope('options/1.0', 'a' * 64, 'ibkr_paper', 11, 'account', environment)
    account, at = 'DU-OPTIONS-FIXTURE', '2026-11-13T17:48:58Z'
    raw = dict(kind='execDetails', native_account_ref=account, observed_at=at,
        contract=dict(secType='BAG', conId=0), parent=None,
        execution=dict(acctNumber=account, modelCode='', permId=123,
            execId='0001.fixture.bag.01', pendingPriceRevision=False,
            orderRef='atiopt_fixture', time=at, cumQty=1.0))
    def event(value):
        return OptionRawEvent(scope, 'IB_OPTION_CALLBACK', json.dumps(value).encode(), None)
    original = event(raw)
    result = await decode(original, None)
    assert result.scope == scope and result.order_id == '123'
    assert result.client_order_ref == 'atiopt_fixture' and result.session_key == 'IBKR_PERM_ID'
    assert result.status is None and result.effective_at == at
    assert not result.legs and not result.executions and not result.fees
    assert json.loads(original.raw_payload) == raw
    # Missing parent evidence never licenses BAG commissions, unknown account
    # allocation, pending price revisions or malformed aggregate quantities.
    for field, value in [('acctNumber', 'OTHER'), ('modelCode', 'allocated'),
                         ('pendingPriceRevision', True), ('permId', 0), ('cumQty', 0)]:
        invalid = deepcopy(raw)
        invalid['execution'][field] = value
        with pytest.raises(BrokerContractError):
            await decode(event(invalid), None)
    with pytest.raises(BrokerContractError):
        await decode(event({**raw, 'kind': 'commissionReport'}), None)
