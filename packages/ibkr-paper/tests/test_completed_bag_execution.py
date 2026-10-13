import json

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
