import json
from hashlib import sha256
from types import SimpleNamespace as N
from unittest.mock import AsyncMock, Mock

import pytest

from algo_trader_broker_adapter_alpaca_paper.options_cash_activities import read
from .test_option_raw_events import VERIFIED


@pytest.mark.asyncio
@pytest.mark.parametrize('asset_class,expected_order_reads', [('us_option',1),('unknown',2)])
async def test_only_exact_native_option_order_classification_reuses_its_retained_identity(asset_class,expected_order_reads):
    order_id='00000000-0000-4000-8000-000000000001'
    page=json.dumps([dict(id='fixture::00000000-0000-4000-8000-000000000002',activity_type='FILL',
        order_id=order_id,symbol='SPY261027C00774000')]).encode()
    order=json.dumps(dict(id=order_id,account_id=VERIFIED.native_account_ref,asset_class=asset_class)).encode()
    calls=[];retained=[]
    async def native(path,**kwargs):
        calls.append(path)
        return page if path=='/v2/account/activities' else order
    async def retain(raw):
        retained.append(raw)
        return sha256(raw).hexdigest()
    adapter=N(_option_bound=AsyncMock(return_value=(VERIFIED,None,None)),_option_still_bound=Mock(),
        _backend=N(get_option_resource=AsyncMock(side_effect=native)))
    assert await read(adapter,VERIFIED.scope,retain_evidence=retain)==()
    assert await read(adapter,VERIFIED.scope,retain_evidence=retain)==()
    assert calls.count('/v2/account/activities')==2
    assert calls.count('/v2/orders/'+order_id)==expected_order_reads
    assert retained.count(page)==2 and order in retained


@pytest.mark.asyncio
async def test_native_account_mismatch_cannot_populate_the_non_cash_identity_cache():
    order_id='00000000-0000-4000-8000-000000000001'
    page=json.dumps([dict(id='fixture::00000000-0000-4000-8000-000000000002',activity_type='FILL',order_id=order_id)]).encode()
    order=json.dumps(dict(id=order_id,account_id='different-native-account',asset_class='us_option')).encode()
    async def native(path,**kwargs):
        return page if path=='/v2/account/activities' else order
    async def retain(raw):return sha256(raw).hexdigest()
    adapter=N(_option_bound=AsyncMock(return_value=(VERIFIED,None,None)),_option_still_bound=Mock(),
        _backend=N(get_option_resource=AsyncMock(side_effect=native)))
    assert await read(adapter,VERIFIED.scope,retain_evidence=retain)==()
    assert not adapter._option_non_cash_orders
