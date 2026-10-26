import json
import asyncio
from hashlib import sha256
from types import SimpleNamespace as N
from unittest.mock import AsyncMock, Mock

import pytest

from algo_trader_broker_sdk import BrokerCapabilityError, BrokerConnectionError
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


@pytest.mark.asyncio
async def test_missing_historical_order_stays_unknown_without_blocking_other_cash_proofs_or_negative_cache():
    missing = '00000000-0000-4000-8000-000000000001'
    known = '00000000-0000-4000-8000-000000000002'
    asset = '00000000-0000-4000-8000-000000000003'
    def activity(order_id, execution):
        return dict(id='fixture::'+execution, activity_type='FILL', order_id=order_id,
            type='fill', symbol='SPY', side='buy', qty='1', price='500',
            transaction_time='2026-10-22T14:00:00Z')
    rows = [activity(missing, '00000000-0000-4000-8000-000000000004'),
        activity(known, '00000000-0000-4000-8000-000000000005')]
    page = json.dumps(rows).encode()
    calls, retained = [], []
    async def native(path, **kwargs):
        calls.append(path)
        if path == '/v2/account/activities':
            return page
        if path == '/v2/orders/'+missing:
            raise BrokerConnectionError('Native order missing', details={'operation': 'option_trading_read',
                'resource_kind': 'ORDER_DETAIL', 'error_type': 'HTTPStatusError', 'status_code': 404})
        return json.dumps(dict(id=known, account_id=VERIFIED.native_account_ref,
            asset_class='us_equity', asset_id=asset, symbol='SPY', side='buy', order_class='simple')).encode()
    async def retain(raw):
        retained.append(raw)
        return sha256(raw).hexdigest()
    adapter = N(_option_bound=AsyncMock(return_value=(VERIFIED, None, None)), _option_still_bound=Mock(),
        _backend=N(get_option_resource=AsyncMock(side_effect=native)))
    for _ in range(2):
        result = await read(adapter, VERIFIED.scope, retain_evidence=retain)
        assert len(result) == 1 and result[0].broker_order_id == known
    assert calls.count('/v2/orders/'+missing) == 2
    assert not adapter._option_non_cash_orders
    assert retained.count(page) == 2
    assert any(json.loads(raw).get('order_id') == missing for raw in retained if raw.startswith(b'{'))


@pytest.mark.asyncio
@pytest.mark.parametrize('kind,status', [('ORDER_DETAIL', 429), ('ORDER_DETAIL', 500),
    ('ACCOUNT_ACTIVITIES', 404), ('UNKNOWN', 404)])
async def test_unrelated_native_failures_still_abort_cash_activity_observation(kind, status):
    order_id = '00000000-0000-4000-8000-000000000001'
    page = json.dumps([dict(id='fixture::00000000-0000-4000-8000-000000000002',
        activity_type='FILL', order_id=order_id)]).encode()
    failure = BrokerConnectionError('Native read failure', details={'operation': 'option_trading_read',
        'resource_kind': kind, 'error_type': 'HTTPStatusError', 'status_code': status})
    async def native(path, **kwargs):
        if path == '/v2/account/activities': return page
        raise failure
    async def retain(raw): return sha256(raw).hexdigest()
    adapter = N(_option_bound=AsyncMock(return_value=(VERIFIED, None, None)), _option_still_bound=Mock(),
        _backend=N(get_option_resource=AsyncMock(side_effect=native)))
    with pytest.raises(BrokerConnectionError) as caught:
        await read(adapter, VERIFIED.scope, retain_evidence=retain)
    assert caught.value is failure
    assert not adapter._option_non_cash_orders


@pytest.mark.asyncio
async def test_own_history_budget_keeps_completed_cash_proof_and_archived_unknown_activity(monkeypatch):
    monkeypatch.setattr('algo_trader_broker_adapter_alpaca_paper.options_cash_activities.CASH_ACTIVITY_WORK_BUDGET_SECONDS', .01)
    known = '00000000-0000-4000-8000-000000000001'
    pending = '00000000-0000-4000-8000-000000000002'
    def activity(order_id, execution):
        return dict(id='fixture::'+execution, activity_type='FILL', order_id=order_id,
            type='fill', symbol='SPY', side='buy', qty='1', price='500', transaction_time='2026-10-22T14:00:00Z')
    page = json.dumps([activity(known, '00000000-0000-4000-8000-000000000003'),
        activity(pending, '00000000-0000-4000-8000-000000000004')]).encode()
    retained, canceled = [], []
    async def native(path, **kwargs):
        if path == '/v2/account/activities': return page
        if path == '/v2/orders/'+pending:
            try: await asyncio.sleep(60)
            finally: canceled.append(path)
        return json.dumps(dict(id=known, account_id=VERIFIED.native_account_ref,
            asset_class='us_equity', asset_id='00000000-0000-4000-8000-000000000005',
            symbol='SPY', side='buy', order_class='simple')).encode()
    async def retain(raw):
        retained.append(raw)
        return sha256(raw).hexdigest()
    adapter = N(_option_bound=AsyncMock(return_value=(VERIFIED, None, None)), _option_still_bound=Mock(),
        _backend=N(get_option_resource=AsyncMock(side_effect=native)))
    reasons = set()
    result = await read(adapter, VERIFIED.scope, retain_evidence=retain, advisory_reasons=reasons)
    assert len(result) == 1 and result[0].broker_order_id == known
    assert reasons == {'CASH_EXECUTION_HISTORY_INCOMPLETE'} and canceled
    assert page in retained
    assert any(json.loads(raw).get('order_id') == pending for raw in retained if raw.startswith(b'{'))
    assert not adapter._option_non_cash_orders


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', [TimeoutError('upstream timeout'),
    BrokerCapabilityError('denied', code='OPTION_RESOURCE_DENIED'),
    BrokerConnectionError('native network timeout', details={'error_type':'ReadTimeout'})])
async def test_budget_does_not_hide_an_upstream_failure(failure):
    async def retain(raw): return sha256(raw).hexdigest()
    adapter = N(_option_bound=AsyncMock(return_value=(VERIFIED, None, None)), _option_still_bound=Mock(),
        _backend=N(get_option_resource=AsyncMock(side_effect=failure)))
    reasons = set()
    with pytest.raises(type(failure)) as caught:
        await read(adapter, VERIFIED.scope, retain_evidence=retain, advisory_reasons=reasons)
    assert caught.value is failure and not reasons
