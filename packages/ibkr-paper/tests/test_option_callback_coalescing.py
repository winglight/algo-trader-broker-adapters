"""Native replay idempotency preserves revisions, parents and original clocks."""
import json
from types import SimpleNamespace as N

import pytest
from ib_async import IB
from algo_trader_broker_sdk.options import OptionScope
from algo_trader_broker_adapter_ibkr_paper.options_events import IBOptionEventStream


def stream():
    received=[]
    async def retain(event):received.append(event)
    bound=N(scope=OptionScope(protocol_version='options/1.0',execution_target='a'*64,profile_id='fixture',
        account='synthetic',environment='paper',expected_generation=1),native_account_ref='DU_SYNTHETIC')
    return IBOptionEventStream(IB(),bound,retain),received


@pytest.mark.asyncio
async def test_repeated_execution_download_is_not_a_new_financial_event_or_clock(monkeypatch):
    import algo_trader_broker_adapter_ibkr_paper.options_events as module
    clock=iter(['2026-11-17T15:00:00Z','2026-11-17T15:00:01Z'])
    monkeypatch.setattr(module,'now_wire',lambda:next(clock))
    source,received=stream()
    try:
        payload=dict(execution=dict(execId='synthetic.01',shares='1',price='2'),parent=None)
        source.enqueue('execDetails',request_id=10,**payload)
        source.enqueue('execDetails',request_id=11,**payload)
        await source.flush()
        assert len(received)==1
        raw=json.loads(received[0].raw_payload)
        assert raw['request_id']==10 and raw['observed_at']=='2026-11-17T15:00:00Z'
        payload['parent']=dict(order=dict(permId=123))
        source.enqueue('execDetails',request_id=12,**payload)
        await source.flush()
        assert len(received)==2 and json.loads(received[1].raw_payload)['parent']==payload['parent']
    finally:source.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('kind,field,identity',[
    ('execDetails','execution',dict(execId='synthetic.01')),
    ('commissionReport','report',dict(execId='synthetic.01')),
    ('completedOrder','order',dict(permId=123)),
    ('openOrder','order',dict(clientId=40,orderId=123)),
    ('orderStatus','state',dict(permId=123)),
])
async def test_changes_and_return_to_prior_values_are_all_preserved(kind,field,identity):
    source,received=stream()
    try:
        for value in ('A','A','B','B','A'):
            source.enqueue(kind,**{field:dict(identity,value=value)})
        await source.flush()
        assert [json.loads(event.raw_payload)[field]['value'] for event in received]==['A','B','A']
    finally:source.close()


@pytest.mark.asyncio
async def test_new_connection_and_evicted_identity_collect_again_without_cross_scope_reuse():
    source,received=stream()
    try:
        for index in range(2049):source.enqueue('commissionReport',report=dict(execId=str(index),commission='1'))
        assert len(source._last_callbacks)==2048
        source.enqueue('commissionReport',report=dict(execId='0',commission='1'))
        await source.flush()
        assert len(received)==2050
    finally:source.close()
    restarted,again=stream()
    try:
        restarted.enqueue('commissionReport',report=dict(execId='0',commission='1'))
        await restarted.flush()
        assert len(again)==1
    finally:restarted.close()
