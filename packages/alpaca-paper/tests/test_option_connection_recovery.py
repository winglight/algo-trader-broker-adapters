import asyncio
from dataclasses import replace
from types import SimpleNamespace as N
from unittest.mock import AsyncMock
import pytest
from algo_trader_broker_adapter_alpaca_paper import AlpacaPaperAdapter
from algo_trader_broker_sdk import BrokerContractError
from .test_option_raw_events import VERIFIED


@pytest.mark.asyncio
async def test_recovered_read_survives_current_scope_read_and_preserves_catalog_until_real_connection_change():
    started, release = asyncio.Event(), asyncio.Event()
    async def capture(path):
        started.set()
        await release.wait()
        return ('{"id":"'+VERIFIED.native_account_ref+'"}').encode()
    adapter = AlpacaPaperAdapter(dict(alpaca_api_key_id='fixture', alpaca_secret_key='fixture'),
        backend=N(get_option_resource=AsyncMock(side_effect=capture)))
    adapter._connected, adapter._account_id = True, VERIFIED.native_account_ref
    current = replace(VERIFIED, scope=replace(VERIFIED.scope, expected_generation=VERIFIED.scope.expected_generation+1))
    adapter.bind_option_account(current)
    adapter._option_catalog = {'native': 'retained qualification'}
    with pytest.raises(BrokerContractError):
        await adapter._option_bound(VERIFIED.scope)
    adapter.register_option_recovery(VERIFIED, current)
    task = asyncio.create_task(adapter._option_bound(VERIFIED.scope, read_account=True))
    await started.wait()
    adapter.bind_option_account(current)
    assert (await adapter._option_bound(current.scope))[0] == current
    adapter.bind_option_account(VERIFIED)
    assert adapter._option_account_binding == current
    assert adapter._option_catalog == {'native':'retained qualification'}
    release.set()
    assert (await task)[0] == VERIFIED
    newer = replace(current, scope=replace(current.scope, expected_generation=current.scope.expected_generation+1))
    adapter.bind_option_account(newer)
    with pytest.raises(BrokerContractError):
        adapter._option_still_bound(VERIFIED)
    assert not adapter._option_recovery_bindings and not adapter._option_catalog


@pytest.mark.asyncio
async def test_recovered_native_close_uses_original_command_and_actual_event_connection(monkeypatch):
    from dataclasses import asdict
    from datetime import datetime, timedelta, timezone
    from algo_trader_broker_sdk.options import BrokerOptionBinding, OptionExecutionRequest, OptionLegIntent
    now=datetime.now(timezone.utc)
    wire=lambda value:value.isoformat().replace('+00:00','Z')
    backend=N(submit_option_order_raw=AsyncMock(return_value=b'{"native":"original bytes"}'))
    adapter=AlpacaPaperAdapter(dict(alpaca_api_key_id='fixture',alpaca_secret_key='fixture'),backend=backend)
    adapter._connected,adapter._account_id=True,VERIFIED.native_account_ref
    current=replace(VERIFIED,scope=replace(VERIFIED.scope,expected_generation=VERIFIED.scope.expected_generation+1))
    adapter.register_option_recovery(VERIFIED,current)
    adapter._option_event_binding=current
    adapter._option_evidence_handler=AsyncMock()
    adapter._option_identity_observation=(VERIFIED,backend,now)
    binding=BrokerOptionBinding('alpaca_paper','fixture','paper',VERIFIED.scope.account,'opt_'+'a'*64,
        '00000000-0000-4000-8000-000000000001','SPY261027C00774000',None,wire(now),'metadata')
    request=OptionExecutionRequest(**asdict(VERIFIED.scope),command_id='close',client_order_id='close',plan_hash='a'*64,
        authorization_ref='permit',quote_snapshot_ref='quote',valid_until=wire(now+timedelta(seconds=5)),capability_revision='native',
        groups=1,order_type='LMT',signed_limit='-1.2',tif='DAY',max_slippage='0',
        legs=(OptionLegIntent('L1',binding,'SELL','CLOSE',1),))
    monkeypatch.setattr('algo_trader_broker_adapter_alpaca_paper.options_orders.order_state',lambda request,raw:'ack')
    gate=object()
    assert await adapter.submit_option_order_guarded(request,gate)=='ack'
    args=backend.submit_option_order_raw.await_args
    assert args.args[0]['position_intent']=='sell_to_close' and args.args[0]['limit_price']=='1.2'
    assert args.kwargs['submission_gate'] is gate
    event=adapter._option_evidence_handler.await_args.args[0]
    assert event.scope==current.scope and event.raw_payload==b'{"native":"original bytes"}'
    opening=replace(request,signed_limit='1.2',legs=(replace(request.legs[0],side='BUY',position_effect='OPEN'),))
    with pytest.raises(BrokerContractError,match='only reduce'):
        await adapter.submit_option_order_guarded(opening,gate)
    assert backend.submit_option_order_raw.await_count==1
