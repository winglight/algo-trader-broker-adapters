"""Synthetic source failures must not retire another healthy IB session."""
import asyncio
from types import SimpleNamespace as N
from unittest.mock import AsyncMock

import pytest
from algo_trader_broker_adapter_ibkr_paper.adapter import IBKRPaperAdapter
from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient
from algo_trader_broker_adapter_ibkr_paper.settings import IBGatewaySettings
from algo_trader_broker_sdk import BrokerConnectionError


def client_for(ib):
    client=IBAsyncClient(IBGatewaySettings(client_id=20))
    client._ib=ib;client._connected.set()
    client.ensure_connected=AsyncMock(side_effect=lambda:client._ib)
    client._schedule_reconnect=AsyncMock()
    return client


@pytest.mark.asyncio
@pytest.mark.parametrize('operation',['qualification','snapshot','history','stream'])
async def test_healthy_request_timeout_preserves_failure_without_reconnecting(operation):
    from tests.test_adapter import FakeIBClient, FakeSupervisor
    native=FakeIBClient()
    adapter=IBKRPaperAdapter({},client=native,supervisor=FakeSupervisor())
    failure=TimeoutError('synthetic single-source timeout')
    if operation=='qualification':
        with pytest.raises(BrokerConnectionError,match='qualification timed out'):
            await adapter._raise_qualification_timeout(reason='qualification-timeout',operation='qualify_contract')
    else:
        call=AsyncMock(side_effect=failure)
        with pytest.raises(TimeoutError) as caught:
            await adapter._call_with_transient_reconnect(operation,call)
        assert caught.value is failure and call.await_count==1
    assert not any(call[0]=='reconnect' for call in native.calls)


@pytest.mark.asyncio
async def test_adapter_actual_disconnected_source_keeps_one_original_recovery():
    from tests.test_adapter import FakeIBClient, FakeSupervisor
    native=FakeIBClient();native.connection_state_snapshot=lambda:{'connected':False}
    adapter=IBKRPaperAdapter({},client=native,supervisor=FakeSupervisor())
    source=AsyncMock(side_effect=[ConnectionError('physical session lost'),'recovered-source'])
    assert await adapter._call_with_transient_reconnect('history',source)=='recovered-source'
    assert [c for c in native.calls if c[0]=='reconnect']==[('reconnect','history_ib_disconnect')]
    assert source.await_count==2


@pytest.mark.asyncio
@pytest.mark.parametrize('error',[ConnectionError,TimeoutError])
async def test_late_old_async_read_failure_never_retires_new_healthy_session(error):
    old=N(isConnected=lambda:False);new=N(isConnected=lambda:True)
    client=client_for(old);failure=error('late old request')
    started,release=asyncio.Event(),asyncio.Event()
    async def source(ib):
        assert ib is old
        started.set();await release.wait();raise failure
    task=asyncio.create_task(client._execute_with_reconnect_async(source))
    await started.wait();client._ib=new;release.set()
    with pytest.raises(error) as caught:await task
    assert caught.value is failure and client._ib is new and client.connection_state_snapshot()['connected']
    client._schedule_reconnect.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('error',[ConnectionError,TimeoutError])
async def test_current_healthy_async_source_failure_is_preserved_without_global_reset(error):
    ib=N(isConnected=lambda:True);client=client_for(ib);failure=error('request-specific failure')
    source=AsyncMock(side_effect=failure)
    with pytest.raises(error) as caught:await client._execute_with_reconnect_async(source)
    assert caught.value is failure and source.await_count==1
    client._schedule_reconnect.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('error',[ConnectionError,TimeoutError])
async def test_current_disconnected_async_session_still_recovers(error):
    old=N(isConnected=lambda:False);new=N(isConnected=lambda:True);client=client_for(old)
    async def recover(**kwargs):
        assert kwargs['expected_ib'] is old
        client._ib=new
    client._schedule_reconnect.side_effect=recover
    async def source(ib):
        if ib is old:raise error('current physical session lost')
        return 'fresh-source'
    assert await client._execute_with_reconnect_async(source)=='fresh-source'
    client._schedule_reconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_late_old_sync_read_error_preserves_replacement_session():
    old=N(isConnected=lambda:False);new=N(isConnected=lambda:True);client=client_for(old)
    failure=ConnectionError('old sync request')
    async def run(func,ib):client._ib=new;raise failure
    client._call_runner=run
    with pytest.raises(ConnectionError) as caught:await client._execute_with_reconnect_sync(lambda ib:None)
    assert caught.value is failure and client._ib is new
    client._schedule_reconnect.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconnect_cooldown_cannot_disconnect_a_replacement_session(monkeypatch):
    import algo_trader_broker_adapter_ibkr_paper.client as module
    old=N(isConnected=lambda:False);new=N(isConnected=lambda:True)
    client=IBAsyncClient(IBGatewaySettings(client_id=20),reconnect_cooldown=1)
    client._ib=old;client._connected.set();client.disconnect=AsyncMock()
    reached,release=asyncio.Event(),asyncio.Event()
    async def delay(seconds):reached.set();await release.wait()
    monkeypatch.setattr(module.asyncio,'sleep',delay)
    task=asyncio.create_task(client._schedule_reconnect(reason='old-lost',expected_ib=old))
    await reached.wait();client._ib=new;release.set();await task
    client.disconnect.assert_not_awaited()
    assert client._ib is new and client._reconnect_task is None


def test_connection_snapshot_does_not_call_dead_native_socket_connected():
    client=client_for(N(isConnected=lambda:False))
    assert client._connected.is_set() and not client.connection_state_snapshot()['connected']
