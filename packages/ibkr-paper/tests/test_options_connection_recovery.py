from dataclasses import asdict, replace
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace as N
import pytest
from algo_trader_broker_adapter_ibkr_paper import IBKRPaperAdapter
from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import (OptionScope,OptionVerifiedAccount,OptionExecutionRequest,
    OptionLegIntent,BrokerOptionBinding,OptionContractKey,option_contract_id)

@pytest.mark.asyncio
async def test_recovery_preserves_logical_reads_physical_connection_and_reduce_only_sends():
    stamp=datetime.now(timezone.utc)
    ib=N(isConnected=lambda:True,managedAccounts=lambda:['DU12345'])
    async def read(operation):return await operation(ib)
    client=N(connection_state_snapshot=lambda:dict(connected=True,connected_since=stamp),read_options=read)
    adapter=IBKRPaperAdapter({},client=client)
    old=OptionScope('options/1.0','a'*64,'profile',1,'account','paper')
    current=replace(old,expected_generation=2)
    before=OptionVerifiedAccount(old,'IBKR','DU12345');now=OptionVerifiedAccount(current,'IBKR','DU12345')
    adapter.register_option_recovery(before,now)
    adapter.bind_option_account(before)
    assert adapter._option_account_binding==now
    async def scope(ib,bound):return bound.scope
    assert await adapter._option_read(old,scope)==old
    assert await adapter._option_read(old,scope,physical=True)==current
    key=OptionContractKey('SPY','2026-12-18','C','600','USD',100,'AMERICAN','PHYSICAL','STANDARD_100_SHARES')
    canonical=option_contract_id(key)
    binding=BrokerOptionBinding('ibkr_paper','fixture','paper','account',canonical,'123','SPY CALL','SMART',stamp.isoformat(),'fixture')
    command=OptionExecutionRequest(**asdict(old),command_id='synthetic',client_order_id='synthetic',plan_hash='b'*64,
        authorization_ref='fixture',quote_snapshot_ref='c'*64,valid_until=(stamp+timedelta(seconds=30)).isoformat(),
        capability_revision='fixture',groups=1,order_type='LMT',signed_limit='1',tif='DAY',max_slippage='0',
        legs=(OptionLegIntent('L1',binding,'BUY','OPEN',1),))
    with pytest.raises(BrokerContractError,match='reduce-only'):
        await adapter._option_read(command,scope,physical=True)
    close=replace(command,signed_limit='-1',legs=(replace(command.legs[0],side='SELL',position_effect='CLOSE'),))
    assert await adapter._option_read(close,scope,physical=True)==current
    for invalid in [replace(now,native_account_ref='DU54321'),replace(now,scope=replace(current,account='other')),
                    replace(now,scope=replace(current,expected_generation=1)),replace(now,scope=replace(current,environment='live'))]:
        with pytest.raises(BrokerContractError):adapter.register_option_recovery(before,invalid)
    stamp+=timedelta(seconds=1)
    with pytest.raises(BrokerContractError,match='connection changed'):
        await adapter._option_read(old,scope)
