import json
from datetime import datetime, timezone
from types import SimpleNamespace as N
from unittest.mock import AsyncMock
import pytest
from algo_trader_broker_adapter_alpaca_paper import AlpacaPaperAdapter
from algo_trader_broker_adapter_alpaca_paper.options_reads import contract_from_native
from .test_option_raw_events import VERIFIED


@pytest.mark.asyncio
async def test_fresh_static_contract_metadata_does_not_delay_or_relabel_financial_capture():
    native='00000000-0000-4000-8000-000000000001'
    stock='00000000-0000-4000-8000-000000000002'
    symbol='SPY261027C00774000'
    item=dict(id=native,underlying_symbol='SPY',root_symbol='SPY',style='american',multiplier='100',size='100',
        underlying_asset_id=stock,deliverables=[dict(type='equity',symbol='SPY',asset_id=stock,amount='100',
        allocation_percentage='100',delayed_settlement=False)],type='call',symbol=symbol,
        expiration_date='2026-10-27',strike_price='774',status='active',tradable=True)
    values={'/v2/account':dict(id=VERIFIED.native_account_ref,currency='USD',equity='1000',cash='900',options_buying_power='900'),
        '/v2/positions':[dict(asset_class='us_option',asset_id=native,symbol=symbol,qty='1',side='long',cost_basis='149')],
        '/v2/orders':[]}
    async def read(path,**kwargs):
        assert path in values, 'Static metadata must use its existing qualified clock'
        return json.dumps(values[path]).encode()
    backend=N(get_option_resource=AsyncMock(side_effect=read))
    adapter=AlpacaPaperAdapter(dict(alpaca_api_key_id='fixture',alpaca_secret_key='fixture'),backend=backend)
    adapter._connected,adapter._account_id=True,VERIFIED.native_account_ref
    adapter.bind_option_account(VERIFIED)
    qualified_at=datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
    binding=adapter._remember_contract(VERIFIED.scope,item,contract_from_native(item),json.dumps(item).encode(),qualified_at)
    state,_,_=await adapter._option_account_snapshot(VERIFIED.scope)
    assert len(state.positions)==1 and not state.unresolved_positions
    assert state.positions[0].binding==binding and binding.qualified_at==qualified_at
    assert state.positions[0].raw_cost_value=='149'
    assert backend.get_option_resource.await_count==3
