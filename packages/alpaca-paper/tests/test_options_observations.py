"""Synthetic native observations only; no broker calls or credentials."""
import json
from datetime import datetime, timezone
from types import SimpleNamespace as N
from unittest.mock import AsyncMock
import pytest
from algo_trader_broker_sdk import BrokerConnectionError, BrokerContractError
from algo_trader_broker_adapter_alpaca_paper.options_reads import contract_from_native
from algo_trader_broker_adapter_alpaca_paper.options_observations import catalog_oi, last_trade, day_volumes, quote_volume


def test_arbitrary_standard_stock_retains_native_deliverables_and_rejects_adjustment():
    item=dict(id='00000000-0000-4000-8000-000000000001',underlying_symbol='AAPL',root_symbol='AAPL',
        underlying_asset_id='00000000-0000-4000-8000-000000000002',style='american',multiplier='100',size='100',
        symbol='AAPL261106C00250000',expiration_date='2026-11-06',type='call',strike_price='250',status='active',tradable=True,
        deliverables=[dict(type='equity',symbol='AAPL',asset_id='00000000-0000-4000-8000-000000000002',amount='100',allocation_percentage='100',delayed_settlement=False)])
    assert contract_from_native(item).key.underlying=='AAPL'
    item['deliverables'][0]['amount']='150'
    with pytest.raises(BrokerContractError):contract_from_native(item)


def test_zero_oi_is_an_observation_and_missing_clocks_do_not_fabricate_it():
    assert catalog_oi(dict(open_interest='0',open_interest_date='2026-11-03'))==(0,'2026-11-03')
    assert catalog_oi(dict(open_interest='20'))==(None,None)
    assert last_trade(dict(latestTrade=dict(p='0',t='2026-11-04T15:00:00Z')))==dict(last_trade_price='0',last_trade_at='2026-11-04T15:00:00Z')
    assert last_trade(dict(latestTrade=dict(p='2.1')))=={}


@pytest.mark.asyncio
async def test_daily_zero_volume_has_independent_clock_session_and_cache():
    raw=json.dumps(dict(bars={'synthetic_occ':[dict(t='2026-11-04T05:00:00Z',v=0)]},next_page_token=None)).encode()
    backend=N(get_option_resource=AsyncMock(return_value=raw));adapter=N(_backend=backend,_remember_option_read=lambda _:None)
    bindings=[N(local_symbol='synthetic_occ')]
    volumes=await day_volumes(adapter,bindings)
    quote='2026-11-04T15:00:00Z';observed=quote_volume(volumes,'synthetic_occ',quote)
    assert observed['volume']==0 and observed['volume_date']=='2026-11-04'
    assert observed['volume_as_of']!=quote
    assert quote_volume(volumes,'synthetic_occ','2026-11-03T15:00:00Z')=={}
    assert await day_volumes(adapter,bindings)==volumes
    backend.get_option_resource.assert_awaited_once()
    cutoff=datetime.fromisoformat(backend.get_option_resource.await_args.kwargs['params']['end'].replace('Z','+00:00'))
    assert (datetime.now(timezone.utc)-cutoff).total_seconds()>=900
    assert observed['volume_as_of']==backend.get_option_resource.await_args.kwargs['params']['end']


@pytest.mark.asyncio
@pytest.mark.parametrize('response',[BrokerConnectionError('synthetic permission failure'),json.dumps(dict(bars={'synthetic_occ':[dict(t='2026-11-04T05:00:00Z',v=10)]},next_page_token='synthetic_more')).encode()])
async def test_optional_volume_failure_or_partial_page_stays_unknown(response):
    read=AsyncMock(side_effect=response) if isinstance(response,Exception) else AsyncMock(return_value=response)
    adapter=N(_backend=N(get_option_resource=read),_remember_option_read=lambda _:None)
    assert await day_volumes(adapter,[N(local_symbol='synthetic_occ')])=={'synthetic_occ':{}}
