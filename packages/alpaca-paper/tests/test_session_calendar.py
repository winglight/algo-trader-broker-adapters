import json
from datetime import datetime,timezone
from types import SimpleNamespace as N
from unittest.mock import AsyncMock

import pytest
from algo_trader_broker_sdk.errors import BrokerContractError
from algo_trader_broker_adapter_alpaca_paper.session_calendar import request_calendar


@pytest.mark.asyncio
async def test_native_calendar_preserves_early_close_and_does_not_guess_foreign_identity():
    native = json.dumps([dict(date='2026-11-27',open='09:30',close='13:00'),
        dict(date='2026-11-30',open='09:30',close='16:00')]).encode()
    adapter = N(qualify_contract=AsyncMock(return_value=dict(symbol='SPY',assetId='asset')),
        _backend=N(get_option_resource=AsyncMock(return_value=native)))
    kwargs = dict(symbol='SPY',session='RTH',timestamp=datetime(2026,11,27,15,tzinfo=timezone.utc))
    rows = await request_calendar(adapter,dict(symbol='SPY',assetId='asset'),**kwargs)
    assert rows[0]['sessionClose'] == '2026-11-27T18:00:00Z'
    adapter._backend.get_option_resource.assert_awaited_once_with('/v2/calendar',
        params=dict(start='2026-10-28',end='2026-12-11',date_type='TRADING'))
    with pytest.raises(BrokerContractError,match='CONTRACT_INVALID'):
        await request_calendar(adapter,dict(symbol='SPY',conId=123),**kwargs)
    with pytest.raises(BrokerContractError,match='AMBIGUOUS'):
        await request_calendar(adapter,dict(symbol='SPY',assetId='other'),**kwargs)
    adapter._backend.get_option_resource.assert_awaited_once()
