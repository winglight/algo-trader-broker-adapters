"""Cold static qualification can trigger one new native financial capture."""
from datetime import datetime,timedelta,timezone
from types import SimpleNamespace as N
from unittest.mock import AsyncMock

import pytest
from algo_trader_broker_adapter_ibkr_paper.options_account import fresh_account_capture


@pytest.mark.asyncio
@pytest.mark.parametrize('old_seconds,second_seconds,expected_calls',[(0,0,1),(2,0,2),(10,9,2),(-1,0,1)])
async def test_native_account_capture_is_bounded_and_never_renews_an_old_clock(monkeypatch,old_seconds,second_seconds,expected_calls):
    import algo_trader_broker_adapter_ibkr_paper.options_account as module
    now=datetime.now(timezone.utc)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return now
    monkeypatch.setattr(module,'datetime',Clock)
    first=N(observed_at=(now-timedelta(seconds=old_seconds)).isoformat().replace('+00:00','Z'),archive='original-native-archive')
    second=N(observed_at=(now-timedelta(seconds=second_seconds)).isoformat().replace('+00:00','Z'),archive='second-native-archive')
    original=first.observed_at
    capture=AsyncMock(side_effect=[first,second])
    ib,bound=object(),object()
    result=await fresh_account_capture(capture,ib,bound)
    assert capture.await_count==expected_calls
    assert all(call.args==(ib,bound) for call in capture.await_args_list)
    assert result is (second if expected_calls==2 else first)
    assert first.observed_at==original
    if second_seconds==9:assert result.observed_at==second.observed_at
