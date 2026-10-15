from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from algo_trader_broker_adapter_ibkr_paper.session_calendar import (
    BrokerSessionCalendarError,parse_broker_session_windows,request_calendar,
)


def detail(**overrides):
    return {"contract": {"conId": 123, "symbol": "MNQ"}, "timeZoneId": "America/New_York",
            "liquidHours": "20261126:CLOSED;20261127:0930-20261127:1300;20261130:0930-20261130:1600",
            "tradingHours": "20261126:1800-20261127:1300;20261129:1800-20261130:1700", **overrides}


def test_actual_holiday_early_close_and_selected_eth_are_distinct():
    rth = parse_broker_session_windows(detail(), session="RTH", symbol="MNQ")
    eth = parse_broker_session_windows(detail(), session="ETH", symbol="MNQ")
    assert len(rth) == 2
    assert rth[0]["sessionClose"] == "2026-11-27T18:00:00+00:00"
    assert rth[0]["sessionOpen"] == "2026-11-27T14:30:00+00:00"
    assert eth[0]["sessionOpen"] == "2026-11-26T23:00:00+00:00"
    assert eth[-1]["sessionClose"] == "2026-11-30T22:00:00+00:00"


def test_dst_uses_exchange_zone_not_fixed_utc():
    data = detail(liquidHours="20260306:0930-20260306:1600;20260309:0930-20260309:1600")
    rows = parse_broker_session_windows(data, session="RTH", symbol="ES")
    assert rows[0]["sessionOpen"].endswith("14:30:00+00:00")
    assert rows[1]["sessionOpen"].endswith("13:30:00+00:00")


@pytest.mark.parametrize("overrides", [
    {"timeZoneId": "EST"}, {"timeZoneId": "Unknown/Zone"}, {"liquidHours": ""},
    {"liquidHours": "20260308:0230-20260308:1600"},
    {"liquidHours": "20261101:0130-20261101:1600"},
    {"liquidHours": "20260102:1600-0930"},
    {"liquidHours": "20260102:0930-20260102:1600;20260102:1500-20260102:1700"},
])
def test_ambiguous_or_missing_broker_evidence_is_rejected(overrides):
    with pytest.raises(BrokerSessionCalendarError):
        parse_broker_session_windows(detail(**overrides), session="RTH", symbol="MNQ")


def test_valid_dst_aware_iana_alias_is_supported():
    data = detail(timeZoneId="CET", liquidHours="20260327:0930-20260327:1600;20260330:0930-20260330:1600")
    rows = parse_broker_session_windows(data, session="RTH", symbol="TEST")
    assert rows[0]["sessionOpen"].endswith("08:30:00+00:00")
    assert rows[1]["sessionOpen"].endswith("07:30:00+00:00")

@pytest.mark.asyncio
async def test_native_calendar_qualifies_exact_identity_and_rejects_ambiguous_scope():
    request = AsyncMock(return_value=[detail()])
    adapter = SimpleNamespace(request_contract_details=request)
    kwargs = dict(symbol='MNQ',session='RTH',timestamp=datetime(2026,11,27,15,tzinfo=UTC))
    rows = await request_calendar(adapter,{'contractId':123},**kwargs)
    request.assert_awaited_once_with({'conId':123})
    assert rows[0]['sessionClose'] == '2026-11-27T18:00:00+00:00'
    request.return_value = [detail(contract={'conId':999,'symbol':'MNQ'})]
    with pytest.raises(BrokerSessionCalendarError,match='AMBIGUOUS'):
        await request_calendar(adapter,{'conId':123},**kwargs)
    with pytest.raises(BrokerSessionCalendarError,match='PROFILE_MISMATCH'):
        await request_calendar(adapter,{'conId':123},session_profile='CME_US_CASH_RTH',**kwargs)
