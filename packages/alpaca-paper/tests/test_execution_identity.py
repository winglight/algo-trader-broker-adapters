"""Native alias extraction is checked before neutral transport construction."""
from types import SimpleNamespace
import pytest
from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_adapter_alpaca_paper import options_identity as native

ID="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def origin(**changes):
    return SimpleNamespace(**{**dict(source="ALPACA_ACTIVITY_FILL",order_id=ID,native_contract_id=ID,local_symbol="QQQ261016C00746000"),**changes})


def test_candidate_preserves_original_source_and_ids():
    full="20261015140000::"+ID
    value=native.execution_identity(origin(),full,"ALPACA_ORDER_UUID")
    assert full.startswith("20261015140000::") and value.match_candidate==ID
    assert value.provider_id=="ALPACA" and value.order_namespace=="ALPACA_ORDER_UUID"
    assert value.order_id==value.native_contract_id==ID


@pytest.mark.parametrize("source,execution",[("OPASN","prefix::"+ID),("ALPACA_ACTIVITY_FILL",ID),
    ("ALPACA_ACTIVITY_FILL","::"+ID),("ALPACA_ACTIVITY_FILL","prefix::extra::"+ID),
    ("ALPACA_ACTIVITY_FILL","prefix::invalid"),("ALPACA_TRADE_UPDATES","prefix::"+ID)])
def test_invalid_candidates_fail_before_transport(monkeypatch,source,execution):
    def forbidden(*args,**kwargs):raise AssertionError("Invalid native alias reached transport")
    monkeypatch.setattr(native,"OptionExecutionOrigin",forbidden)
    with pytest.raises(BrokerContractError):native.execution_identity(origin(source=source),execution,"ALPACA_ORDER_UUID")


@pytest.mark.parametrize("change",[dict(order_id="bad"),dict(native_contract_id="bad"),dict(local_symbol=""),
    dict(provider_id="other"),dict(match_candidate="other"),dict(order_namespace="other")])
def test_invalid_origin_cannot_attest_a_candidate(monkeypatch,change):
    def forbidden(*args,**kwargs):raise AssertionError("Invalid origin reached transport")
    monkeypatch.setattr(native,"OptionExecutionOrigin",forbidden)
    with pytest.raises(BrokerContractError):native.execution_identity(origin(**change),"prefix::"+ID,"ALPACA_ORDER_UUID")
