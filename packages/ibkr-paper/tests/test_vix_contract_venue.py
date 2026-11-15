"""VIX discovery must not inherit the generic futures venue."""
import importlib.util
import sys
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "vix_contract_builder_real",
    Path(__file__).parents[1] / "src/algo_trader_broker_adapter_ibkr_paper/contract_builder.py",
)
builder = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = builder
spec.loader.exec_module(builder)


@pytest.mark.parametrize("symbol", ["VX", "VIX"])
@pytest.mark.parametrize("exchange", [None, "", "SMART", "GLOBEX", "CME", "CFE"])
def test_vix_discovery_uses_cfe(symbol, exchange):
    result = builder.IBContractFactory().build(builder.ContractParams(
        symbol=symbol, sec_type="FUT", exchange=exchange, currency="USD",
        last_trade_date_or_contract_month="20261118",
    ))
    assert result.contract.symbol == "VIX"
    assert result.contract.secType == "FUT"
    assert result.contract.tradingClass == "VX"
    assert result.contract.exchange == "CFE"
    assert result.contract.lastTradeDateOrContractMonth == "20261118"
    assert result.contract.primaryExchange == ("SMART" if exchange == "SMART" else "")


@pytest.mark.parametrize("exchange", ["CFE", "GLOBEX", "CME", "CBOE"])
def test_qualified_native_identity_is_preserved(exchange):
    result = builder.IBContractFactory().build(builder.ContractParams(
        symbol="VIX", sec_type="FUT", con_id=12345, exchange=exchange,
        currency="USD", local_symbol="VXX6", trading_class="VX",
        last_trade_date_or_contract_month="20261118",
    ))
    assert result.contract.conId == 12345
    assert result.contract.exchange == exchange
    assert result.contract.localSymbol == "VXX6"


@pytest.mark.parametrize("symbol", ["MNQ", "ES", "CL"])
def test_other_future_venues_unchanged(symbol):
    result = builder.IBContractFactory().build(builder.ContractParams(
        symbol=symbol, sec_type="FUT", exchange="GLOBEX", currency="USD",
        last_trade_date_or_contract_month="202612",
    ))
    assert result.contract.symbol == symbol
    assert result.contract.exchange != "CFE"


def test_unqualified_vix_explicit_cboe_venue_is_preserved():
    result = builder.IBContractFactory().build(builder.ContractParams(
        symbol="VIX", sec_type="FUT", exchange="CBOE", currency="USD",
        last_trade_date_or_contract_month="20261118",
    ))
    assert result.contract.exchange == "CBOE"


def test_vix_index_is_not_changed_into_a_future():
    result = builder.IBContractFactory().build(builder.ContractParams(
        symbol="VIX", sec_type="IND", exchange="CBOE", currency="USD",
    ))
    assert result.contract.symbol == "VIX"
    assert result.contract.secType == "IND"
    assert result.contract.exchange == "CBOE"
