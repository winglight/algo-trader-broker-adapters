"""Native account identity validation, before normalized DTO publication."""
import re
from algo_trader_broker_sdk.options import check
from algo_trader_broker_sdk.options_account import NativeCashExecution, NativeStockPosition, NativeStockInventory


def execution_identity(execution_id):
    check(type(execution_id) is str and len(execution_id) <= 128 and
          re.fullmatch(r"[^\s.]+(?:\.[^\s.]+)+\.[0-9]+", execution_id) is not None,
          "IB execution requires its complete native revision ID")
    family, _, suffix = execution_id.rpartition(".")
    revision = int(suffix)
    check(0 < revision <= 2**53-1 and suffix == f"{revision:02d}", "IB execution revision sequence is not canonical")
    return family + ".01", revision


def stock_identity(instrument_id):
    check(type(instrument_id) is str and re.fullmatch(r"IBKR:[1-9][0-9]*", instrument_id) is not None,
          "IB stock identity requires the exact native conId")


def cash_execution(source, activity_id, execution_id, broker_order_id, instrument_id, *args, **kwargs):
    check(source == "IB_ACCOUNT_EXECUTION" and activity_id == execution_id, "Cash execution must retain its complete native execId")
    execution_identity(execution_id)
    check(type(broker_order_id) is str and re.fullmatch(r"IBKR_PERM_ID:[1-9][0-9]*", broker_order_id) is not None,
          "IB cash execution requires its permanent native order identity")
    stock_identity(instrument_id)
    check(kwargs.pop("provider_id", "IBKR") == "IBKR", "Native source provider cannot be overridden")
    return NativeCashExecution(source, activity_id, execution_id, broker_order_id, instrument_id, *args, provider_id="IBKR", **kwargs)


def stock_position(instrument_id, *args, **kwargs):
    stock_identity(instrument_id)
    return NativeStockPosition(instrument_id, *args, **kwargs)


def stock_inventory(source, positions, *args, **kwargs):
    check(source == "IB_POSITION_MULTI", "Stock inventory belongs to another native source")
    for item in positions:
        stock_identity(item.instrument_id)
    check(kwargs.pop("provider_id", "IBKR") == "IBKR", "Native source provider cannot be overridden")
    return NativeStockInventory(source, positions, *args, provider_id="IBKR", **kwargs)
