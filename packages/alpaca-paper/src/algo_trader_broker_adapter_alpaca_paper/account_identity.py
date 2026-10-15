"""Native account identity validation, before normalized DTO publication."""
from algo_trader_broker_sdk.options import check
from algo_trader_broker_sdk.options_account import NativeCashExecution, NativeStockPosition, NativeStockInventory
from .options_codec import _uuid


def stock_identity(instrument_id):
    check(type(instrument_id) is str and instrument_id.startswith("ALPACA:"), "Alpaca stock identity requires its native namespace")
    _uuid(instrument_id.removeprefix("ALPACA:"))


def cash_execution(source, activity_id, execution_id, broker_order_id, instrument_id, *args, **kwargs):
    check(source == "ALPACA_ACCOUNT_FILL", "Cash execution has another native source")
    _uuid(execution_id)
    _uuid(broker_order_id)
    check(type(activity_id) is str and activity_id.endswith("::" + execution_id) and bool(activity_id[:-len(execution_id)-2]),
          "Retain the full native FILL activity identity")
    stock_identity(instrument_id)
    check(kwargs.pop("provider_id", "ALPACA") == "ALPACA", "Native source provider cannot be overridden")
    return NativeCashExecution(source, activity_id, execution_id, broker_order_id, instrument_id, *args, provider_id="ALPACA", **kwargs)


def stock_position(instrument_id, *args, **kwargs):
    stock_identity(instrument_id)
    return NativeStockPosition(instrument_id, *args, **kwargs)


def stock_inventory(source, positions, *args, **kwargs):
    check(source == "ALPACA_POSITIONS", "Stock inventory belongs to another native source")
    for item in positions:
        stock_identity(item.instrument_id)
    check(kwargs.pop("provider_id", "ALPACA") == "ALPACA", "Native source provider cannot be overridden")
    return NativeStockInventory(source, positions, *args, provider_id="ALPACA", **kwargs)
