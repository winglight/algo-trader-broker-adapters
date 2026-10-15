"""Native cross-source execution identity; never truncate IDs in business code."""
from algo_trader_broker_sdk.options import check
from algo_trader_broker_sdk.options_events import OptionExecutionOrigin
from .options_codec import _uuid


def match_candidate(source, native_id):
    if source == "ALPACA_TRADE_UPDATES":
        candidate = native_id
    else:
        check(source == "ALPACA_ACTIVITY_FILL" and type(native_id) is str and native_id.count("::") == 1,
              "Only an original FILL activity has an execution UUID candidate")
        prefix, candidate = native_id.split("::")
        check(bool(prefix), "Activity prefix must remain present")
    return _uuid(candidate)


def execution_identity(origin, execution_id, order_namespace):
    check(order_namespace == "ALPACA_ORDER_UUID", "Native order namespace differs")
    check(getattr(origin,"provider_id",None) in {None,"ALPACA"}, "Native source provider differs")
    _uuid(origin.order_id)
    _uuid(origin.native_contract_id)
    check(type(origin.local_symbol) is str and 0 < len(origin.local_symbol) <= 128 and origin.local_symbol.strip() == origin.local_symbol, "Invalid native execution symbol")
    candidate = match_candidate(origin.source, execution_id)
    check(getattr(origin,"match_candidate",None) in {None,candidate}, "Native execution candidate changed")
    check(getattr(origin,"order_namespace",None) in {None,order_namespace}, "Native order namespace changed")
    return OptionExecutionOrigin(origin.source, origin.order_id, origin.native_contract_id, origin.local_symbol,
        provider_id="ALPACA", order_namespace=order_namespace, match_candidate=candidate)
