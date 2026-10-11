"""Preserve snapshot model observations without guessing time, version or units."""

from decimal import Decimal
from hashlib import sha256
import json

from algo_trader_broker_sdk.options import NativeGreekObservation


def snapshot_greeks(item, binding, *, feed, received, response_hash):
    if item.get("greeks") is None and item.get("impliedVolatility") is None:
        return None

    def decimal_value(value):
        if isinstance(value, Decimal):
            return str(value)
        raise TypeError("Unsupported native Greek value")

    # This is a parsed observation; the original response remains retained by
    # the adapter read archive and is identified separately by its byte hash.
    raw = json.dumps(dict(source="ALPACA_OPTION_SNAPSHOT", local_symbol=binding.local_symbol,
        provider_contract_id=binding.broker_contract_id, feed=feed, response_hash=response_hash,
        greeks=item.get("greeks"), impliedVolatility=item.get("impliedVolatility")),
        sort_keys=True, separators=(",", ":"), default=decimal_value, allow_nan=False)
    reasons = ["GREEKS_INPUT_TIME_UNAVAILABLE", "GREEKS_MODEL_VERSION_UNAVAILABLE", "GREEKS_UNITS_UNVERIFIED"]
    if feed != "OPRA":
        reasons.append("GREEKS_LIVE_DATA_UNVERIFIED")
    return NativeGreekObservation(binding.canonical_id, "ALPACA", "PROVIDER_MODEL", None, None, "UNKNOWN",
        received, raw, sha256(raw.encode()).hexdigest(), tuple(reasons))
