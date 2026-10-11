"""Native per-share model Greeks with explicit feed and receipt provenance."""

from decimal import Decimal, InvalidOperation, localcontext, ROUND_HALF_EVEN
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
    reasons = []
    if feed != "OPRA":
        reasons.append("GREEKS_LIVE_DATA_UNVERIFIED")
    return NativeGreekObservation(binding.canonical_id, "ALPACA", "PROVIDER_MODEL", None, None,
        "PER_SHARE_PER_VOL_POINT_PER_DAY",
        received, raw, sha256(raw.encode()).hexdigest(), tuple(reasons))


def normalized_greeks(item, observation):
    if observation is None:
        return (None,) * 7
    greeks = item.get("greeks")
    greeks = greeks if isinstance(greeks, dict) else {}
    normalized = []
    for name in ("delta", "gamma", "vega", "theta", "impliedVolatility"):
        try:
            value = Decimal(str(item.get(name) if name == "impliedVolatility" else greeks.get(name)))
            if (not value.is_finite() or abs(value) >= Decimal("1e53")
                    or (name in {"gamma", "vega", "impliedVolatility"} and value < 0)
                    or (name == "delta" and abs(value) > 1)):
                normalized.append(None)
                continue
            with localcontext() as context:
                context.prec = 80
                value = value.quantize(Decimal("0.000000000001"), rounding=ROUND_HALF_EVEN)
            wire = format(value, "f")
            normalized.append("0" if value == 0 else wire.rstrip("0").rstrip(".") if "." in wire else wire)
        except (InvalidOperation, TypeError, ValueError):
            normalized.append(None)
    return (*normalized, "PROVIDER_MODEL", observation.observed_at)
