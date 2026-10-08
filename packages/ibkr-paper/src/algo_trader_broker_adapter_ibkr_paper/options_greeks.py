"""Retain native model callbacks without inventing their input time or units."""

from hashlib import sha256

from algo_trader_broker_sdk.options import NativeGreekObservation

from .options_account_native import raw_bytes
from .options_reads import now_wire


class GreekCapture:
    """One callback tee per connection, routed to request-owned observers.

    Multiple quote owners may finish in any order. Restoring stacked wrapper
    overrides per subscription would otherwise detach a still-active owner.
    """
    def __init__(self, ib):
        self.ib, self.rows, self.ids = ib, {}, set()
        wrapper = ib.wrapper
        router = getattr(wrapper, "_ati_native_greeks", None)
        if router is None:
            original = wrapper.tickOptionComputation
            router = dict(original=original, overridden="tickOptionComputation" in wrapper.__dict__, owners={})
            def capture(req_id, tick_type, tick_attrib, implied_vol, delta, option_price, pv_dividend,
                        gamma, vega, theta, underlying_price):
                owner = router["owners"].get(req_id)
                if owner is not None and tick_type == 13:
                    # Capture before ib_async converts sentinels into None.
                    owner.rows[req_id] = (now_wire(), dict(req_id=req_id, tick_type=tick_type,
                        tick_attrib=tick_attrib, implied_vol=implied_vol, delta=delta, option_price=option_price,
                        pv_dividend=pv_dividend, gamma=gamma, vega=vega, theta=theta, underlying_price=underlying_price))
                return original(req_id, tick_type, tick_attrib, implied_vol, delta, option_price, pv_dividend,
                                gamma, vega, theta, underlying_price)
            wrapper.tickOptionComputation = capture
            wrapper._ati_native_greeks = router
        self.router = router

    def register(self, req_id):
        self.ids.add(req_id)
        self.router["owners"][req_id] = self

    def observation(self, req_id, binding, market_data_type):
        row = self.rows.get(req_id)
        if row is None:
            return None
        observed, values = row
        raw = raw_bytes(dict(source="IB_TICK_OPTION_COMPUTATION", broker_contract_id=binding.broker_contract_id,
            server_version=self.ib.client.serverVersion(), market_data_type=market_data_type, values=values))
        reasons = ["GREEKS_INPUT_TIME_UNAVAILABLE", "GREEKS_MODEL_VERSION_UNAVAILABLE", "GREEKS_UNITS_UNVERIFIED"]
        if market_data_type != 1:
            reasons.append("GREEKS_LIVE_DATA_UNVERIFIED")
        return NativeGreekObservation(binding.canonical_id, "IBKR", "PROVIDER_MODEL", None, None, "UNKNOWN",
            observed, raw.decode(), sha256(raw).hexdigest(), tuple(reasons))

    def close(self):
        for req_id in self.ids:
            self.router["owners"].pop(req_id, None)
        if not self.router["owners"]:
            wrapper = self.ib.wrapper
            if self.router["overridden"]:
                wrapper.tickOptionComputation = self.router["original"]
            else:
                del wrapper.tickOptionComputation
            del wrapper._ati_native_greeks
