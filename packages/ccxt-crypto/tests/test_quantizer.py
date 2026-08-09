from __future__ import annotations

from decimal import Decimal

import pytest
from algo_trader_broker_adapter_ccxt_crypto.quantizer import MarketRules, native_client_order_id
from algo_trader_broker_sdk import BrokerOrderError

from .fakes import MARKETS


def rules() -> MarketRules:
    return MarketRules.from_ccxt("BTC/USDT", MARKETS["BTC/USDT"], minimum_notional=Decimal(5))


def test_decimal_quantization_uses_okx_lot_and_side_aware_tick() -> None:
    rule = rules()
    assert str(rule.quantize_quantity("0.001000009", market=False)) == "0.00100000"
    assert str(rule.quantize_price("10000.09", side="BUY")) == "10000.0"
    assert str(rule.quantize_price("10000.01", side="SELL")) == "10000.1"


def test_notional_and_min_size_fail_closed() -> None:
    rule = rules()
    with pytest.raises(BrokerOrderError, match="minSz"):
        rule.quantize_quantity("0.000001", market=False)
    with pytest.raises(BrokerOrderError, match="notional"):
        rule.validate_notional(Decimal("0.0001"), Decimal(100))


def test_native_client_id_is_stable_and_okx_bounded() -> None:
    value = native_client_order_id("client/order:with:unsupported:characters:and:long")
    assert value == native_client_order_id("client/order:with:unsupported:characters:and:long")
    assert value.startswith("ati")
    assert len(value) == 32
