from decimal import Decimal

from algo_trader_broker_adapter_ccxt_crypto.fee_policy import CryptoSpotFeePolicy


def test_reviewed_fee_policy_is_decimal_and_versioned() -> None:
    policy = CryptoSpotFeePolicy()
    assert policy.as_dict() == {
        "policyId": "crypto_spot_okx_demo_v1",
        "makerRate": "0.001",
        "takerRate": "0.0015",
        "feeCurrency": "USDT",
    }
    assert policy.estimate(
        quantity="0.01", price="10000", liquidity_role="MAKER"
    ) == Decimal("0.10000")
    assert policy.estimate(
        quantity="0.01", price="10000", liquidity_role="UNKNOWN"
    ) == Decimal("0.150000")
