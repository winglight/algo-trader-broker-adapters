from __future__ import annotations

from pathlib import Path

import pytest
from algo_trader_broker_adapter_ccxt_crypto import CCXTCryptoAdapter
from algo_trader_broker_adapter_ccxt_crypto.settings import CCXTCryptoSettings
from algo_trader_broker_sdk import BrokerContractError, assert_manifest_compatible

from .fakes import FakeClient, settings


def test_manifest_is_paper_crypto_spot_and_fail_closed() -> None:
    adapter = CCXTCryptoAdapter(settings(public_data_enabled=False), backend=FakeClient())
    manifest = adapter.manifest()
    assert_manifest_compatible(manifest)
    assert manifest.adapter_id == "ccxt_crypto"
    assert manifest.environment == "PAPER"
    assert manifest.capabilities.asset_classes == {"CRYPTO_SPOT"}
    assert manifest.capabilities.order_types == {"MKT", "LMT"}
    assert manifest.capabilities.time_in_force == {"GTC"}
    assert manifest.capabilities.supports_shorting is False


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"exchange_id": "binance"}, "only OKX Demo"),
        ({"sandbox": False}, "sandbox must remain true"),
        ({"live": True}, "live must remain false"),
        ({"allowed_symbols": "SOL/USDT"}, "allowed_symbols"),
        ({"trading_enabled": True}, "requires private_read_enabled"),
    ],
)
def test_settings_reject_unsafe_configuration(override, message) -> None:
    with pytest.raises(BrokerContractError, match=message):
        CCXTCryptoSettings.from_mapping(settings(**override))


def test_private_flags_require_all_three_credentials() -> None:
    with pytest.raises(BrokerContractError, match="api_key, secret, and passphrase"):
        CCXTCryptoSettings.from_mapping(settings(private_read_enabled=True))
    parsed = CCXTCryptoSettings.from_mapping(
        settings(private_read_enabled=True, api_key="key", secret="secret", passphrase="pass")
    )
    redacted = parsed.redacted()
    assert redacted["credentials_configured"] is True
    assert "key" not in repr(redacted)
    assert "pass" not in repr(redacted)


def test_package_is_isolated_and_pins_ccxt() -> None:
    root = Path(__file__).parents[1]
    source = "\n".join(path.read_text() for path in (root / "src").rglob("*.py"))
    pyproject = (root / "pyproject.toml").read_text()
    assert "from src" not in source
    assert "import src" not in source
    assert "ati_shared_sdk" not in source
    assert '"ccxt==4.5.56"' in pyproject
    assert 'ccxt_crypto = "algo_trader_broker_adapter_ccxt_crypto:create_adapter"' in pyproject
