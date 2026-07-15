from __future__ import annotations

import pytest

from algo_trader_broker_adapter_alpaca_paper.settings import AlpacaPaperSettings
from algo_trader_broker_sdk import BrokerContractError


def test_settings_require_credentials_only_at_adapter_construction() -> None:
    with pytest.raises(BrokerContractError) as exc:
        AlpacaPaperSettings.from_mapping({})

    assert exc.value.details == {"setting": "alpaca_api_key_id"}


def test_settings_are_strict_and_redacted() -> None:
    settings = AlpacaPaperSettings.from_mapping(
        {
            "alpaca_api_key_id": "KEY-VALUE",
            "alpaca_secret_key": "SECRET-VALUE",
            "alpaca_data_feed": "SIP",
            "alpaca_request_timeout_seconds": "10",
            "alpaca_reconcile_lookback_hours": "24",
            "alpaca_max_concurrency": "4",
            "alpaca_stream_queue_size": "256",
        }
    )

    assert settings.data_feed == "sip"
    assert settings.redacted() == {
        "data_feed": "sip",
        "request_timeout_seconds": 10.0,
        "reconcile_lookback_hours": 24,
        "max_concurrency": 4,
        "stream_queue_size": 256,
    }
    assert "KEY-VALUE" not in repr(settings.redacted())
    assert "SECRET-VALUE" not in repr(settings.redacted())


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("alpaca_data_feed", "auto"),
        ("alpaca_request_timeout_seconds", "0"),
        ("alpaca_reconcile_lookback_hours", "0"),
        ("alpaca_max_concurrency", "100"),
        ("alpaca_stream_queue_size", "4"),
    ],
)
def test_settings_reject_invalid_or_fallback_values(key: str, value: str) -> None:
    payload = {
        "alpaca_api_key_id": "test-key",
        "alpaca_secret_key": "test-secret",
        key: value,
    }
    with pytest.raises(BrokerContractError):
        AlpacaPaperSettings.from_mapping(payload)
