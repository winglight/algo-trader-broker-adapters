"""Strict configuration for the Alpaca Paper package."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from algo_trader_broker_sdk import BrokerContractError


def _required(settings: Mapping[str, Any], key: str) -> str:
    value = str(settings.get(key) or "").strip()
    if not value:
        raise BrokerContractError(
            f"{key} is required for Alpaca Paper",
            details={"setting": key},
        )
    return value


def _bounded_float(
    settings: Mapping[str, Any], key: str, default: float, minimum: float, maximum: float
) -> float:
    raw = settings.get(key, default)
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise BrokerContractError(
            f"{key} must be numeric", details={"setting": key}
        ) from exc
    if not minimum <= value <= maximum:
        raise BrokerContractError(
            f"{key} is outside the supported range",
            details={"setting": key, "minimum": minimum, "maximum": maximum},
        )
    return value


def _bounded_int(
    settings: Mapping[str, Any], key: str, default: int, minimum: int, maximum: int
) -> int:
    raw = settings.get(key, default)
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise BrokerContractError(
            f"{key} must be an integer", details={"setting": key}
        ) from exc
    if not minimum <= value <= maximum:
        raise BrokerContractError(
            f"{key} is outside the supported range",
            details={"setting": key, "minimum": minimum, "maximum": maximum},
        )
    return value


@dataclass(frozen=True, slots=True)
class AlpacaPaperSettings:
    api_key_id: str
    secret_key: str
    data_feed: str = "iex"
    request_timeout_seconds: float = 15.0
    reconcile_lookback_hours: int = 72
    max_concurrency: int = 8
    stream_queue_size: int = 512

    @classmethod
    def from_mapping(cls, settings: Mapping[str, Any]) -> "AlpacaPaperSettings":
        data_feed = str(settings.get("alpaca_data_feed") or "iex").strip().lower()
        if data_feed not in {"iex", "sip"}:
            raise BrokerContractError(
                "alpaca_data_feed must be iex or sip",
                details={"setting": "alpaca_data_feed"},
            )
        return cls(
            api_key_id=_required(settings, "alpaca_api_key_id"),
            secret_key=_required(settings, "alpaca_secret_key"),
            data_feed=data_feed,
            request_timeout_seconds=_bounded_float(
                settings,
                "alpaca_request_timeout_seconds",
                15.0,
                1.0,
                60.0,
            ),
            reconcile_lookback_hours=_bounded_int(
                settings,
                "alpaca_reconcile_lookback_hours",
                72,
                1,
                720,
            ),
            max_concurrency=_bounded_int(
                settings, "alpaca_max_concurrency", 8, 1, 32
            ),
            stream_queue_size=_bounded_int(
                settings, "alpaca_stream_queue_size", 512, 32, 4096
            ),
        )

    def redacted(self) -> dict[str, object]:
        return {
            "data_feed": self.data_feed,
            "request_timeout_seconds": self.request_timeout_seconds,
            "reconcile_lookback_hours": self.reconcile_lookback_hours,
            "max_concurrency": self.max_concurrency,
            "stream_queue_size": self.stream_queue_size,
        }
