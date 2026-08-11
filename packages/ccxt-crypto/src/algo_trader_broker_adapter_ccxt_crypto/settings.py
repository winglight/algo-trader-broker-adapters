"""Strict settings for the OKX Demo-only CCXT adapter."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from algo_trader_broker_sdk import BrokerContractError

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}
_DEFAULT_SYMBOLS = ("BTC/USDT", "ETH/USDT")


def _bool(settings: Mapping[str, Any], key: str, default: bool) -> bool:
    raw = settings.get(key)
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    value = str(raw).strip().lower()
    if value in _TRUE:
        return True
    if value in _FALSE:
        return False
    raise BrokerContractError(f"{key} must be an explicit boolean", details={"setting": key})


def _int(
    settings: Mapping[str, Any], key: str, default: int, minimum: int, maximum: int
) -> int:
    try:
        value = int(str(settings.get(key, default)).strip())
    except (TypeError, ValueError) as exc:
        raise BrokerContractError(f"{key} must be an integer", details={"setting": key}) from exc
    if not minimum <= value <= maximum:
        raise BrokerContractError(
            f"{key} is outside the supported range",
            details={"setting": key, "minimum": minimum, "maximum": maximum},
        )
    return value


def _decimal(
    settings: Mapping[str, Any], key: str, default: str, minimum: str, maximum: str
) -> Decimal:
    try:
        value = Decimal(str(settings.get(key, default)).strip())
    except (InvalidOperation, ValueError) as exc:
        raise BrokerContractError(f"{key} must be decimal", details={"setting": key}) from exc
    if not Decimal(minimum) <= value <= Decimal(maximum):
        raise BrokerContractError(
            f"{key} is outside the supported range",
            details={"setting": key, "minimum": minimum, "maximum": maximum},
        )
    return value


def _secret(settings: Mapping[str, Any], key: str) -> str:
    return str(settings.get(key) or "").strip()


@dataclass(frozen=True, slots=True)
class CCXTCryptoSettings:
    api_key: str
    secret: str
    passphrase: str
    allowed_symbols: tuple[str, ...]
    execution_target_id: str
    market_data_target_id: str
    rest_max_concurrency: int
    request_timeout_ms: int
    reconcile_interval_seconds: int
    full_reconcile_interval_seconds: int
    clock_skew_block_ms: int
    minimum_notional: Decimal

    @classmethod
    def from_mapping(cls, settings: Mapping[str, Any]) -> CCXTCryptoSettings:
        exchange_id = str(settings.get("exchange_id") or "okx").strip().lower()
        if exchange_id != "okx":
            raise BrokerContractError(
                "CCXT Crypto Paper Phase 4 supports only OKX Demo Trading",
                details={"setting": "exchange_id"},
            )
        if not _bool(settings, "sandbox", True):
            raise BrokerContractError("sandbox must remain true for OKX Demo")
        if _bool(settings, "live", False):
            raise BrokerContractError("live must remain false for OKX Demo")

        raw_symbols = settings.get("allowed_symbols", _DEFAULT_SYMBOLS)
        if isinstance(raw_symbols, str):
            symbols = tuple(item.strip().upper() for item in raw_symbols.split(",") if item.strip())
        else:
            symbols = tuple(str(item).strip().upper() for item in raw_symbols if str(item).strip())
        if not symbols or set(symbols) - set(_DEFAULT_SYMBOLS):
            raise BrokerContractError(
                "allowed_symbols must be a non-empty subset of BTC/USDT and ETH/USDT",
                details={"setting": "allowed_symbols"},
            )

        api_key = _secret(settings, "api_key")
        secret = _secret(settings, "secret")
        passphrase = _secret(settings, "passphrase")
        if not all((api_key, secret, passphrase)):
            raise BrokerContractError(
                "OKX Demo requires api_key, secret, and passphrase",
                details={"configured": False},
            )

        execution_target_id = str(
            settings.get("execution_target_id") or "okx-spot-demo-paper-1"
        ).strip()
        market_data_target_id = str(
            settings.get("market_data_target_id") or "okx-spot-demo-market-1"
        ).strip()
        if execution_target_id != "okx-spot-demo-paper-1":
            raise BrokerContractError("execution_target_id does not match Phase 4 design")
        if market_data_target_id != "okx-spot-demo-market-1":
            raise BrokerContractError("market_data_target_id does not match Phase 4 design")

        return cls(
            api_key=api_key,
            secret=secret,
            passphrase=passphrase,
            allowed_symbols=symbols,
            execution_target_id=execution_target_id,
            market_data_target_id=market_data_target_id,
            rest_max_concurrency=_int(settings, "rest_max_concurrency", 4, 1, 4),
            request_timeout_ms=_int(
                settings, "request_timeout_ms", 30000, 5000, 60000
            ),
            reconcile_interval_seconds=_int(
                settings, "reconcile_interval_seconds", 60, 15, 3600
            ),
            full_reconcile_interval_seconds=_int(
                settings, "full_reconcile_interval_seconds", 900, 60, 86400
            ),
            clock_skew_block_ms=_int(settings, "clock_skew_block_ms", 1000, 250, 5000),
            minimum_notional=_decimal(
                settings, "minimum_notional", "5", "0.01", "1000"
            ),
        )

    def redacted(self) -> dict[str, object]:
        fingerprint = None
        if all((self.api_key, self.secret, self.passphrase)):
            material = f"{self.api_key}\0{self.secret}\0{self.passphrase}"
            fingerprint = hashlib.sha256(material.encode("utf-8")).hexdigest()[:12]
        return {
            "exchange_id": "okx",
            "sandbox": True,
            "live": False,
            "credentials_configured": all((self.api_key, self.secret, self.passphrase)),
            "credential_fingerprint": fingerprint,
            "allowed_symbols": list(self.allowed_symbols),
            "execution_target_id": self.execution_target_id,
            "market_data_target_id": self.market_data_target_id,
            "rest_max_concurrency": self.rest_max_concurrency,
            "request_timeout_ms": self.request_timeout_ms,
            "reconcile_interval_seconds": self.reconcile_interval_seconds,
            "full_reconcile_interval_seconds": self.full_reconcile_interval_seconds,
            "clock_skew_block_ms": self.clock_skew_block_ms,
            "minimum_notional": str(self.minimum_notional),
        }
