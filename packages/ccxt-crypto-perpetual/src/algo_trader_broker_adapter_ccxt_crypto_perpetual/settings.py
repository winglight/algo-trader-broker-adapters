"""Strict OKX Demo-only settings for the Phase 5 perpetual adapter."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from algo_trader_broker_sdk import BrokerContractError

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}
_ALLOWED_SYMBOLS = ("BTC/USDT:USDT", "ETH/USDT:USDT")


def _bool(settings: Mapping[str, Any], key: str, default: bool) -> bool:
    raw = settings.get(key)
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    token = str(raw).strip().lower()
    if token in _TRUE:
        return True
    if token in _FALSE:
        return False
    raise BrokerContractError(f"{key} must be an explicit boolean")


def _int(settings: Mapping[str, Any], key: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(str(settings.get(key, default)).strip())
    except (TypeError, ValueError) as exc:
        raise BrokerContractError(f"{key} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise BrokerContractError(
            f"{key} is outside the supported range",
            details={"minimum": minimum, "maximum": maximum},
        )
    return value


@dataclass(frozen=True, slots=True)
class CCXTCryptoPerpetualSettings:
    api_key: str
    secret: str
    passphrase: str
    allowed_symbols: tuple[str, ...]
    execution_target_id: str
    market_data_target_id: str
    account_id: str
    fixed_leverage: int
    request_timeout_ms: int
    reconcile_interval_seconds: int
    full_reconcile_interval_seconds: int
    clock_skew_block_ms: int

    @classmethod
    def from_mapping(cls, settings: Mapping[str, Any]) -> "CCXTCryptoPerpetualSettings":
        if str(settings.get("exchange_id") or "okx").strip().lower() != "okx":
            raise BrokerContractError("Phase 5 supports only OKX Demo Trading")
        if not _bool(settings, "sandbox", True):
            raise BrokerContractError("sandbox must remain true")
        if _bool(settings, "live", False):
            raise BrokerContractError("live must remain false")
        if str(settings.get("position_mode") or "ONE_WAY").strip().upper() != "ONE_WAY":
            raise BrokerContractError("position_mode must remain ONE_WAY")
        if str(settings.get("margin_mode") or "ISOLATED").strip().upper() != "ISOLATED":
            raise BrokerContractError("margin_mode must remain ISOLATED")
        leverage = _int(settings, "fixed_leverage", 2, 1, 2)
        if leverage != 2:
            raise BrokerContractError("fixed_leverage must equal 2")

        raw_symbols = settings.get("allowed_symbols", _ALLOWED_SYMBOLS)
        if isinstance(raw_symbols, str):
            symbols = tuple(item.strip().upper() for item in raw_symbols.split(",") if item.strip())
        else:
            symbols = tuple(str(item).strip().upper() for item in raw_symbols if str(item).strip())
        if not symbols or set(symbols) - set(_ALLOWED_SYMBOLS):
            raise BrokerContractError(
                "allowed_symbols must be a non-empty subset of BTC/USDT:USDT and ETH/USDT:USDT"
            )

        api_key = str(settings.get("api_key") or "").strip()
        secret = str(settings.get("secret") or "").strip()
        passphrase = str(settings.get("passphrase") or "").strip()
        if not all((api_key, secret, passphrase)):
            raise BrokerContractError("OKX Demo credentials are required")

        execution_target_id = str(
            settings.get("execution_target_id") or "okx-perpetual-demo-paper-1"
        ).strip()
        market_data_target_id = str(
            settings.get("market_data_target_id") or "okx-perpetual-demo-market-1"
        ).strip()
        if execution_target_id != "okx-perpetual-demo-paper-1":
            raise BrokerContractError("execution_target_id does not match Phase 5 design")
        if market_data_target_id != "okx-perpetual-demo-market-1":
            raise BrokerContractError("market_data_target_id does not match Phase 5 design")
        if execution_target_id == market_data_target_id:
            raise BrokerContractError("execution and market-data targets must be distinct")

        return cls(
            api_key=api_key,
            secret=secret,
            passphrase=passphrase,
            allowed_symbols=symbols,
            execution_target_id=execution_target_id,
            market_data_target_id=market_data_target_id,
            account_id=str(settings.get("account_id") or "okx-demo-perpetual").strip(),
            fixed_leverage=leverage,
            request_timeout_ms=_int(settings, "request_timeout_ms", 30000, 5000, 60000),
            reconcile_interval_seconds=_int(
                settings, "reconcile_interval_seconds", 60, 15, 3600
            ),
            full_reconcile_interval_seconds=_int(
                settings, "full_reconcile_interval_seconds", 900, 60, 86400
            ),
            clock_skew_block_ms=_int(settings, "clock_skew_block_ms", 1000, 250, 5000),
        )

    def redacted(self) -> dict[str, Any]:
        fingerprint = hashlib.sha256(
            f"{self.api_key}\0{self.secret}\0{self.passphrase}".encode()
        ).hexdigest()[:12]
        return {
            "exchange_id": "okx",
            "sandbox": True,
            "live": False,
            "credentials_configured": True,
            "credential_fingerprint": fingerprint,
            "allowed_symbols": list(self.allowed_symbols),
            "execution_target_id": self.execution_target_id,
            "market_data_target_id": self.market_data_target_id,
            "position_mode": "ONE_WAY",
            "margin_mode": "ISOLATED",
            "fixed_leverage": self.fixed_leverage,
        }
