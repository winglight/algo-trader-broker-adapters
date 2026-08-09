"""CCXT Crypto Paper adapter package."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .adapter import CCXTCryptoAdapter


def create_adapter(settings: Mapping[str, Any]) -> CCXTCryptoAdapter:
    return CCXTCryptoAdapter(settings)


__all__ = ["CCXTCryptoAdapter", "create_adapter"]
