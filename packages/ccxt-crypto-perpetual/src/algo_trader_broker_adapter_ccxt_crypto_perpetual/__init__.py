"""OKX Demo USDT perpetual adapter package."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .adapter import CCXTCryptoPerpetualAdapter


def create_adapter(settings: Mapping[str, Any]) -> CCXTCryptoPerpetualAdapter:
    return CCXTCryptoPerpetualAdapter(settings)


__all__ = ["CCXTCryptoPerpetualAdapter", "create_adapter"]
