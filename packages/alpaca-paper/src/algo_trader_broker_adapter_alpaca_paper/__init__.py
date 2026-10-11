"""Alpaca Paper adapter for algo-trader Broker SDK 1.x."""

from .adapter import AlpacaPaperAdapter, create_adapter
from ._version import VERSION

__version__ = VERSION

__all__ = ["AlpacaPaperAdapter", "create_adapter"]
