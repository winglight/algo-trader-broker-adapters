"""IBKR Paper adapter for algo-trader Broker SDK 1.x."""

from .adapter import IBKRPaperAdapter, create_adapter
from ._version import VERSION

__version__ = VERSION

__all__ = ["IBKRPaperAdapter", "create_adapter"]
