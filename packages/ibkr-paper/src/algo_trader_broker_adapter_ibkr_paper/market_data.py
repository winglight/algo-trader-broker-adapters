"""Dataclasses used by market data helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping


@dataclass(slots=True)
class DOMLevel:
    """Single level in the depth of market."""

    price: float
    size: float
    market_maker: str | None
    position: int


@dataclass(slots=True)
class DOMSnapshot:
    """Aggregated depth of market snapshot."""

    bids: tuple[DOMLevel, ...]
    asks: tuple[DOMLevel, ...]
    timestamp: datetime
    exchanges: tuple[Mapping[str, Any], ...] = field(default_factory=tuple)


@dataclass(slots=True)
class HistoricalBar:
    """Historical OHLCV bar."""

    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    wap: float | None
    count: int | None


@dataclass(slots=True)
class RealTimePrice:
    """Real-time price tick."""

    symbol: str
    bid: float | None
    ask: float | None
    last: float | None
    last_size: float | None
    close: float | None
    timestamp: datetime
    bid_size: float | None = None
    ask_size: float | None = None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    volume: float | None = None


@dataclass(slots=True)
class TickByTickBidAsk:
    """Bid/ask tick-by-tick update."""

    time: datetime
    bid_price: float
    ask_price: float
    bid_size: float
    ask_size: float
    bid_past_low: bool | None = None
    ask_past_high: bool | None = None


@dataclass(slots=True)
class TickByTickLast:
    """Last trade tick-by-tick update."""

    time: datetime
    price: float
    size: float
    exchange: str | None = None
    special_conditions: str | None = None
    past_limit: bool | None = None
    unreported: bool | None = None


@dataclass(slots=True)
class TickByTickMidPoint:
    """Midpoint tick-by-tick update."""

    time: datetime
    mid_price: float


@dataclass(slots=True)
class HistoricalTickBidAsk:
    """Historical bid/ask tick."""

    time: datetime
    bid_price: float
    ask_price: float
    bid_size: float
    ask_size: float
    bid_past_low: bool | None = None
    ask_past_high: bool | None = None


@dataclass(slots=True)
class HistoricalTickLast:
    """Historical last trade tick."""

    time: datetime
    price: float
    size: float
    exchange: str | None = None
    special_conditions: str | None = None
    past_limit: bool | None = None
    unreported: bool | None = None
