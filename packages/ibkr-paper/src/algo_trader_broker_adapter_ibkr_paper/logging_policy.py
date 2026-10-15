"""Logging filters for Interactive Brokers integrations."""

from __future__ import annotations

import logging

__all__ = [
    "IBError300Filter",
    "IBCancelMktDepthFilter",
    "IBHistoricalCancelFilter",
    "IBHistoricalNotFoundFilter",
    "IBWrapperInfoFilter",
]


class IBError300Filter(logging.Filter):
    """Filter out noisy IB API error 300 messages."""

    target_logger = "ib_async.wrapper"
    error_code = "300"
    error_message = "Can't find EId"

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401 - inherited
        if record.name != self.target_logger:
            return True

        if self._matches(record.getMessage()):
            return False

        for key, value in record.__dict__.items():
            if key in {"msg", "args"}:
                continue
            if self._matches(value):
                return False

        return True

    @classmethod
    def _matches(cls, value: object) -> bool:
        if value is None:
            return False

        if isinstance(value, str):
            text = value
        else:
            try:
                text = str(value)
            except Exception:
                return False

        return cls.error_code in text and cls.error_message in text


class IBCancelMktDepthFilter(logging.Filter):
    """Filter out noisy IB cancelMktDepth 'No reqId found' messages."""

    target_logger = "ib_async.ib"
    error_patterns = [
        "cancelMktDepth: No reqId found",
        "cancelMktData: No reqId found",
        "cancelHistoricalData: No reqId found",
    ]

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401 - inherited
        if record.name != self.target_logger:
            return True

        message = record.getMessage()
        if any(pattern in message for pattern in self.error_patterns):
            return False

        # Also check other record attributes
        for key, value in record.__dict__.items():
            if key in {"msg", "args"}:
                continue
            if self._matches_patterns(value):
                return False

        return True

    @classmethod
    def _matches_patterns(cls, value: object) -> bool:
        if value is None:
            return False

        if isinstance(value, str):
            text = value
        else:
            try:
                text = str(value)
            except Exception:
                return False

        return any(pattern in text for pattern in cls.error_patterns)


class IBHistoricalCancelFilter(logging.Filter):
    """Filter out IB error 162 'API historical data query cancelled' noise.

    When historical data requests are intentionally cancelled (e.g. closing
    an async iterator), IB emits error code 162 via the ``ib_async.wrapper``
    logger. This is expected behaviour and should not be treated as an error
    in our logs; we filter these records to reduce noise.
    """

    target_logger = "ib_async.wrapper"
    error_code = "162"
    error_message = "API historical data query cancelled"

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401 - inherited
        if record.name != self.target_logger:
            return True

        if self._matches(record.getMessage()):
            return False

        # Also inspect other attributes for structured logs
        for key, value in record.__dict__.items():
            if key in {"msg", "args"}:
                continue
            if self._matches(value):
                return False

        return True

    @classmethod
    def _matches(cls, value: object) -> bool:
        if value is None:
            return False

        if isinstance(value, str):
            text = value
        else:
            try:
                text = str(value)
            except Exception:
                return False

        # Match either "Error 162" + message or direct message text
        return ("Error " + cls.error_code) in text and cls.error_message in text


class IBHistoricalNotFoundFilter(logging.Filter):
    """Filter out IB error 366 'No historical data query found' noise."""

    target_logger = "ib_async.wrapper"
    error_code = "366"
    error_message = "No historical data query found"

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401 - inherited
        if record.name != self.target_logger:
            return True

        if self._matches(record.getMessage()):
            return False

        for key, value in record.__dict__.items():
            if key in {"msg", "args"}:
                continue
            if self._matches(value):
                return False

        return True

    @classmethod
    def _matches(cls, value: object) -> bool:
        if value is None:
            return False

        if isinstance(value, str):
            text = value
        else:
            try:
                text = str(value)
            except Exception:
                return False

        return ("Error " + cls.error_code) in text and cls.error_message in text


class IBWrapperInfoFilter(logging.Filter):
    """Filter out high-volume INFO messages from ``ib_async.wrapper``.

    Suppresses routine portfolio/position/execution chatter that is noisy at
    INFO level, while allowing WARNING/ERROR through.
    """

    target_logger = "ib_async.wrapper"
    suppressed_prefixes = (
        "position:",
        "execDetails",
        "orderStatus:",
        "commissionReport:",
        "updatePortfolio:",
    )

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401 - inherited
        if record.name != self.target_logger:
            return True
        # Only suppress INFO-level noise
        if getattr(record, "levelno", logging.INFO) != logging.INFO:
            return True

        message = record.getMessage()
        if self._matches(message):
            return False

        # Also inspect other attributes for structured logs
        for key, value in record.__dict__.items():
            if key in {"msg", "args"}:
                continue
            if self._matches(value):
                return False

        return True

    @classmethod
    def _matches(cls, value: object) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            text = value
        else:
            try:
                text = str(value)
            except Exception:
                return False
        # Fast check by prefix
        for prefix in cls.suppressed_prefixes:
            if text.startswith(prefix):
                return True
        return False


import re
import threading
import time


_SCANNER_CANCELLED = re.compile(
    r"Error\s+162,\s*reqId\s+(-?\d+):.*API scanner subscription cancelled",
    re.IGNORECASE,
)
_EXPECTED_TTL_SECONDS = 60.0
_EXPECTED: dict[int, float] = {}
_LOCK = threading.Lock()


def mark_expected_scanner_cancellation(req_id: int | None) -> None:
    """Mark one native reqId before actively cancelling its scanner stream."""

    if req_id is None:
        return
    now = time.monotonic()
    with _LOCK:
        _discard_expired(now)
        _EXPECTED[int(req_id)] = now + _EXPECTED_TTL_SECONDS


class IBExpectedScannerCancellationFilter(logging.Filter):
    """Demote only acknowledgements paired with an active local cancellation."""

    def filter(self, record: logging.LogRecord) -> bool:
        match = _SCANNER_CANCELLED.search(record.getMessage())
        if match is None:
            return True
        req_id = int(match.group(1))
        now = time.monotonic()
        with _LOCK:
            _discard_expired(now)
            expected = _EXPECTED.pop(req_id, None)
        if expected is None:
            return True
        record.levelno = logging.DEBUG
        record.levelname = "DEBUG"
        record.ib_expected_cancellation = True  # type: ignore[attr-defined]
        return True


def _discard_expired(now: float) -> None:
    for req_id, deadline in tuple(_EXPECTED.items()):
        if deadline <= now:
            _EXPECTED.pop(req_id, None)




class IBCancelledConnectionFilter(logging.Filter):
    def filter(self, record):
        return "API connection failed: CancelledError()" not in record.getMessage()


def install_native_logging():
    """Install policies on their emitting native loggers, once per process."""
    groups = {
        "ib_async.wrapper": (IBError300Filter, IBHistoricalCancelFilter,
            IBHistoricalNotFoundFilter, IBWrapperInfoFilter, IBExpectedScannerCancellationFilter),
        "ib_async.ib": (IBCancelMktDepthFilter,),
        "ib_async.client": (IBCancelledConnectionFilter,),
    }
    for name, policies in groups.items():
        logger = logging.getLogger(name)
        for policy in policies:
            if not any(isinstance(current, policy) for current in logger.filters):
                logger.addFilter(policy())
