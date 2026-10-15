from __future__ import annotations

import logging

from algo_trader_broker_adapter_ibkr_paper.logging_policy import (
    IBExpectedScannerCancellationFilter,
    mark_expected_scanner_cancellation,
)


def _record(message: str) -> logging.LogRecord:
    return logging.LogRecord(
        name="ib_async.wrapper",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )


def test_active_scanner_cancel_acknowledgement_is_demoted_once() -> None:
    filter_ = IBExpectedScannerCancellationFilter()
    mark_expected_scanner_cancellation(41)
    expected = _record(
        "Error 162, reqId 41: Historical Market Data Service error message:"
        "API scanner subscription cancelled: 41"
    )

    assert filter_.filter(expected) is True
    assert expected.levelno == logging.DEBUG
    assert expected.levelname == "DEBUG"
    assert expected.ib_expected_cancellation is True  # type: ignore[attr-defined]

    repeated = _record(
        "Error 162, reqId 41: Historical Market Data Service error message:"
        "API scanner subscription cancelled: 41"
    )
    assert filter_.filter(repeated) is True
    assert repeated.levelno == logging.ERROR


def test_untracked_scanner_cancel_and_other_162_errors_remain_errors() -> None:
    filter_ = IBExpectedScannerCancellationFilter()
    untracked = _record(
        "Error 162, reqId 99: Historical Market Data Service error message:"
        "API scanner subscription cancelled: 99"
    )
    competing = _record(
        "Error 162, reqId 42: Trading TWS session is connected from a different IP"
    )

    assert filter_.filter(untracked) is True
    assert untracked.levelno == logging.ERROR
    assert filter_.filter(competing) is True
    assert competing.levelno == logging.ERROR


def test_native_logging_installs_once_and_keeps_unrelated_errors():
    from algo_trader_broker_adapter_ibkr_paper.logging_policy import install_native_logging
    logger=logging.getLogger("ib_async.wrapper")
    saved=list(logger.filters)
    try:
        logger.filters.clear()
        install_native_logging()
        first=list(logger.filters)
        install_native_logging()
        assert logger.filters == first
        unrelated=_record("Error 162: Trading TWS session is connected from a different IP")
        assert logger.filter(unrelated) is True and unrelated.levelno == logging.ERROR
        expected=_record("Error 366: No historical data query found")
        assert logger.filter(expected) is False
        foreign=logging.LogRecord("another.provider",logging.ERROR,__file__,1,
            "Error 366: No historical data query found",(),None)
        assert all(policy.filter(foreign) for policy in first)
    finally:
        logger.filters[:]=saved
