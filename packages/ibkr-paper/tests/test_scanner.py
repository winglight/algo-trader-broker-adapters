from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from algo_trader_broker_adapter_ibkr_paper.client import (
    IBAsyncClient,
    _build_scanner_subscription,
)
from algo_trader_broker_adapter_ibkr_paper.exceptions import IBScreenerError
from algo_trader_broker_sdk import ScreenerDiscoveryRequest


class _Event:
    def __init__(self) -> None:
        self.handlers: list[Any] = []

    def __iadd__(self, handler: Any):
        self.handlers.append(handler)
        return self

    def __isub__(self, handler: Any):
        self.handlers.remove(handler)
        return self

    def emit(self, *args: Any) -> None:
        for handler in list(self.handlers):
            handler(*args)


class _ScanData(list[Any]):
    def __init__(self) -> None:
        super().__init__()
        self.reqId = 41
        self.updateEvent = _Event()


class _IB:
    def __init__(self) -> None:
        self.errorEvent = _Event()
        self.scan_data = _ScanData()
        self.cancel_count = 0
        self.scanner_filter_options: list[Any] = []

    def reqScannerSubscription(
        self,
        _subscription: Any,
        scannerSubscriptionOptions: list[Any] | None = None,
        scannerSubscriptionFilterOptions: list[Any] | None = None,
    ) -> _ScanData:
        self.scanner_filter_options = list(scannerSubscriptionFilterOptions or [])
        return self.scan_data

    def cancelScannerSubscription(self, scan_data: _ScanData) -> None:
        assert scan_data is self.scan_data
        self.cancel_count += 1


def _entry(rank: int, symbol: str, con_id: int) -> Any:
    contract = SimpleNamespace(
        symbol=symbol,
        localSymbol=symbol,
        exchange="SMART",
        primaryExchange="NASDAQ",
        currency="USD",
        secType="STK",
        conId=con_id,
        tradingClass="NMS",
    )
    return SimpleNamespace(
        rank=rank,
        contractDetails=SimpleNamespace(contract=contract),
        distance="1.25",
        benchmark="SPY",
        projection="2.50",
        legsStr="",
    )


def _request() -> ScreenerDiscoveryRequest:
    return ScreenerDiscoveryRequest(
        source_key="TOP_PERC_GAIN",
        instrument="STK",
        location_code="STK.US.MAJOR",
        max_rows=50,
        parameters={"priceAbove": 2, "priceBelow": 20},
    )


@pytest.mark.asyncio
async def test_external_adapter_stream_emits_full_snapshot_and_cancels_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ib = _IB()
    client = object.__new__(IBAsyncClient)

    async def ensure_connected() -> _IB:
        return ib

    monkeypatch.setattr(client, "ensure_connected", ensure_connected)
    stream = client.stream_scanner_data(_request())
    pending = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)
    ib.scan_data.extend([_entry(0, "AAPL", 1), _entry(1, "TSLA", 2)])
    ib.scan_data.updateEvent.emit(ib.scan_data)

    snapshot = await pending
    await stream.aclose()
    await stream.aclose()

    assert [(row.rank, row.symbol, row.contract_id) for row in snapshot.rows] == [
        (0, "AAPL", 1),
        (1, "TSLA", 2),
    ]
    assert snapshot.replace is True
    assert snapshot.rows[0].native_fields == {
        "distance": "1.25",
        "benchmark": "SPY",
        "projection": "2.50",
    }
    assert [(item.tag, item.value) for item in ib.scanner_filter_options] == [
        ("priceAbove", "2"),
        ("priceBelow", "20"),
    ]
    assert ib.cancel_count == 1
    assert ib.scan_data.updateEvent.handlers == []
    assert ib.errorEvent.handlers == []


def test_external_adapter_scanner_rejects_invalid_native_parameters() -> None:
    with pytest.raises(IBScreenerError) as error:
        _build_scanner_subscription(
            {
                "scanCode": "TOP_PERC_GAIN",
                "instrument": "STK",
                "locationCode": "STK.US.MAJOR",
                "numberOfRows": 51,
            }
        )

    assert error.value.code == "screener_scanner_parameter_invalid"
