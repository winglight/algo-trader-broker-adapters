from __future__ import annotations

from datetime import UTC, datetime

import pytest

from algo_trader_broker_adapter_alpaca_paper.mapping import (
    asset_is_active,
    broker_status,
    map_fill_activity,
    map_trade_update,
)


def test_asset_active_supports_native_status_enum() -> None:
    from enum import Enum

    class Status(str, Enum):
        ACTIVE = "active"
        INACTIVE = "inactive"

    assert asset_is_active({"status": Status.ACTIVE}) is True
    assert asset_is_active({"status": Status.INACTIVE}) is False
    assert asset_is_active({"active": True, "status": Status.INACTIVE}) is True


@pytest.mark.parametrize(
    ("native", "expected"),
    [
        ("accepted", "PendingSubmit"),
        ("new", "Submitted"),
        ("partially_filled", "Submitted"),
        ("filled", "Filled"),
        ("canceled", "Cancelled"),
        ("replaced", "Cancelled"),
        ("rejected", "Rejected"),
        ("expired", "Inactive"),
        ("pending_cancel", "Submitted"),
        ("future_vendor_status", "Unknown"),
    ],
)
def test_order_status_mapping_is_frozen(native: str, expected: str) -> None:
    assert broker_status(native) == expected


def test_trade_update_preserves_uuid_client_id_and_utc() -> None:
    update = map_trade_update(
        {
            "id": "7c41fb25-57ad-4b60-8ad7-c2d713ebd080",
            "client_order_id": "client-123",
            "symbol": "SPY",
            "qty": "10",
            "filled_qty": "4",
            "filled_avg_price": "601.25",
            "status": "partially_filled",
        },
        event="partial_fill",
        execution_id="execution-uuid",
        event_time="2026-07-15T14:31:00Z",
        last_fill_price="601.25",
        last_fill_quantity="4",
    )

    assert update.adapter_order_id == "7c41fb25-57ad-4b60-8ad7-c2d713ebd080"
    assert update.client_order_id == "client-123"
    assert update.adapter_execution_id == "execution-uuid"
    assert update.status == "Submitted"
    assert update.filled == 4
    assert update.remaining == 6
    assert update.event_time == datetime(2026, 7, 15, 14, 31, tzinfo=UTC)
    assert update.commission == 0.0
    assert update.message["commissionReport"] == {
        "execId": "execution-uuid",
        "commission": 0.0,
        "currency": "USD",
        "authoritativeZero": True,
        "source": "zero_commission_us_equity_paper",
    }
    accounting = update.adapter_metadata["extensions"]["accounting"]
    assert accounting["commission"]["authoritativeZero"] is True
    assert accounting["realizedPnl"] == {
        "model": "matched_fill_ledger_v1",
        "currency": "USD",
        "priceMultiplier": 1.0,
    }


def test_fill_activity_uses_stable_activity_id() -> None:
    update = map_fill_activity(
        {
            "id": "fill-activity-uuid",
            "order_id": "order-uuid",
            "symbol": "AAPL",
            "qty": "2",
            "price": "210.50",
            "transaction_time": "2026-07-15T14:31:00+00:00",
        }
    )

    assert update.adapter_execution_id == "fill-activity-uuid"
    assert update.adapter_order_id == "order-uuid"


def test_fill_activity_canonicalizes_composite_id_to_live_execution_id() -> None:
    update = map_fill_activity(
        {
            "id": "20260715122204835::1f1e081b-a14f-44bc-a6b5-fd45d1ea5491",
            "order_id": "order-uuid",
            "symbol": "AAPL",
            "qty": "1",
            "price": "327.66",
            "transaction_time": "2026-07-15T16:22:04+00:00",
        }
    )

    assert update.adapter_execution_id == "1f1e081b-a14f-44bc-a6b5-fd45d1ea5491"
