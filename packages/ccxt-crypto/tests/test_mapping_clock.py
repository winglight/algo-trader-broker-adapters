from __future__ import annotations

from datetime import UTC, datetime, timedelta

from ati_shared_sdk.common.schemas import BrokerOrderUpdateV2
from algo_trader_broker_adapter_ccxt_crypto.mapping import order_update


def test_order_update_available_at_never_precedes_future_provider_event() -> None:
    event_time = datetime.now(UTC) + timedelta(seconds=2)
    payload = order_update(
        {
            "id": "order-1",
            "clientOrderId": "client-1",
            "symbol": "BTC/USDT",
            "amount": "0.001",
            "filled": "0",
            "status": "open",
            "timestamp": int(event_time.timestamp() * 1000),
        },
        execution_target_id="okx-spot-demo-paper-1",
        command_id="command-1",
    )

    parsed = BrokerOrderUpdateV2.model_validate(payload)
    assert parsed.available_at >= parsed.event_time
