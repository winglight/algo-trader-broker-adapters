import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from algo_trader_broker_adapter_alpaca_paper.adapter import AlpacaPaperAdapter
from algo_trader_broker_adapter_alpaca_paper.clients import AlpacaClients
from algo_trader_broker_adapter_alpaca_paper.settings import AlpacaPaperSettings
from algo_trader_broker_sdk import BrokerContractError, BrokerOrderError, StockOrderRequest, SubmissionGate


@pytest.mark.parametrize("case", ["success", "wrong-account", "changed-during-asset-read"])
def test_real_adapter_backend_checks_native_account_and_consumes_gate_in_send_worker(case):
    async def scenario():
        sequence = []
        settings = AlpacaPaperSettings("synthetic-key", "synthetic-secret")
        backend = AlpacaClients(settings)
        changed = [False]
        def asset(symbol):
            sequence.append("asset")
            if case == "changed-during-asset-read": changed[0] = True
            return {"id": "asset", "class": "us_equity", "active": True, "tradable": True}
        async def authority():
            sequence.append("authority")
            if changed[0]: raise BrokerOrderError("connection changed", code="broker_submission_fenced")
        gate = SubmissionGate(authority, lambda: sequence.append("final-check"))
        def send(*, order_data):
            assert gate.consumed and order_data.symbol == "SPY" and order_data.qty == 1
            sequence.append("native-send")
            return {"id": "native-order", "client_order_id": "client", "symbol": "SPY", "qty": "1", "filled_qty": "0", "status": "accepted", "asset_class": "us_equity"}
        backend._trading = SimpleNamespace(get_account=lambda: {"id": "wrong" if case == "wrong-account" else "native-account"},
            get_asset=asset, submit_order=Mock(side_effect=send))
        adapter = AlpacaPaperAdapter({"alpaca_api_key_id": "synthetic-key", "alpaca_secret_key": "synthetic-secret"}, backend=backend)
        adapter.ensure_connected = AsyncMock()
        request = StockOrderRequest(symbol="SPY", side="BUY", quantity=1, order_type="LMT", limit_price=20, account="native-account", client_order_id="client")
        if case == "success":
            result = await adapter.place_stock_order_guarded(request, gate)
            assert result.adapter_order_id == "native-order"
            assert sequence == ["asset", "authority", "final-check", "native-send"]
        else:
            with pytest.raises((BrokerContractError, BrokerOrderError)):
                await adapter.place_stock_order_guarded(request, gate)
            backend._trading.submit_order.assert_not_called()
    asyncio.run(scenario())


def test_authority_check_waits_until_backend_concurrency_slot_is_available():
    async def scenario():
        backend = AlpacaClients(SimpleNamespace(max_concurrency=1, request_timeout_seconds=1))
        native = Mock(return_value="accepted")
        backend._trading = SimpleNamespace(submit_order=native)
        valid = [True]
        async def authorize():
            if not valid[0]: raise BrokerOrderError("stale after queue", code="broker_submission_fenced")
        gate = SubmissionGate(AsyncMock(side_effect=authorize), Mock())
        await backend._semaphore.acquire()
        task = asyncio.create_task(backend.submit_order(object(), submission_gate=gate))
        await asyncio.sleep(0)
        gate.validate.assert_not_awaited()
        valid[0] = False
        backend._semaphore.release()
        with pytest.raises(BrokerOrderError): await task
        native.assert_not_called()
    asyncio.run(scenario())


def test_cancelled_queued_worker_cannot_later_submit(monkeypatch):
    async def scenario():
        backend = AlpacaClients(SimpleNamespace(max_concurrency=1, request_timeout_seconds=1))
        native = Mock(return_value="accepted")
        backend._trading = SimpleNamespace(submit_order=native)
        gate = SubmissionGate(AsyncMock(), Mock())
        queued, ready = [], asyncio.Event()
        async def queue(function):
            queued.append(function)
            ready.set()
            await asyncio.Future()
        monkeypatch.setattr(asyncio, "to_thread", queue)
        task = asyncio.create_task(backend.submit_order(object(), submission_gate=gate))
        await ready.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError): await task
        with pytest.raises(BrokerOrderError): queued[0]()
        native.assert_not_called()
        assert not gate.consumed
    asyncio.run(scenario())
