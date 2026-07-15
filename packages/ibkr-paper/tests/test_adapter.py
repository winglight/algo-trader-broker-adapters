from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from algo_trader_broker_adapter_ibkr_paper.adapter import (
    IBKRPaperAdapter,
    _map_ib_order_result,
)
from algo_trader_broker_sdk import (
    BrokerCapabilityError,
    BrokerConnectionError,
    OptionOrderRequest,
)


class FakeIBClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.historical_kwargs: list[dict[str, object]] = []
        self.trade_handler = None
        self.position_handler = None
        self.account_handler = None
        self.connection_listeners = []
        self.resub_tasks = []

    async def connect(self) -> None:
        self.calls.append(("connect", None))

    async def disconnect(self, reason: str | None = None) -> None:
        self.calls.append(("disconnect", reason))

    async def reconnect(self, *, reason: str | None = None) -> None:
        self.calls.append(("reconnect", reason))

    async def ensure_connected(self) -> None:
        self.calls.append(("ensure_connected", None))

    def connection_state_snapshot(self) -> dict[str, object]:
        return {
            "connected": True,
            "connected_since": None,
            "reconnect_reason": None,
            "host": "127.0.0.1",
            "port": 4002,
            "client_id": 40,
        }

    def set_trade_update_handler(self, handler) -> None:
        self.trade_handler = handler

    def set_position_update_handler(self, handler) -> None:
        self.position_handler = handler

    def set_account_update_handler(self, handler) -> None:
        self.account_handler = handler

    def add_connection_listener(self, listener) -> None:
        self.connection_listeners.append(listener)

    def remove_connection_listener(self, listener) -> None:
        self.connection_listeners.remove(listener)

    def add_resub_task(self, coro_factory) -> None:
        self.resub_tasks.append(coro_factory)

    async def get_account_summary(self, account: str | None = None) -> list[object]:
        self.calls.append(("get_account_summary", account))
        return []

    async def get_account_pnl(self, account=None, *, model_code=None, timeout=5.0):
        self.calls.append(("get_account_pnl", (account, model_code, timeout)))
        return {"daily": 1.0}

    async def get_positions(self) -> list[object]:
        self.calls.append(("get_positions", None))
        return []

    async def cancel_order(self, order_id) -> None:
        self.calls.append(("cancel_order", order_id))

    async def request_open_orders(self) -> list[object]:
        self.calls.append(("request_open_orders", None))
        return []

    async def request_executions(self, since=None) -> list[object]:
        self.calls.append(("request_executions", since))
        return []

    async def request_completed_orders(self) -> list[object]:
        self.calls.append(("request_completed_orders", None))
        return []

    async def request_scanner_parameters(self) -> str:
        self.calls.append(("request_scanner_parameters", None))
        return "<ScannerParameters/>"

    async def request_scanner_data(self, payload, *, tag_filters=None, stream_seconds=None):
        self.calls.append(("request_scanner_data", (payload, tag_filters, stream_seconds)))
        return [{"symbol": "AAPL"}]

    async def qualify_contract(self, contract):
        self.calls.append(("qualify_contract", contract))
        return contract

    async def request_market_snapshot(self, contract, *, generic_tick_list="", snapshot_timeout=None):
        self.calls.append(("request_market_snapshot", contract))
        return {"symbol": getattr(contract, "symbol", None)}

    async def get_historical_data(self, contract, **kwargs):
        self.calls.append(("get_historical_data", contract))
        self.historical_kwargs.append(dict(kwargs))
        return []

    async def stream_real_time_price(self, contract, *, snapshot=False):
        self.calls.append(("stream_real_time_price", contract))
        return _FakeStream()

    async def stream_historical_bars(self, contract, **kwargs):
        self.calls.append(("stream_historical_bars", contract))
        self.historical_kwargs.append(dict(kwargs))
        return _FakeStream()


class FakeSupervisor:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


class _FakeStream:
    req_id = 42

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


def test_ib_initial_empty_inactive_ack_remains_non_terminal_until_update() -> None:
    result = _map_ib_order_result(
        SimpleNamespace(
            order_id=17617,
            perm_id=None,
            status="Inactive",
            filled=0,
            remaining=0,
            avg_fill_price=None,
            contract={"symbol": "MNQ"},
            rejection_reason=None,
        ),
        open_close="O",
    )

    assert result.status == "PendingSubmit"
    assert result.adapter_metadata["diagnostics"] == {
        "initialNativeStatus": "Inactive"
    }


def test_ib_initial_blank_ack_remains_non_terminal_until_update() -> None:
    result = _map_ib_order_result(
        SimpleNamespace(
            order_id=17624,
            perm_id=None,
            status="",
            filled=0,
            remaining=0,
            avg_fill_price=None,
            contract={"symbol": "MNQ"},
            rejection_reason=None,
        ),
        open_close="O",
    )

    assert result.status == "PendingSubmit"
    assert result.adapter_metadata["diagnostics"] == {"initialNativeStatus": ""}


@pytest.mark.asyncio
async def test_ibkr_paper_adapter_delegates_lifecycle_and_account_calls() -> None:
    client = FakeIBClient()
    supervisor = FakeSupervisor()
    adapter = IBKRPaperAdapter({}, client=client, supervisor=supervisor)

    await adapter.start()
    await adapter.connect()
    await adapter.get_account_summary("DU123")
    await adapter.get_account_pnl("DU123", model_code="M", timeout=1.5)
    await adapter.cancel_order(42)
    await adapter.close()

    assert supervisor.started is True
    assert supervisor.stopped is True
    assert ("connect", None) in client.calls
    assert ("get_account_summary", "DU123") in client.calls
    assert ("get_account_pnl", (None, "M", 1.5)) in client.calls
    assert ("cancel_order", 42) in client.calls


def test_ibkr_paper_adapter_manifest_excludes_dom_and_options() -> None:
    adapter = IBKRPaperAdapter({}, client=FakeIBClient(), supervisor=FakeSupervisor())

    manifest = adapter.manifest()
    capabilities = manifest.capabilities

    assert manifest.adapter_id == "ibkr_paper"
    assert capabilities.environment == "PAPER"
    assert "dom" not in capabilities.market_data_streams
    assert capabilities.supports_options is False


def test_ibkr_paper_adapter_delegates_event_registration() -> None:
    client = FakeIBClient()
    adapter = IBKRPaperAdapter({}, client=client, supervisor=FakeSupervisor())

    trade_handler = object()
    position_handler = object()
    listener = lambda state, payload: None
    resub = lambda: None

    adapter.set_trade_update_handler(trade_handler)  # type: ignore[arg-type]
    adapter.set_position_update_handler(position_handler)  # type: ignore[arg-type]
    adapter.set_account_update_handler(trade_handler)  # type: ignore[arg-type]
    adapter.add_connection_listener(listener)
    adapter.add_resub_task(resub)  # type: ignore[arg-type]
    adapter.remove_connection_listener(listener)

    assert client.trade_handler is trade_handler
    assert client.position_handler is position_handler
    assert client.account_handler is trade_handler
    assert client.connection_listeners == []
    assert client.resub_tasks == [resub]


@pytest.mark.asyncio
async def test_ibkr_paper_adapter_rejects_options_and_dom_without_delegation() -> None:
    client = FakeIBClient()
    adapter = IBKRPaperAdapter({}, client=client, supervisor=FakeSupervisor())

    with pytest.raises(BrokerCapabilityError) as option_exc:
        await adapter.place_option_order(
            OptionOrderRequest(
                symbol="AAPL",
                side="BUY",
                quantity=1,
                order_type="MKT",
            )
        )
    with pytest.raises(BrokerCapabilityError) as dom_exc:
        await adapter.stream_market_depth({"symbol": "AAPL"})

    assert option_exc.value.code == "options_trading_not_supported"
    assert dom_exc.value.code == "dom_not_supported"
    assert client.calls == []


@pytest.mark.asyncio
async def test_ibkr_paper_adapter_passes_contract_objects_to_ib_client() -> None:
    client = FakeIBClient()
    adapter = IBKRPaperAdapter({}, client=client, supervisor=FakeSupervisor())
    payload = {"symbol": "MNQ", "secType": "FUT", "exchange": "CME", "currency": "USD"}

    await adapter.request_market_snapshot(payload)
    await adapter.get_historical_data(payload, duration="1 D", bar_size="5 mins")
    await adapter.stream_real_time_price(payload)
    await adapter.stream_historical_bars(payload, duration="1 D", bar_size="5 mins")

    delegated = [
        call
        for call in client.calls
        if call[0]
        in {
            "request_market_snapshot",
            "get_historical_data",
            "stream_real_time_price",
            "stream_historical_bars",
        }
    ]
    assert delegated
    for _, contract in delegated:
        assert not isinstance(contract, dict)
        assert getattr(contract, "symbol", None) == "MNQ"
        assert getattr(contract, "secType", None) == "FUT"
        assert hasattr(contract, "includeExpired")


@pytest.mark.asyncio
async def test_ibkr_paper_adapter_keeps_built_contract_when_qualification_returns_none() -> None:
    class EmptyQualificationClient(FakeIBClient):
        async def qualify_contract(self, contract):
            self.calls.append(("qualify_contract", contract))
            return None

    client = EmptyQualificationClient()
    adapter = IBKRPaperAdapter({}, client=client, supervisor=FakeSupervisor())

    await adapter.stream_historical_bars(
        {
            "symbol": "MNQ",
            "secType": "FUT",
            "exchange": "CME",
            "currency": "USD",
            "conId": 900476,
            "localSymbol": "MNQU6",
            "lastTradeDateOrContractMonth": "20260918",
        },
        duration="1 D",
        bar_size="5 mins",
    )

    delegated = [call for call in client.calls if call[0] == "stream_historical_bars"]
    assert len(delegated) == 1
    contract = delegated[0][1]
    assert contract is not None
    assert getattr(contract, "symbol", None) == "MNQ"
    assert getattr(contract, "secType", None) == "FUT"
    assert not getattr(contract, "conId", None)
    assert getattr(contract, "localSymbol", None) == "MNQU6"


@pytest.mark.asyncio
async def test_ibkr_paper_adapter_normalizes_iso_datetimes_for_ib_client() -> None:
    client = FakeIBClient()
    adapter = IBKRPaperAdapter({}, client=client, supervisor=FakeSupervisor())
    payload = {"symbol": "MNQ", "secType": "FUT", "exchange": "CME", "currency": "USD"}

    await adapter.get_historical_data(
        payload,
        end_datetime="2026-07-01T03:01:37.131Z",
        duration="1 D",
    )
    await adapter.stream_historical_bars(
        payload,
        end_datetime="2026-07-01T03:01:37.131+00:00",
        duration="1 D",
    )
    await adapter.get_historical_data(
        payload,
        end_datetime="20260701-03:01:37",
        duration="1 D",
    )

    assert client.historical_kwargs[0]["end_datetime"] == "20260701-03:01:37"
    assert client.historical_kwargs[1]["end_datetime"] == "20260701-03:01:37"
    assert client.historical_kwargs[2]["end_datetime"] == "20260701-03:01:37"


@pytest.mark.asyncio
async def test_ibkr_paper_adapter_reconnects_once_for_transient_stream_disconnect() -> None:
    class FlakyStreamClient(FakeIBClient):
        def __init__(self) -> None:
            super().__init__()
            self.failures_left = 1

        async def stream_historical_bars(self, contract, **kwargs):
            self.calls.append(("stream_historical_bars", contract))
            self.historical_kwargs.append(dict(kwargs))
            if self.failures_left:
                self.failures_left -= 1
                raise TimeoutError("disconnected client timed out")
            return _FakeStream()

    client = FlakyStreamClient()
    adapter = IBKRPaperAdapter({}, client=client, supervisor=FakeSupervisor())

    stream = await adapter.stream_historical_bars(
        {"symbol": "MNQ", "secType": "FUT", "exchange": "CME", "currency": "USD"},
        end_datetime="2026-07-01T03:01:37Z",
        duration="1 D",
    )

    assert isinstance(stream, _FakeStream)
    assert ("reconnect", "stream_historical_bars_ib_disconnect") in client.calls
    assert [call[0] for call in client.calls].count("stream_historical_bars") == 2
    assert client.historical_kwargs[0]["end_datetime"] == "20260701-03:01:37"


@pytest.mark.asyncio
async def test_ibkr_paper_adapter_raises_connection_error_after_reconnect_retry_fails() -> None:
    class DownClient(FakeIBClient):
        async def get_historical_data(self, contract, **kwargs):
            self.calls.append(("get_historical_data", contract))
            self.historical_kwargs.append(dict(kwargs))
            raise ConnectionError("not connected")

    client = DownClient()
    adapter = IBKRPaperAdapter({}, client=client, supervisor=FakeSupervisor())

    with pytest.raises(BrokerConnectionError) as exc_info:
        await adapter.get_historical_data(
            {"symbol": "MNQ", "secType": "FUT", "exchange": "CME", "currency": "USD"},
            end_datetime="2026-07-01T03:01:37Z",
            duration="1 D",
        )

    assert exc_info.value.details["operation"] == "get_historical_data"
    assert ("reconnect", "get_historical_data_ib_disconnect") in client.calls
    assert [call[0] for call in client.calls].count("get_historical_data") == 2


@pytest.mark.asyncio
async def test_ibkr_paper_adapter_times_out_stalled_contract_qualification() -> None:
    class StalledQualificationClient(FakeIBClient):
        async def qualify_contract(self, contract):
            self.calls.append(("qualify_contract", contract))
            await asyncio.sleep(10)
            return contract

    client = StalledQualificationClient()
    adapter = IBKRPaperAdapter(
        {"ib_qualification_timeout": 0.01},
        client=client,
        supervisor=FakeSupervisor(),
    )

    with pytest.raises(BrokerConnectionError) as exc_info:
        await adapter.qualify_contract(
            {"symbol": "MNQ", "secType": "FUT", "exchange": "CME", "currency": "USD"}
        )
    assert exc_info.value.code == "broker_connection_error"
    assert exc_info.value.details["operation"] == "qualify_contract"
    assert ("reconnect", "contract_qualification_timeout") in client.calls


@pytest.mark.asyncio
async def test_ibkr_paper_adapter_preserves_connection_error_when_reconnect_is_cancelled() -> None:
    class CancelledReconnectClient(FakeIBClient):
        async def qualify_contract(self, contract):
            self.calls.append(("qualify_contract", contract))
            await asyncio.sleep(10)
            return contract

        async def reconnect(self, *, reason: str | None = None) -> None:
            self.calls.append(("reconnect", reason))
            raise asyncio.CancelledError()

    client = CancelledReconnectClient()
    adapter = IBKRPaperAdapter(
        {"ib_qualification_timeout": 0.01},
        client=client,
        supervisor=FakeSupervisor(),
    )

    with pytest.raises(BrokerConnectionError) as exc_info:
        await adapter.qualify_contract(
            {"symbol": "MNQ", "secType": "FUT", "exchange": "CME", "currency": "USD"}
        )
    assert exc_info.value.code == "broker_connection_error"
    assert exc_info.value.details["operation"] == "qualify_contract"
    assert ("reconnect", "contract_qualification_timeout") in client.calls
