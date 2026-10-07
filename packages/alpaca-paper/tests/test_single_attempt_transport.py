"""Exercise the pinned real SDK with an offline HTTP transport."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from requests import Response
from requests.exceptions import ConnectionError as RequestsConnectionError

from algo_trader_broker_adapter_alpaca_paper.clients import AlpacaClients
from algo_trader_broker_adapter_alpaca_paper.settings import AlpacaPaperSettings
from algo_trader_broker_sdk import BrokerOrderError, StockOrderRequest, SubmissionGate


def response(status, body=b'{"code":42910000,"message":"synthetic rejection"}'):
    result = Response()
    result.status_code = status
    result._content = body
    return result


def backend():
    result = AlpacaClients(AlpacaPaperSettings("synthetic-key", "synthetic-secret"))
    result._load()
    return result


@pytest.mark.parametrize("failure,code", [
    (response(429), "broker_order_error"),
    (response(422), "broker_order_error"),
    (response(408), "broker_order_outcome_unknown"),
    (response(503), "broker_order_outcome_unknown"),
    (response(200, b"invalid-json"), "broker_order_outcome_unknown"),
    (RequestsConnectionError("response lost"), "broker_order_outcome_unknown"),
])
def test_actual_sdk_post_never_retries_and_preserves_uncertain_outcome(monkeypatch, failure, code):
    async def scenario():
        clients = backend()
        gate = SubmissionGate(AsyncMock(), Mock())
        def request(method, url, **kwargs):
            assert gate.consumed
            assert method == "POST" and url == "https://paper-api.alpaca.markets/v2/orders"
            assert kwargs["timeout"] == clients.settings.request_timeout_seconds
            assert kwargs["allow_redirects"] is False
            assert kwargs["json"]["client_order_id"] == "synthetic-order"
            if isinstance(failure, Exception): raise failure
            return failure
        native = Mock(side_effect=request)
        clients._trading._session.request = native
        sleep = Mock(side_effect=AssertionError("a write must not schedule a retry"))
        monkeypatch.setattr("alpaca.common.rest.time.sleep", sleep)
        order = clients.make_order_request(StockOrderRequest(symbol="SPY", side="BUY", quantity=1,
            order_type="LMT", limit_price=20, client_order_id="synthetic-order"))
        with pytest.raises(BrokerOrderError) as error:
            await clients.submit_order(order, submission_gate=gate)
        assert error.value.code == code
        assert native.call_count == 1
        sleep.assert_not_called()
    asyncio.run(scenario())


@pytest.mark.parametrize("method", ["POST", "PATCH", "DELETE"])
def test_all_native_writes_disable_rate_limit_retry_without_changing_shared_config(monkeypatch, method):
    from alpaca.common.exceptions import APIError
    client = backend()._trading
    retry = client._retry
    client._session.request = Mock(return_value=response(429))
    sleep = Mock(side_effect=AssertionError("write retry"))
    monkeypatch.setattr("alpaca.common.rest.time.sleep", sleep)
    with pytest.raises(APIError): client._request(method, "/orders/synthetic")
    assert client._session.request.call_count == 1
    assert client._retry == retry
    sleep.assert_not_called()


def test_read_retry_is_preserved(monkeypatch):
    client = backend()._trading
    client._session.request = Mock(side_effect=[response(429), response(200, b'{"ok":true}')])
    sleep = Mock()
    monkeypatch.setattr("alpaca.common.rest.time.sleep", sleep)
    assert client.get("/synthetic") == {"ok": True}
    assert client._session.request.call_count == 2
    sleep.assert_called_once()
