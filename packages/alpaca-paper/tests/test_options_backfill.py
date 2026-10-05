"""Synthetic native pages and mocked HTTP transport; no broker connections."""

from dataclasses import replace
from decimal import Decimal
import json

import httpx
import pytest

from algo_trader_broker_sdk import BrokerConnectionError, BrokerContractError, dataclass_to_payload
from algo_trader_broker_sdk.options_backfill import OptionActivityScanPage, OptionRawActivityPage
from algo_trader_broker_sdk.options_events import OptionNativeOrderQuery
from algo_trader_broker_adapter_alpaca_paper.clients import AlpacaClients
from algo_trader_broker_adapter_alpaca_paper.options_backfill import SOURCE, index_activity_page
from algo_trader_broker_adapter_alpaca_paper.raw_stream import decode_native
from algo_trader_broker_adapter_alpaca_paper.settings import AlpacaPaperSettings
from .test_option_raw_events import Backend, SCOPE, VERIFIED
from .test_adapter import SETTINGS, connected


def query(cursor=None):
    return OptionActivityScanPage(**dataclass_to_payload(SCOPE), range_start="2026-10-01T00:00:00Z",
        range_end="2026-10-05T00:00:00Z", cursor=cursor)


def test_original_byte_spans_keep_numeric_spelling_unicode_full_ids_types_and_unknown_activities():
    raw = (' \n[ {"id":"time::same-uuid","activity_type":"FILL","price":1.123456789012,"note":"中😀\\\""},\n'
           '{"id":"time::same-uuid","activity_type":"OPASN","qty":"2"},'
           '{"id":"opaque-final","activity_type":"NEW_UNKNOWN","data":{"x":[1,2]}} ] \t').encode()
    index = index_activity_page(OptionRawActivityPage(query(), SOURCE, raw))
    assert index.next_cursor == "opaque-final"
    assert len(index.items) == 3
    native = [decode_native(raw[item.start:item.end]) for item in index.items]
    assert native[0]["price"] == Decimal("1.123456789012")
    assert [item.activity_type for item in index.items] == ["FILL", "OPASN", "NEW_UNKNOWN"]
    assert [item.activity_id for item in index.items][:2] == ["time::same-uuid"] * 2
    assert raw[index.items[0].start:index.items[0].end].startswith(b'{"id"')
    assert index_activity_page(OptionRawActivityPage(query(), SOURCE, b" \n[]\t")).next_cursor is None


@pytest.mark.parametrize("raw", [b"", b"{}", b"[", b"[{},]", b'[null]', b'[{}]', b'[] junk',
    b'[{"id":"a","id":"b","activity_type":"FILL"}]', b'[{"id":"a","activity_type":"FILL","qty":NaN}]',
    b'[{"id":"a","activity_type":"FILL"},]', b'[{"id":"a","activity_type":"FILL"} {"id":"b"}]',
    b'[{"id":"a","activity_type":"FILL"},{"id":"a","activity_type":"FILL"}]',
    json.dumps([dict(id=str(i), activity_type="FILL") for i in range(101)]).encode(),
    b'[{"id":123,"activity_type":"FILL"}]', b'[{"id":"a","activity_type":"FILL"}]\xff'])
def test_malformed_missing_duplicate_and_oversized_pages_are_not_silently_truncated(raw):
    with pytest.raises(BrokerContractError): index_activity_page(OptionRawActivityPage(query(), SOURCE, raw))


def test_short_nonempty_page_requires_next_request_and_cursor_echo_is_rejected():
    raw = b'[{"id":"full::native","activity_type":"FILL"}]'
    assert index_activity_page(OptionRawActivityPage(query(), SOURCE, raw)).next_cursor == "full::native"
    with pytest.raises(BrokerContractError): index_activity_page(OptionRawActivityPage(query("full::native"), SOURCE, raw))


@pytest.mark.asyncio
async def test_real_http_client_uses_same_paper_credentials_bounded_unfiltered_query_and_original_bytes(monkeypatch):
    requests = []
    raw = b'[{"id":"full::native","activity_type":"FILL","price":1.123456789012}]'
    def handler(request):
        requests.append(request)
        return httpx.Response(200, content=raw)
    original = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: original(**kwargs, transport=httpx.MockTransport(handler)))
    client = AlpacaClients(AlpacaPaperSettings.from_mapping(SETTINGS))
    assert await client.get_option_activity_page(query("prior::full")) == raw
    assert len(requests) == 1
    request = requests[0]
    assert str(request.url).startswith("https://paper-api.alpaca.markets/v2/account/activities?")
    assert dict(request.url.params) == {"after": query().range_start, "until": query().range_end,
        "direction": "asc", "page_size": "100", "page_token": "prior::full"}
    assert request.headers["APCA-API-KEY-ID"] == client.settings.api_key_id
    assert request.headers["APCA-API-SECRET-KEY"] == client.settings.secret_key


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["redirect", "rate", "empty", "size", "timeout"])
async def test_transport_errors_redirects_and_resource_limits_do_not_retry_or_expose_credentials(monkeypatch, failure):
    requests = []
    def handler(request):
        requests.append(request)
        if failure == "timeout": raise httpx.ReadTimeout("fixture secret should not leak", request=request)
        return httpx.Response(302 if failure == "redirect" else 429 if failure == "rate" else 200,
            headers={"location": "https://untrusted.invalid"}, content=b"" if failure == "empty" else b"x" * (8388609 if failure == "size" else 1))
    original = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: original(**kwargs, transport=httpx.MockTransport(handler)))
    client = AlpacaClients(AlpacaPaperSettings.from_mapping(SETTINGS))
    with pytest.raises(BrokerConnectionError) as error: await client.get_option_activity_page(query())
    assert "secret should not leak" not in str(error.value) and len(requests) == 1


@pytest.mark.asyncio
async def test_adapter_rejects_wrong_scope_but_keeps_captured_scope_if_connection_changes_during_read():
    adapter, backend = await connected(Backend())
    calls = []
    async def retain(event): pass
    async def read(request):
        calls.append(request)
        adapter._clear_option_event_handler()
        return b"[]"
    backend.get_option_activity_page = read
    with pytest.raises(BrokerContractError): await adapter.read_option_activity_page(query())
    adapter.set_option_event_handler(VERIFIED, retain)
    for wrong in (replace(query(), expected_generation=2), replace(query(), account="another"), replace(query(), environment="live")):
        with pytest.raises(BrokerContractError): await adapter.read_option_activity_page(wrong)
    assert not calls
    page = await adapter.read_option_activity_page(query())
    assert page.request == query() and page.raw_payload == b"[]" and adapter.index_option_activity_page(page).items == ()
    assert adapter._option_event_binding is None


@pytest.mark.asyncio
async def test_native_order_http_uses_exact_uuid_and_retains_original_response_before_parsing(monkeypatch):
    requests = []
    raw = b'{"id":"00000000-0000-4000-8000-000000000001","filled_avg_price":1.123456789012}'
    def handler(request):
        requests.append(request)
        return httpx.Response(200, content=raw)
    original = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: original(**kwargs, transport=httpx.MockTransport(handler)))
    client = AlpacaClients(AlpacaPaperSettings.from_mapping(SETTINGS))
    request = OptionNativeOrderQuery(**dataclass_to_payload(SCOPE), order_id="00000000-0000-4000-8000-000000000001")
    assert await client.get_option_order_evidence(request) == raw
    assert str(requests[0].url) == "https://paper-api.alpaca.markets/v2/orders/00000000-0000-4000-8000-000000000001?nested=true"
    with pytest.raises(BrokerContractError): await client.get_option_order_evidence(replace(request, order_id="../account?secret"))
    assert len(requests) == 1
