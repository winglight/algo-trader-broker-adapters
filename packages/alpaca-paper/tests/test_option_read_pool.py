import httpx
import pytest
from algo_trader_broker_sdk import BrokerConnectionError
from algo_trader_broker_adapter_alpaca_paper.clients import AlpacaClients
from algo_trader_broker_adapter_alpaca_paper.settings import AlpacaPaperSettings


@pytest.mark.asyncio
async def test_read_connections_are_reused_but_every_native_response_is_captured_anew():
    reads = []
    def native(request):
        reads.append((request.url.host, request.url.path))
        return httpx.Response(200, content=str(len(reads)).encode())
    client = AlpacaClients(AlpacaPaperSettings.from_mapping(dict(alpaca_api_key_id='fixture', alpaca_secret_key='fixture')),
        option_http_transport=httpx.MockTransport(native))
    try:
        assert await client.get_option_resource('/v2/account') == b'1'
        trading = client._option_read_clients[False]
        assert await client.get_option_resource('/v2/positions') == b'2'
        assert client._option_read_clients[False] is trading and not trading.is_closed
        assert await client.get_option_resource('/v1beta1/options/snapshots', data=True) == b'3'
        assert len(client._option_read_clients) == 2
        assert reads == [('paper-api.alpaca.markets', '/v2/account'),
                         ('paper-api.alpaca.markets', '/v2/positions'),
                         ('data.alpaca.markets', '/v1beta1/options/snapshots')]
    finally:
        pools = list(client._option_read_clients.values())
        await client.close()
    assert all(pool.is_closed for pool in pools) and not client._option_read_clients


@pytest.mark.asyncio
async def test_failed_read_keeps_only_typed_status_and_never_retries_or_exposes_native_body():
    calls=[]
    def native(request):
        calls.append(request)
        return httpx.Response(429, content=b'private-native-body')
    client=AlpacaClients(AlpacaPaperSettings.from_mapping(dict(alpaca_api_key_id='fixture-key',alpaca_secret_key='fixture-secret')),
        option_http_transport=httpx.MockTransport(native))
    try:
        with pytest.raises(BrokerConnectionError) as caught:
            await client.get_option_resource('/v2/account')
        assert caught.value.details == {'operation':'option_trading_read','resource_kind':'ACCOUNT','error_type':'HTTPStatusError','status_code':429}
        assert len(calls)==1
        assert all(secret not in str(caught.value.to_payload()) for secret in ['private-native-body','fixture-key','fixture-secret'])
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('path,kind', [
    ('/v2/account', 'ACCOUNT'),
    ('/v2/account/activities', 'ACCOUNT_ACTIVITIES'),
    ('/v2/positions', 'POSITIONS'),
    ('/v2/orders', 'ORDERS'),
    ('/v2/options/contracts', 'OPTION_CONTRACTS'),
    ('/v2/orders/private-order-id', 'ORDER_DETAIL'),
    ('/v2/options/contracts/private-option-symbol', 'OPTION_CONTRACT_DETAIL'),
    ('/unrecognized/private-account-id', 'OTHER'),
])
async def test_failed_native_read_classifies_resource_without_retaining_private_input(path, kind):
    calls = []
    def native(request):
        calls.append(request)
        return httpx.Response(404, content=b'private-response-body')
    client = AlpacaClients(AlpacaPaperSettings.from_mapping(dict(
        alpaca_api_key_id='private-key', alpaca_secret_key='private-secret')),
        option_http_transport=httpx.MockTransport(native))
    try:
        with pytest.raises(BrokerConnectionError) as caught:
            await client.get_option_resource(path, params={'page_token': 'private-cursor'})
        assert caught.value.details == {'operation': 'option_trading_read', 'resource_kind': kind,
            'error_type': 'HTTPStatusError', 'status_code': 404}
        assert len(calls) == 1
        assert 'private-' not in str(caught.value.to_payload())
        assert '/v2/' not in str(caught.value.to_payload())
    finally:
        await client.close()
