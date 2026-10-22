import httpx
import pytest
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
