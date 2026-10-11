"""Configured environment selects actual native transports, without network access."""
import asyncio
from dataclasses import replace

import httpx
import pytest

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_adapter_alpaca_paper import AlpacaPaperAdapter
from algo_trader_broker_adapter_alpaca_paper.clients import AlpacaClients
from algo_trader_broker_adapter_alpaca_paper.raw_stream import create_trading_stream
from algo_trader_broker_adapter_alpaca_paper.settings import AlpacaPaperSettings
from .test_option_raw_events import VERIFIED


@pytest.mark.parametrize('environment,host', [('paper', 'paper-api.alpaca.markets'), ('live', 'api.alpaca.markets')])
def test_native_clients_streams_and_binding_use_configured_environment(environment, host):
    settings = dict(alpaca_api_key_id=environment + '-key', alpaca_secret_key=environment + '-secret', alpaca_environment=environment)
    parsed = AlpacaPaperSettings.from_mapping(settings)
    requests = []

    def native(request):
        requests.append(request)
        assert request.headers['APCA-API-KEY-ID'] == environment + '-key'
        assert request.headers['APCA-API-SECRET-KEY'] == environment + '-secret'
        return httpx.Response(200, json={})

    client = AlpacaClients(parsed, option_http_transport=httpx.MockTransport(native))
    client._load()
    assert client._trading._base_url.value == 'https://' + host
    stream = create_trading_stream(parsed, lambda: None)
    assert stream._endpoint == 'wss://' + host + '/stream'

    async def reads():
        await client.get_option_resource('/v2/account')
        await client.get_option_resource('/v1beta1/options/snapshots', data=True)
    asyncio.run(reads())
    assert [request.url.host for request in requests] == [host, 'data.alpaca.markets']

    adapter = AlpacaPaperAdapter(settings, backend=client)
    adapter._connected, adapter._account_id = True, VERIFIED.native_account_ref
    binding = replace(VERIFIED, scope=replace(VERIFIED.scope, environment=environment))
    adapter.bind_option_account(binding)
    assert adapter.manifest().environment == environment.upper()
    assert adapter.capabilities().environment == environment.upper()
    assert adapter.capabilities().native['paperOnly'] == (environment == 'paper')
    opposite = replace(binding, scope=replace(binding.scope, environment='paper' if environment == 'live' else 'live'))
    with pytest.raises(BrokerContractError):
        adapter.bind_option_account(opposite)
    assert adapter._option_account_binding == binding
