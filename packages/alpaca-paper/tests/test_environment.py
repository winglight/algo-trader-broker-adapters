"""Configured environment selects actual native transports, without network access."""
import asyncio
from dataclasses import asdict, replace
import json

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


@pytest.mark.asyncio
@pytest.mark.parametrize('environment', ['paper', 'live'])
async def test_original_option_mapping_and_receipt_recovery_keep_exact_environment(environment):
    """Same original two-leg inputs, no network or real-account certification."""
    from algo_trader_broker_sdk.options import OptionExecutionRequest, OptionLegIntent
    from algo_trader_broker_sdk.options_events import OptionRawEvent
    from algo_trader_broker_sdk.options_backfill import OptionRawActivityPage
    from algo_trader_broker_adapter_alpaca_paper.options_orders import order_payload, order_state, decode_order_evidence
    from algo_trader_broker_adapter_alpaca_paper.options_codec import decode_trade_update, decode_fill_activity
    from algo_trader_broker_adapter_alpaca_paper.options_backfill import SOURCE, index_activity_page
    from .test_options_codec import fixture, IDS, TIME
    from .test_options_backfill import query
    from .test_options_activity_codec import EXEC

    data, original, _ = fixture()
    scope = replace(VERIFIED.scope, environment=environment)
    qualified = {key: replace(value, binding=replace(value.binding, environment=environment))
                 for key, value in original.items()}

    async def resolve(asset_id, symbol):
        return qualified[(asset_id, symbol)]

    request = OptionExecutionRequest(**asdict(scope), command_id='environment-option',
        client_order_id=data['order']['client_order_id'], plan_hash='b' * 64,
        authorization_ref='synthetic-only', quote_snapshot_ref='c' * 64,
        valid_until='2026-10-05T15:00:00Z', capability_revision='synthetic-only', groups=2,
        order_type='LMT', signed_limit='1.3', tif='DAY', max_slippage='0',
        legs=tuple(OptionLegIntent('L'+str(index), value.binding, 'BUY' if index == 0 else 'SELL', 'OPEN', 1)
                   for index, value in enumerate(qualified.values())))
    payload = order_payload(request)
    assert payload['qty'] == '2' and payload['limit_price'] == '1.3' and payload['order_class'] == 'mleg'
    assert [leg['position_intent'] for leg in payload['legs']] == ['buy_to_open', 'sell_to_open']
    assert [leg['ratio_qty'] for leg in payload['legs']] == ['1', '1']
    opposite = 'paper' if environment == 'live' else 'live'
    with pytest.raises(BrokerContractError, match='Binding does not match execution account/environment'):
        replace(request, environment=opposite)

    trade = await decode_trade_update(OptionRawEvent(scope, 'ALPACA_TRADE_UPDATES',
        json.dumps(dict(stream='trade_updates', data=data)).encode()), resolve)
    assert trade.scope == scope and [item.contracts for item in trade.executions] == [2, 2]
    assert [item.execution_id for item in trade.executions] == ['full::exec-0', 'full::exec-1']

    order = data['order']
    order.update(status='accepted', qty='2', updated_at=TIME, created_at=TIME)
    for leg, intent in zip(order['legs'], request.legs):
        leg.update(qty='2', filled_qty='0', position_intent=intent.side.lower()+'_to_open')
    raw = json.dumps(order).encode()
    state = order_state(request, raw)
    assert state.command_id == request.command_id and state.remaining_groups == 2 and state.filled_groups == 0
    proof = OptionRawEvent(scope, 'ALPACA_ORDER_DETAIL', raw, IDS[0])
    observed = await decode_order_evidence(proof, resolve)
    assert observed.scope == scope and len(observed.legs) == 2 and observed.executions == ()
    activity = dict(activity_type='FILL', type='partial_fill', id='native-date-prefix::'+EXEC,
        order_id=IDS[0], symbol=order['legs'][0]['symbol'], side='buy', qty='2',
        price='2.123456789012', transaction_time=TIME)
    async def resolve_order(order_id):
        assert order_id == IDS[0]
        return proof
    recovered = await decode_fill_activity(OptionRawEvent(scope, 'ALPACA_ACTIVITY_FILL',
        json.dumps(activity).encode()), resolve, resolve_order)
    assert recovered.scope == scope and recovered.executions[0].contracts == 2
    assert recovered.executions[0].execution_id == activity['id']
    page = OptionRawActivityPage(replace(query(), environment=environment), SOURCE,
        json.dumps([activity]).encode())
    indexed = index_activity_page(page)
    assert indexed.next_cursor == activity['id'] and indexed.items[0].activity_type == 'FILL'
