"""Exact native callback fee observations; no gateway, account or order is contacted."""
from copy import deepcopy
from dataclasses import asdict, make_dataclass, replace
from datetime import datetime, timedelta, timezone
from itertools import count
import json

import pytest
import pytest_asyncio
from ib_async import IB, ContractDetails, Option, OrderState

from algo_trader_broker_adapter_ibkr_paper import IBKRPaperAdapter
from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient
from algo_trader_broker_adapter_ibkr_paper.settings import IBGatewaySettings
from algo_trader_broker_adapter_ibkr_paper.options_reads import contract_from_details
from algo_trader_broker_sdk import BrokerContractError, dataclass_to_payload
from algo_trader_broker_sdk.options import (
    OptionExecutionRequest, OptionLegIntent, OptionScope, OptionVerifiedAccount,
    OrderPreview, option_from_payload, option_request_hash,
)


@pytest_asyncio.fixture
async def native_preview(monkeypatch):
    ib = IB()
    ib.client.connState = ib.client.CONNECTED
    ib.client._serverVersion = 178
    ib.client.clientId = ib.wrapper.clientId = 40
    monkeypatch.setattr(ib, 'isConnected', lambda: True)
    monkeypatch.setattr(ib, 'managedAccounts', lambda: ['DU-OPTIONS-FIXTURE'])
    ids = count(1000)
    monkeypatch.setattr(ib.client, 'getReqId', lambda: next(ids))
    client = IBAsyncClient(IBGatewaySettings())
    client._ib, client._connected_since = ib, datetime.now(timezone.utc)
    client._connected.set()
    adapter = IBKRPaperAdapter({}, client=client)
    scope = OptionScope('options/1.0', 'a' * 64, 'fixture-profile', 1, 'fixture-account', 'paper')
    bound = OptionVerifiedAccount(scope, 'IBKR', 'DU-OPTIONS-FIXTURE')
    adapter.bind_option_account(bound)
    expiry = (datetime.now(timezone.utc) + timedelta(days=30)).strftime('%Y%m%d')
    option = Option('SPY', expiry, 590, 'C', 'SMART', multiplier='100', currency='USD',
                    tradingClass='SPY', conId=1590, localSymbol=f'SPY   {expiry[2:]}C00590000')
    detail = ContractDetails(contract=option, underConId=100, underSymbol='SPY', underSecType='STK',
                             validExchanges='SMART', minTick=0.01, marketRuleIds='26')
    contract = contract_from_details(detail, 'SPY', 100)
    binding = adapter._remember_contract(bound, detail, contract)
    request = OptionExecutionRequest(**asdict(scope), command_id='native-cost-fixture',
        client_order_id='ato-native-cost-fixture', plan_hash='b' * 64, authorization_ref='fixture-read',
        quote_snapshot_ref='c' * 64, valid_until=(datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat().replace('+00:00', 'Z'),
        capability_revision='fixture-ib', groups=2, order_type='LMT', signed_limit='1.25',
        tif='DAY', max_slippage='0', legs=(OptionLegIntent('L1', binding, 'BUY', 'OPEN', 1),))
    sockets, sent = [], []
    monkeypatch.setattr(ib.client.conn, 'sendMsg', sockets.append)
    # Reuse the production account callback observer, with native decoded fixture fields.
    def account(req_id, account, model, ledger):
        ib.wrapper.accountUpdateMulti(req_id, account, '', 'Currency', 'USD', 'BASE')
        ib.wrapper.accountUpdateMultiEnd(req_id)
    monkeypatch.setattr(ib.client, 'reqAccountUpdatesMulti', account)
    monkeypatch.setattr(ib.client, 'cancelAccountUpdatesMulti', lambda *_: None)
    native_place = ib.client.placeOrder
    state = OrderState(status='PreSubmitted', initMarginChange='250', commission=1.3,
                       minCommission=1.0, maxCommission=1.6, commissionCurrency='USD')
    controls = dict(state=state, mutate=None, error=None)
    def place(order_id, native_contract, order):
        assert order.whatIf is True
        sent.append(deepcopy(order))
        native_place(order_id, native_contract, order)
        if controls['error']:
            ib.errorEvent.emit(order_id, controls['error'], 'synthetic native rejection')
            return
        returned_contract, returned_order = deepcopy(native_contract), deepcopy(order)
        if controls['mutate']:
            controls['mutate'](returned_contract, returned_order)
        ib.wrapper.openOrder(order_id, returned_contract, returned_order, deepcopy(controls['state']))
    monkeypatch.setattr(ib.client, 'placeOrder', place)
    try:
        yield adapter, request, controls, client, sockets, sent
    finally:
        ib.client.connState = ib.client.DISCONNECTED
        monkeypatch.setattr(ib, 'isConnected', lambda: False)
        client._connected.clear()
        client._ib = None
        await client._shutdown_sync_executor()


@pytest.mark.asyncio
async def test_native_open_and_reverse_close_keep_exact_request_raw_fees(native_preview):
    adapter, request, controls, _, sockets, sent = native_preview
    first = await adapter.preview_option_order(request)
    close = replace(request, command_id='close-fixture', client_order_id='ato-close-fixture',
                    signed_limit='-1.25', legs=(replace(request.legs[0], side='SELL', position_effect='CLOSE'),))
    second = await adapter.preview_option_order(close)
    for preview, exact in ((first, request), (second, close)):
        cost = preview.native_costs
        cost.require_request(exact)
        assert cost.request_hash == option_request_hash(exact)
        assert (cost.min_cash, cost.estimate_cash, cost.max_cash) == ('1', '1.3', '1.6')
        assert cost.field_basis == 'IB_COMMISSION_FIELDS'
        assert cost.coverage == 'EXACT_WHAT_IF_ORDER_ONLY'
        assert cost.api_version.endswith(';tws_server/178')
        raw = json.loads(cost.raw_evidence_json)
        assert raw['native_fields']['maxCommission'] == '1.6'
        assert 'DU-OPTIONS-FIXTURE' not in cost.raw_evidence_json
        assert option_from_payload(OrderPreview, dataclass_to_payload(preview)) == preview
    assert first.native_costs.request_hash != second.native_costs.request_hash
    assert [order.openClose for order in sent] == ['O', 'C']
    assert all(order.whatIf for order in sent) and len(sockets) == 2
    with pytest.raises(BrokerContractError, match='another exact request'):
        first.native_costs.require_request(close)


@pytest.mark.asyncio
@pytest.mark.parametrize('field,value', [
    ('maxCommission', 1.7976931348623157e308), ('maxCommission', float('nan')),
    ('maxCommission', float('inf')), ('maxCommission', -1.0), ('maxCommission', 0.5),
    ('commissionCurrency', 'EUR'),
])
async def test_unusable_native_max_never_becomes_zero(native_preview, field, value):
    adapter, request, controls, *_ = native_preview
    setattr(controls['state'], field, value)
    result = await adapter.preview_option_order(request)
    assert result.native_costs.max_cash is None
    assert 'IB_USD_MAX_COMMISSION_UNAVAILABLE' in result.warnings
    assert json.loads(result.native_costs.raw_evidence_json)['native_fields'][field] == str(value)


@pytest.mark.asyncio
@pytest.mark.parametrize('mutation', ['quantity', 'contract', 'account', 'intent', 'connection'])
async def test_changed_native_response_or_context_is_rejected(native_preview, mutation):
    adapter, request, controls, client, *_ = native_preview
    def mutate(contract, order):
        if mutation == 'quantity':
            order.totalQuantity += 1
        elif mutation == 'contract':
            contract.conId += 1
        elif mutation == 'account':
            order.account = 'DU-DIFFERENT-FIXTURE'
        elif mutation == 'intent':
            order.openClose = 'C'
        else:
            client._connected_since += timedelta(seconds=1)
    controls['mutate'] = mutate
    with pytest.raises(BrokerContractError):
        await adapter.preview_option_order(request)


@pytest.mark.asyncio
async def test_reverse_close_native_rejection_is_not_turned_into_evidence(native_preview):
    adapter, request, controls, *_ = native_preview
    close = replace(request, signed_limit='-1.25',
                    legs=(replace(request.legs[0], side='SELL', position_effect='CLOSE'),))
    controls['error'] = 201
    with pytest.raises(BrokerContractError, match='one unambiguous'):
        await adapter.preview_option_order(close)


@pytest.mark.asyncio
async def test_expired_request_never_issues_native_whatif(native_preview):
    adapter, request, _, _, sockets, _ = native_preview
    request = replace(request, valid_until='2000-01-01T00:00:00Z')
    with pytest.raises(BrokerContractError, match='expired'):
        await adapter.preview_option_order(request)
    assert sockets == []


@pytest.mark.asyncio
@pytest.mark.parametrize('both,maximum', [(False, '1.60'), (True, '1.60'), (True, '1.59')])
async def test_native_renamed_fields_and_conflicting_aliases(native_preview, both, maximum):
    adapter, request, controls, *_ = native_preview
    # The official API 10.33 renamed OrderState fields; ib_async 2.x still uses
    # the legacy dataclass. Test both decoded field shapes without changing I/O.
    values = dict(status='PreSubmitted', initMarginChange='250', warningText='',
                  commissionAndFees='1.3', minCommissionAndFees='1',
                  maxCommissionAndFees=maximum, commissionAndFeesCurrency='USD')
    if both:
        values.update(commission='1.3', minCommission='1', maxCommission='1.6',
                      commissionCurrency='USD')
    callback_type = make_dataclass('RenamedOrderStateFixture', [(name, str) for name in values])
    controls['state'] = callback_type(**values)
    preview = await adapter.preview_option_order(request)
    assert preview.native_costs.max_cash == ('1.6' if maximum == '1.60' else None)
    assert preview.native_costs.field_basis == (
        'IB_BOTH_COMMISSION_FIELD_SETS' if both else 'IB_COMMISSION_AND_FEES_FIELDS')


@pytest.mark.asyncio
@pytest.mark.parametrize('status', ['Inactive', 'Cancelled', 'ApiCancelled', 'Rejected'])
async def test_rejected_native_order_state_cannot_supply_cost_evidence(native_preview, status):
    adapter, request, controls, *_ = native_preview
    controls['state'].status = status
    with pytest.raises(BrokerContractError, match='rejected the exact order'):
        await adapter.preview_option_order(request)


@pytest.mark.asyncio
async def test_native_reject_reason_cannot_supply_cost_evidence(native_preview):
    adapter, request, controls, *_ = native_preview
    values = {**asdict(controls['state']), 'rejectReason': 'synthetic broker rejection'}
    callback_type = make_dataclass('RejectedOrderStateFixture', [(name, type(value)) for name, value in values.items()])
    controls['state'] = callback_type(**values)
    with pytest.raises(BrokerContractError, match='rejected the exact order'):
        await adapter.preview_option_order(request)
