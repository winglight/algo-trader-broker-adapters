"""Environment fences using the original single-call order shape, without a gateway."""
import asyncio
import json
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone

import pytest
from ib_async import IB, Option

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import (OptionScope, OptionVerifiedAccount, BrokerOptionBinding,
    OptionExecutionRequest, OptionLegIntent)
from algo_trader_broker_sdk.options_events import OptionRawEvent
from algo_trader_broker_sdk.options_capabilities import OptionCapability, OptionShapeCapability
from algo_trader_broker_adapter_ibkr_paper import IBKRPaperAdapter
from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient
from algo_trader_broker_adapter_ibkr_paper.settings import IBGatewaySettings
from algo_trader_broker_adapter_ibkr_paper.options_orders import build_order
from algo_trader_broker_adapter_ibkr_paper.options_events import SOURCE, decode


@pytest.mark.parametrize('environment',['paper','live'])
@pytest.mark.parametrize('native_account',['DU-OPTIONS-FIXTURE','U-OPTIONS-FIXTURE','F-OPTIONS-FIXTURE'])
def test_connection_order_and_callback_share_explicit_environment(environment, native_account, monkeypatch):
    async def scenario():
        settings = IBGatewaySettings(environment=environment, account=native_account, port=14004)
        settings.validate()  # A custom port does not determine the environment.
        ib = IB()
        monkeypatch.setattr(ib, 'isConnected', lambda: True)
        ib.wrapper.accounts = [native_account]
        client = IBAsyncClient(settings)
        client._ib = ib
        client._connected.set()
        client._connected_since = datetime.now(timezone.utc)
        adapter = IBKRPaperAdapter({'ib_environment': environment, 'ib_account': native_account}, client=client)
        scope = OptionScope('options/1.0', 'a' * 64, 'fixture-profile', 1, 'fixture-account', environment)
        verified = OptionVerifiedAccount(scope, 'IBKR', native_account)
        adapter.bind_option_account(verified)
        assert adapter.manifest().environment == adapter.capabilities().environment == environment.upper()

        async def read(connection, bound):
            assert connection is ib and bound == verified
            return bound.native_account_ref
        assert await adapter._option_read(scope, read) == native_account
        with pytest.raises(BrokerContractError):
            adapter.bind_option_account(replace(verified, scope=replace(scope, environment='live' if environment == 'paper' else 'paper')))
        with pytest.raises(BrokerContractError):
            adapter.bind_option_account(replace(verified, native_account_ref=native_account + '-OTHER'))

        now = datetime.now(timezone.utc)
        at = now.isoformat().replace('+00:00', 'Z')
        binding = BrokerOptionBinding('ibkr_paper', adapter.manifest().adapter_version, environment, scope.account,
            'opt_' + '1' * 64, '1590', 'SPY261009C00590000', 'SMART', at, 'fixture-metadata')
        contract = Option(symbol='SPY', lastTradeDateOrContractMonth='20261009', strike=590, right='C',
            exchange='SMART', currency='USD', multiplier='100', conId=1590, localSymbol=binding.local_symbol)
        request = OptionExecutionRequest(**asdict(scope), command_id='ib-opt', client_order_id='ato-fixture-opt',
            plan_hash='b' * 64, authorization_ref='fixture-reservation', quote_snapshot_ref='c' * 64,
            valid_until=(now + timedelta(seconds=30)).isoformat().replace('+00:00', 'Z'), capability_revision='fixture-ib',
            groups=2, order_type='LMT', signed_limit='1.25', tif='DAY', max_slippage='0',
            legs=(OptionLegIntent('L1', binding, 'BUY', 'OPEN', 1),))
        shape = OptionShapeCapability('LONG_CALL', 'NATIVE',
            OptionCapability(environment.upper() + '_CERTIFIED', ('EXPLICIT_FIXTURE_ONLY',), ('synthetic-ib-shape',)),
            1, 1, '0.01', False, False, False, False)
        _, order = build_order(request, [contract], native_account, 40, 100, shape)
        assert (order.account, order.orderRef, order.totalQuantity) == (native_account, request.client_order_id, 2)
        # Execution uses the explicit native environment, not certification labels.
        with pytest.raises(BrokerContractError):
            build_order(replace(request, environment='paper' if environment == 'live' else 'live'),
                [contract], native_account, 40, 100, shape)

        raw = dict(native_account_ref=native_account, kind='openOrder', observed_at=at, contract={'secType': 'OPT'},
            order=dict(account=native_account, permId=123, orderRef=request.client_order_id, whatIf=False), state={'status': 'Submitted'})
        event = OptionRawEvent(scope, SOURCE, json.dumps(raw).encode(), None)
        result = await decode(event, None)
        assert result.scope == scope and result.status == 'ACKNOWLEDGED'
        ib.wrapper.accounts = []
        with pytest.raises(BrokerContractError):
            await adapter._option_read(scope, read)
        monkeypatch.setattr(ib, 'isConnected', lambda: False)
    asyncio.run(scenario())


@pytest.mark.parametrize('environment',['paper','live'])
@pytest.mark.parametrize('account',[None,'DU-OPTIONS-FIXTURE','U-OPTIONS-FIXTURE','F-OPTIONS-FIXTURE'])
def test_configured_account_is_not_inferred_or_restricted_by_environment(environment,account):
    IBGatewaySettings(environment=environment,account=account).validate()
    adapter=IBKRPaperAdapter({'ib_environment':environment,'ib_account':account},client=object())
    assert adapter.manifest().environment==environment.upper()
    assert adapter._configured_account==account


def test_unknown_environment_is_rejected_as_invalid_configuration():
    with pytest.raises(BrokerContractError):IBKRPaperAdapter({'ib_environment':'auto'},client=object())
