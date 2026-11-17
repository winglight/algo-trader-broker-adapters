from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as N
from copy import deepcopy

import pytest
from ib_async import Option, ContractDetails

from algo_trader_broker_adapter_ibkr_paper.options_account import cached_position_contract


def fixture():
    at = (datetime.now(timezone.utc)-timedelta(seconds=2)).isoformat().replace('+00:00', 'Z')
    option = Option('QQQ', '20261120', 665, 'C', 'SMART', multiplier='100', currency='USD',
        conId=1234, localSymbol='QQQ   261120C00665000')
    binding = N(adapter_id='ibkr_paper', account_scope='account', environment='paper',
        broker_contract_id='1234', qualified_at=at)
    bound = N(scope=N(account='account', environment='paper', expected_generation=1))
    cached = (binding, object(), ContractDetails(contract=option))
    adapter = N(_option_catalog={'1234': cached}, _option_account_binding=bound, adapter_id='ibkr_paper')
    return adapter, bound, deepcopy(vars(option)), cached


def test_static_metadata_keeps_its_original_clock_and_current_position_fields_are_not_cached():
    adapter, bound, native, cached = fixture()
    at = cached[0].qualified_at
    assert cached_position_contract(adapter, bound, native) is cached
    native['quantity'] = '-2'
    native['average_cost'] = '123'
    assert cached_position_contract(adapter, bound, native) is cached
    assert cached[0].qualified_at == at


@pytest.mark.parametrize('case', ['stale', 'future', 'account', 'environment', 'generation', 'symbol',
    'expiry', 'right', 'strike', 'multiplier', 'local_symbol', 'currency'])
def test_changed_scope_or_economics_requires_native_qualification(case):
    adapter, bound, native, cached = fixture()
    if case in {'stale', 'future'}:
        cached[0].qualified_at = (datetime.now(timezone.utc)+timedelta(seconds=-31 if case == 'stale' else 10)).isoformat().replace('+00:00', 'Z')
    if case == 'account': cached[0].account_scope = 'other'
    if case == 'environment': cached[0].environment = 'live'
    if case == 'generation': bound = N(scope=N(account='account', environment='paper', expected_generation=2))
    for field, value in {'symbol': ('symbol', 'SPY'), 'expiry': ('lastTradeDateOrContractMonth', '20261121'),
        'right': ('right', 'P'), 'strike': ('strike', 666), 'multiplier': ('multiplier', '10'),
        'local_symbol': ('localSymbol', 'another'), 'currency': ('currency', 'EUR')}.items():
        if case == field: native[value[0]] = value[1]
    assert cached_position_contract(adapter, bound, native) is None
