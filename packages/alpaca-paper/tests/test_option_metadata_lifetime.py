from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as N

import pytest
from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_adapter_alpaca_paper.options_reads import AlpacaOptionReads


@pytest.mark.parametrize('age,valid', [(120, True), (299, True), (301, False), (-1, False)])
def test_static_binding_lifetime_keeps_original_clock_and_scope(age, valid):
    at = (datetime.now(timezone.utc)-timedelta(seconds=age)).isoformat().replace('+00:00','Z')
    binding = N(broker_contract_id='native', canonical_id='contract', local_symbol='symbol',
        adapter_id='alpaca_paper', adapter_version='test', account_scope='account', environment='paper', qualified_at=at)
    adapter = N(_option_catalog={'native': (binding, None, None)})
    bound = N(scope=N(account='account', environment='paper'))
    if valid:
        AlpacaOptionReads._validate_option_bindings(adapter, bound, [binding])
        assert binding.qualified_at == at
        with pytest.raises(BrokerContractError):
            AlpacaOptionReads._validate_option_bindings(adapter, N(scope=N(account='another', environment='paper')), [binding])
    else:
        with pytest.raises(BrokerContractError):
            AlpacaOptionReads._validate_option_bindings(adapter, bound, [binding])
