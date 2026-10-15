"""Native identity rejection is enforced even when the transport DTO is neutral."""
from types import SimpleNamespace
import pytest
from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_adapter_ibkr_paper import account_identity as native

TIME = "2026-10-15T14:00:00Z"
SOURCE = 'IB_ACCOUNT_EXECUTION'
INVENTORY = 'IB_POSITION_MULTI'
ACTIVITY = '000123.abcdef.01.01'
EXECUTION = '000123.abcdef.01.01'
ORDER = 'IBKR_PERM_ID:5'
INSTRUMENT = 'IBKR:100'


def test_valid_native_facts_retain_complete_original_identities():
    execution = native.cash_execution(SOURCE, ACTIVITY, EXECUTION, ORDER, INSTRUMENT,
        "QQQ", "BUY", "1", "20", "USD", TIME, "a"*64, "b"*64)
    assert (execution.activity_id, execution.execution_id, execution.broker_order_id, execution.instrument_id) == (ACTIVITY, EXECUTION, ORDER, INSTRUMENT)
    assert execution.provider_id == "IBKR"
    position = native.stock_position(INSTRUMENT, "QQQ", "1", "USD", "a"*64, TIME, TIME)
    assert native.stock_inventory(INVENTORY, (position,), True, (), TIME, TIME, "a"*64).positions == (position,)


@pytest.mark.parametrize("index,value", [(0, 'wrong-source'), (1, 'unrelated-activity'), (2, ''), (2, 'not-a-native-execution'), (2, ' 000123.abcdef.01.01'), (3, 'wrong-order'), (4, 'wrong-asset'), (4, 'OTHER:100'), (2, '000123.abcdef.01.1'), (2, '000123.abcdef.01.00'), (2, '000123.abcdef.01.001'), (3, 'IBKR_PERM_ID:0'), (4, 'IBKR:0')])
def test_invalid_cash_identities_fail_before_transport_dto(monkeypatch,index,value):
    def forbidden(*args, **kwargs):
        raise AssertionError("Invalid native identity reached normalized transport")
    monkeypatch.setattr(native,"NativeCashExecution",forbidden)
    values = [SOURCE, ACTIVITY, EXECUTION, ORDER, INSTRUMENT]
    values[index] = value
    with pytest.raises(BrokerContractError):
        native.cash_execution(*values)


@pytest.mark.parametrize("instrument", ['', 'wrong-asset', 'OTHER:100', 'IBKR:0', 'IBKR:-1', 'IBKR: 100'])
def test_invalid_stock_identity_fails_before_transport_dto(monkeypatch,instrument):
    def forbidden(*args, **kwargs):
        raise AssertionError("Invalid native stock reached normalized transport")
    monkeypatch.setattr(native,"NativeStockPosition",forbidden)
    monkeypatch.setattr(native,"NativeStockInventory",forbidden)
    with pytest.raises(BrokerContractError):
        native.stock_position(instrument)
    with pytest.raises(BrokerContractError):
        native.stock_inventory(INVENTORY,(SimpleNamespace(instrument_id=instrument),))


def test_native_provider_identity_cannot_be_overridden():
    with pytest.raises(BrokerContractError):
        native.cash_execution(SOURCE,ACTIVITY,EXECUTION,ORDER,INSTRUMENT,provider_id="other-provider")
    with pytest.raises(BrokerContractError):
        native.stock_inventory(INVENTORY,(),provider_id="other-provider")
