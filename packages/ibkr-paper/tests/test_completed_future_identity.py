"""Historical zero session ids are raw evidence, never writable DTO identity."""
from types import SimpleNamespace as N

from algo_trader_broker_adapter_ibkr_paper.client import _trade_to_update


def test_completed_future_zero_order_id_preserves_permanent_identity_and_raw_zero():
    trade=N(order=N(orderId=0,parentId=0,permId=1426890514,
        orderRef='fprot-fixture',account='DU_TEST',totalQuantity=0,orderType='STP',
        action='BUY',openClose='C',auxPrice=29664.75),
        orderStatus=N(status='Filled',orderId=0,parentId=0,permId=1426890514),
        contract=N(secType='FUT',symbol='MNQ',conId=815824267,multiplier='2'))
    update=_trade_to_update(trade,'poll.completed_orders')
    assert update.adapter_order_id is None
    assert update.adapter_order_ref=='1426890514'
    assert update.client_order_id=='fprot-fixture'
    assert update.message['order']['orderId']==0
    assert update.message['order']['parentId']==0
    assert update.message['order']['totalQuantity']==0
    assert update.message['contract']['multiplier']=='2'
