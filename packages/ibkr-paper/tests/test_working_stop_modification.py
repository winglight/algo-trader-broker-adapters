import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from ib_async import Contract, StopOrder

from algo_trader_broker_adapter_ibkr_paper.client import IBAsyncClient, IBOrderError


def test_stop_modification_copies_native_fields_and_preserves_identity():
    async def scenario():
        adapter = object.__new__(IBAsyncClient)
        adapter._open_orders_lock = asyncio.Lock()
        contract = Contract(conId=11)
        existing = StopOrder("BUY", 2, 105, orderId=89, account="DU1", tif="GTC", permId=42,
                             parentId=13, ocaGroup="bracket-13", outsideRth=True, triggerMethod=4)
        trade = SimpleNamespace(order=existing, contract=contract, orderStatus=SimpleNamespace(status="PreSubmitted"))
        ib = SimpleNamespace(reqOpenOrdersAsync=AsyncMock(return_value=[trade]))
        request = StopOrder("BUY", 3, 105.75, orderId=89, account="DU1")
        modified = await adapter._copy_working_stop(ib, contract, request)
        assert (modified.auxPrice, modified.totalQuantity) == (105.75, 3)
        assert (existing.auxPrice, existing.totalQuantity) == (105, 2)
        assert (modified.orderId, modified.permId, modified.parentId) == (89, 42, 13)
        assert (modified.tif, modified.ocaGroup, modified.outsideRth, modified.triggerMethod) == (
            "GTC", "bracket-13", True, 4,
        )
        trade.orderStatus.status = "Filled"
        with pytest.raises(IBOrderError):
            await adapter._copy_working_stop(ib, contract, request)

    asyncio.run(scenario())
