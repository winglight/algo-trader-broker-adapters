"""Exercise the pinned conditional native path without loading a Gateway or SDK client."""
import ast
import inspect
from pathlib import Path
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from algo_trader_broker_sdk.submission_gate import SubmissionGate

SRC = Path(__file__).resolve().parents[1] / "src" / "algo_trader_broker_adapter_ibkr_paper"


class NativeError(Exception):
    def __init__(self, message, code=None):
        super().__init__(message)
        self.code = code


class IB:
    def __init__(self):
        self.records = []
        self.placeOrderAsync = AsyncMock(side_effect=AssertionError("Must not use asynchronous writer"))
        self.fail_native = False

    def isConnected(self):
        return True

    def placeOrder(self, contract, order):
        self.records.append((contract, order))
        if self.fail_native:
            raise TimeoutError
        return "fake-trade"


def methods(file, cls, names, scope):
    path = SRC / file
    tree = ast.parse(path.read_text())
    source = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == cls)
    tree.body = [
        ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0),
        *[node for node in source.body if getattr(node, "name", None) in names
          or isinstance(node, ast.Assign) and any(
              getattr(target, "id", None) in names for target in node.targets)],
    ]
    exec(compile(ast.fix_missing_locations(tree), str(path), "exec"), scope)
    return {name: scope[name] for name in names}


class ConditionalNativeClientTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.IB = IB()
        self.client = type("FixtureClient", (), methods(
            "client.py", "IBAsyncClient", {"place_conditional_future_ioc", "_submit_order"}, {
                "IB": IB, "inspect": inspect, "IBOrderError": NativeError, "IBConnectionError": NativeError,
            }))()
        self.client._ib = self.IB
        self.client.ensure_connected = AsyncMock(return_value=self.IB)
        self.client._trade_update_handler = None
        self.client._account_update_handler = None
        self.client._await_placed_order = AsyncMock(return_value="fake-ack")
        self.contract = SimpleNamespace(secType="FUT", conId=42, localSymbol="MNQZ6")
        self.order = SimpleNamespace(
            tif="IOC", orderType="LMT", lmtPrice=25889.0, account="fixture-account", orderRef="stable",
            openClose="O", orderId=0, parentId=0, ocaGroup="", action="BUY", totalQuantity=1,
        )
        self.request = SimpleNamespace(
            tif="IOC", order_type="LMT", limit_price=25889.0, side="BUY", quantity=1, open_close="O",
            order_id=None, account="fixture-account", client_order_id="stable", contract_id=42,
            local_symbol="MNQZ6", build=lambda: (self.contract, self.order),
        )
        self.client.qualify_contract = AsyncMock(return_value=self.contract)
        self.validate = AsyncMock()
        self.events = []
        self.gate = SubmissionGate(self.validate, lambda: self.events.append("consume"))

    async def test_exact_futures_ioc_reuses_ack_with_sync_send_after_gate(self):
        self.assertEqual(await self.client.place_conditional_future_ioc(self.request, gate=self.gate), "fake-ack")
        self.validate.assert_awaited_once()
        self.assertEqual(self.events, ["consume"])
        self.assertEqual(self.IB.records, [(self.contract, self.order)])
        self.client._await_placed_order.assert_awaited_once_with(
            self.IB, self.contract, self.order, "fake-trade",
        )
        self.IB.placeOrderAsync.assert_not_called()

    async def test_expired_final_scope_never_sends_even_after_qualification(self):
        def refused():
            raise NativeError("Fixture terminal guard rejected")
        gate = SubmissionGate(self.validate, refused)
        with self.assertRaisesRegex(NativeError, "Fixture terminal guard rejected"):
            await self.client.place_conditional_future_ioc(self.request, gate=gate)
        self.assertEqual(self.IB.records, [])
        self.client.qualify_contract.assert_awaited_once()
        self.client._await_placed_order.assert_not_called()

    async def test_requalified_other_conid_rejected_without_dispatch(self):
        self.client.qualify_contract.return_value = SimpleNamespace(secType="FUT", conId=43, localSymbol="MNQH7")
        with self.assertRaisesRegex(NativeError, "order changed"):
            await self.client.place_conditional_future_ioc(self.request, gate=self.gate)
        self.validate.assert_not_awaited()
        self.assertEqual(self.IB.records, [])

    async def test_explicit_day_or_transformed_native_order_is_rejected(self):
        self.request.tif = "DAY"
        with self.assertRaisesRegex(NativeError, "require an exact"):
            await self.client.place_conditional_future_ioc(self.request, gate=self.gate)
        self.request.tif = "IOC"
        self.order.tif = "DAY"
        with self.assertRaisesRegex(NativeError, "order changed"):
            await self.client.place_conditional_future_ioc(self.request, gate=self.gate)
        self.assertEqual(self.IB.records, [])

    async def test_client_replacement_between_preparation_and_send_is_rejected(self):
        async def replace():
            self.client._ib = IB()
        gate = SubmissionGate(replace, lambda: None)
        with self.assertRaisesRegex(NativeError, "disconnected"):
            await self.client.place_conditional_future_ioc(self.request, gate=gate)
        self.assertEqual(self.IB.records, [])

    async def test_unexpected_native_throw_is_unknown_and_not_retried(self):
        self.IB.fail_native = True
        with self.assertRaises(NativeError) as error:
            await self.client.place_conditional_future_ioc(self.request, gate=self.gate)
        self.assertEqual(error.exception.code, "broker_order_outcome_unknown")
        self.assertEqual(len(self.IB.records), 1)
        self.IB.placeOrderAsync.assert_not_called()

    async def test_default_adapter_has_no_capability_and_refuses_early(self):
        adapter = type("FixtureAdapter", (), methods("adapter.py", "IBKRPaperAdapter", {
            "conditional_future_ioc_version", "place_conditional_future_order_guarded",
        }, {"BrokerOrderError": NativeError}))()
        adapter._option_environment = "paper"
        self.assertEqual(adapter.conditional_future_ioc_version, 0)
        with self.assertRaises(NativeError) as error:
            await adapter.place_conditional_future_order_guarded(self.request, self.gate)
        self.assertEqual(error.exception.code, "conditional_future_ioc_unavailable")
        self.assertEqual(self.IB.records, [])

    async def test_installed_ib_async_wire_keeps_ioc_limit_and_exact_contract_without_network(self):
        from ib_async import Contract, IB as RealIB, Order

        # Invoke the real synchronous IB.placeOrder and client serializer; no
        # connection is created and the completed wire is captured in memory.
        ib = RealIB()
        ib.client.connState = type(ib.client).CONNECTED
        ib.client._apiReady = True
        ib.client._serverVersion = 178
        ib.client.getReqId = lambda: 71
        wire = []
        ib.client.sendMsg = lambda msg: wire.append(msg)
        self.client = type("FixtureRealWire", (), methods(
            "client.py", "IBAsyncClient", {"place_conditional_future_ioc", "_submit_order"}, {
                "IB": RealIB, "inspect": inspect, "IBOrderError": NativeError, "IBConnectionError": NativeError,
            }))()
        self.client._ib = ib
        self.client.ensure_connected = AsyncMock(return_value=ib)
        self.client._trade_update_handler = self.client._account_update_handler = None
        self.client._await_placed_order = AsyncMock(return_value="fixture-no-gateway-ack")
        contract = Contract(secType="FUT", conId=42, symbol="MNQ", exchange="CME", currency="USD",
                            localSymbol="MNQZ6")
        order = Order(action="BUY", totalQuantity=1, orderType="LMT", lmtPrice=25889.0,
                      tif="IOC", account="fixture-account", orderRef="stable", openClose="O")
        self.request.build = lambda: (contract, order)
        self.client.qualify_contract = AsyncMock(return_value=contract)
        self.assertEqual(await self.client.place_conditional_future_ioc(self.request, gate=self.gate),
                         "fixture-no-gateway-ack")
        packet, = wire
        wire_fields = packet.split("\0")
        self.assertEqual(wire_fields[:5], ["3", "71", "42", "MNQ", "FUT"])
        self.assertIn("MNQZ6", wire_fields)
        start = wire_fields.index("BUY")
        self.assertEqual(wire_fields[start:start+12],
                         ["BUY","1","LMT","25889.0","","IOC","","fixture-account","O","0","stable","1"])
