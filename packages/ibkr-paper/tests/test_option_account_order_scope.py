"""Native account orders retain their asset domain before Option ownership."""
import asyncio
from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
from types import SimpleNamespace as N
from unittest.mock import AsyncMock

import pytest
from ib_async import Contract, ContractDetails

from algo_trader_broker_adapter_ibkr_paper import options_account
from algo_trader_broker_adapter_ibkr_paper.options_account_orders import option_working_orders
from algo_trader_broker_sdk.options import OptionScope


def order(asset="OPT", *, perm_id=19001, status="Submitted", what_if=False, legs=None):
    contract = dict(secType=asset, conId=101, symbol="SPY", currency="USD")
    if legs is not None:
        contract["comboLegs"] = [dict(conId=value) for value in legs]
    return dict(order_id=17, contract=contract,
        order=dict(permId=perm_id, clientId=40, account="DU-FIXTURE", whatIf=what_if),
        state=dict(status=status), received_at=datetime.now(timezone.utc).isoformat())


def setup(rows, native_types=None):
    scope = OptionScope("options/1.0", sha256(b'["IBKR","PAPER","account"]').hexdigest(), "fixture", 9, "account", "paper")
    bound = N(scope=scope, native_account_ref="DU-FIXTURE")
    adapter = N(_qualification_timeout=.1, _option_catalog={}, adapter_id="ibkr_paper")
    async def details(contract):
        value = (native_types or {}).get(contract.conId)
        return [] if value is None else [ContractDetails(contract=Contract(conId=contract.conId, secType=value))]
    ib = N(reqContractDetailsAsync=AsyncMock(side_effect=details))
    archived = {}
    async def retain(value):
        raw = options_account.raw_bytes(value)
        reference = sha256(raw).hexdigest()
        archived[reference] = json.loads(raw)
        return reference
    working = dict(source="IB_ALL_OPEN_ORDERS", orders=deepcopy(rows))
    return adapter, ib, bound, working, retain, archived


@pytest.mark.asyncio
@pytest.mark.parametrize("asset", ["STK", "FUT", "CASH", "BOND"])
async def test_known_nonoption_orders_never_enter_option_ownership_or_pending_identity(asset):
    adapter, ib, bound, working, retain, _ = setup([order(asset, perm_id=0, status="PendingSubmit")])
    original = deepcopy(working)
    selected, reasons, proofs = await option_working_orders(adapter, ib, bound, working, retain=retain)
    assert selected == [] and reasons == set() and proofs == []
    assert working == original
    ib.reqContractDetailsAsync.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["Cancelled", "ApiCancelled", "Filled"])
async def test_only_explicit_native_terminal_states_leave_open_order_projection(status):
    adapter, ib, bound, working, retain, _ = setup([order(status=status)])
    selected, reasons, _ = await option_working_orders(adapter, ib, bound, working, retain=retain)
    assert not selected and not reasons
    assert working["orders"][0]["state"]["status"] == status


@pytest.mark.asyncio
async def test_real_option_pending_cancel_and_inactive_orders_stay_visible_and_whatif_is_not_an_order():
    rows = [order(), order(status="Inactive", perm_id=19002), order(status="PendingCancel", perm_id=19003),
            order(what_if=True, perm_id=0)]
    adapter, ib, bound, working, retain, _ = setup(rows)
    selected, reasons, _ = await option_working_orders(adapter, ib, bound, working, retain=retain)
    assert [row["order"]["permId"] for row in selected] == [19001, 19002, 19003]
    assert not reasons
    ib.reqContractDetailsAsync.assert_not_awaited()


@pytest.mark.asyncio
async def test_bag_requires_native_per_leg_proof_and_preserves_option_order_identity():
    row = order("BAG", legs=[101, 102])
    adapter, ib, bound, working, retain, archived = setup([row], {101:"OPT", 102:"OPT"})
    selected, reasons, proofs = await option_working_orders(adapter, ib, bound, working, retain=retain)
    assert selected == working["orders"] and not reasons
    assert options_account.native_order_reference(selected[0], bound.scope)[0].broker_order_id == "19001"
    assert {archived[ref]["contract"]["conId"] for ref in proofs} == {101, 102}
    assert all(archived[ref]["basis"] == "IB_CONTRACT_DETAILS" for ref in proofs)
    assert ib.reqContractDetailsAsync.await_count == 2


@pytest.mark.asyncio
async def test_verified_current_catalog_can_classify_option_bag_without_another_native_request():
    adapter, ib, bound, working, retain, archived = setup([order("BAG", legs=[101, 102])])
    for con_id in (101, 102):
        adapter._option_catalog[str(con_id)] = (N(account_scope=bound.scope.account,
            environment=bound.scope.environment, adapter_id=adapter.adapter_id,
            broker_contract_id=str(con_id), qualified_at="2026-10-29T15:00:00Z"),
            None, ContractDetails(contract=Contract(conId=con_id, secType="OPT")))
    selected, reasons, proofs = await option_working_orders(adapter, ib, bound, working, retain=retain)
    assert selected and not reasons
    assert all(archived[ref]["basis"] == "QUALIFIED_OPTION_CATALOG" for ref in proofs)
    ib.reqContractDetailsAsync.assert_not_awaited()
    # A different account's cached option types cannot certify this BAG.
    adapter._option_catalog["101"][0].account_scope = "another-account"
    selected, reasons, _ = await option_working_orders(adapter, ib, bound, working, retain=retain)
    assert not selected and reasons == {"NATIVE_OPEN_ORDER_ASSET_UNRESOLVED"}
    ib.reqContractDetailsAsync.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("row,types", [
    (order("BAG", legs=[101,102]), {101:"OPT",102:"STK"}),
    (order("BAG", legs=[101,102]), {101:"FUT",102:"STK"}),
    (order("BAG", legs=[101,102]), {101:"OPT"}),
    (order("BAG", legs=[]), {}),
    (order("BAG", legs=[[]]), {}),
    (order("FOP"), {}),
    (order("UNKNOWN"), {}),
])
async def test_unknown_or_mixed_assets_remain_explicit_blockers(row, types):
    adapter, ib, bound, working, retain, _ = setup([row], types)
    selected, reasons, _ = await option_working_orders(adapter, ib, bound, working, retain=retain)
    assert not selected and reasons == {"NATIVE_OPEN_ORDER_ASSET_UNRESOLVED"}
    assert working["orders"], "Unknown native exposure cannot disappear from its raw snapshot"


@pytest.mark.asyncio
async def test_qualified_nonoption_bag_is_outside_option_ownership_and_metadata_failure_is_not_flatness():
    row = order("BAG", legs=[101,102])
    adapter, ib, bound, working, retain, _ = setup([row], {101:"FUT",102:"FUT"})
    selected, reasons, proofs = await option_working_orders(adapter, ib, bound, working, retain=retain)
    assert not selected and not reasons and len(proofs) == 2
    ib.reqContractDetailsAsync.side_effect = TimeoutError("native metadata unavailable")
    selected, reasons, _ = await option_working_orders(adapter, ib, bound, working, retain=retain)
    assert not selected and reasons == {"NATIVE_OPEN_ORDER_ASSET_UNRESOLVED"}


@pytest.mark.asyncio
@pytest.mark.parametrize("with_option_bag", [False, True])
async def test_account_snapshot_keeps_all_raw_orders_and_collects_funds_after_bag_classification(monkeypatch, with_option_bag):
    future_stop = order("FUT", perm_id=19200, status="PreSubmitted")
    future_stop["contract"].update(symbol="MNQ", localSymbol="MNQZ6", lastTradeDateOrContractMonth="20261218")
    future_stop["order"].update(orderId=900, parentId=899, action="BUY", totalQuantity=1,
        orderType="STP", tif="GTC", auxPrice="29664.75", orderRef="fprot-synthetic-protection")
    rows = [order("STK", perm_id=0), future_stop, order(status="Cancelled", perm_id=19003)]
    if with_option_bag:
        rows.append(order("BAG", perm_id=19002, legs=[101,102]))
    adapter, ib, bound, working, retain, archived = setup(rows, {101:"OPT",102:"OPT"})
    adapter._option_account_read_lock = asyncio.Lock()
    adapter._option_account_evidence = []
    calls = []
    now = lambda: datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    async def read(request, operation):
        return await operation(ib, bound)
    adapter._option_read = read
    ib.portfolio = lambda _: []
    native_details = ib.reqContractDetailsAsync.side_effect
    async def details(contract):
        calls.append("bag-leg")
        return await native_details(contract)
    ib.reqContractDetailsAsync.side_effect = details
    async def values(*_, **__):
        calls.append("funds")
        return dict(source="IB_ACCOUNT_UPDATE_MULTI", started_at=now(), completed_at=now(),
            values=[dict(tag=tag,value=value,currency=currency) for tag,value,currency in (
                ("Currency","USD","BASE"),("NetLiquidation","10000","USD"),
                ("TotalCashValue","9000","USD"),("AvailableFunds","8000","USD"))])
    monkeypatch.setattr(options_account,"positions",AsyncMock(return_value=dict(source="IB_POSITION_MULTI",
        positions=[], started_at=now(), completed_at=now())))
    monkeypatch.setattr(options_account,"open_orders",AsyncMock(return_value=working))
    monkeypatch.setattr(options_account,"completed_orders",AsyncMock(return_value=dict(orders=[])))
    monkeypatch.setattr(options_account,"executions",AsyncMock(return_value=dict(executions=[])))
    monkeypatch.setattr(options_account,"account_values",values)
    async def retain_raw(raw):
        ref=sha256(raw).hexdigest()
        archived[ref]=json.loads(raw)
        return ref
    state = await options_account.account_state(adapter, bound.scope, retain_evidence=retain_raw)
    assert calls == (["bag-leg","bag-leg","funds"] if with_option_bag else ["funds"])
    assert [(ref.broker_order_session_key,ref.broker_order_id) for ref in state.open_order_refs] == ([("IBKR_PERM_ID","19002")] if with_option_bag else [])
    assert "NATIVE_ORDER_PERM_ID_PENDING" not in state.quality_reasons
    assert (state.cash_available,state.option_buying_power) == ("9000","8000")
    raw = archived[state.source_checkpoint]
    assert raw["orders"] == working and len(raw["orders"]["orders"]) == len(rows)
    assert raw["orders"]["orders"][1] == future_stop, "A futures protective stop must remain intact, never canceled or relabeled"
    assert len(raw["open_order_asset_evidence_refs"]) == (2 if with_option_bag else 0)
    assert state.orders_complete

    # The typed Account boundary must still reject the real unowned option BAG.
    from ati_shared_sdk.common.options.account_evidence import OrdersOwnershipEvidence, reconcile_account
    from ati_shared_sdk.common.options.models import ExecutionTarget
    from src.account.options_projection import RunnerAccountEvidence
    target = ExecutionTarget(broker="IBKR",environment="PAPER",account_id=bound.scope.account,
        profile_id=bound.scope.profile_id,expected_generation=bound.scope.expected_generation)
    source = RunnerAccountEvidence(N(option_account_state=AsyncMock(return_value=state),
        option_recovery_context=AsyncMock(return_value=None)))
    observation = await source.option_account_observation(target)
    ownership = OrdersOwnershipEvidence(execution_target=target,as_of=now(),checkpoint="a"*64,
        complete=True,lots=[],allocated_lifecycle_refs=[],unresolved_order_refs=[],owned_order_refs=[],quality_reasons=[])
    evidence = reconcile_account(observation,ownership)
    if with_option_bag:
        assert not evidence.complete and evidence.blocking_reasons == ["UNOWNED_ORDERS"]
        assert evidence.unowned_order_refs == observation.open_order_refs
    else:
        assert evidence.complete and evidence.blocking_reasons == []
        assert evidence.unowned_order_refs == []
