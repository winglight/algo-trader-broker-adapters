"""Scoped native callbacks retained before ib_async mutates or deduplicates them."""

import asyncio
from collections import deque
from decimal import Decimal, InvalidOperation
import json
import logging
import re

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import check, decimal_wire
from algo_trader_broker_sdk.options_events import (
    OptionExecutionRevision, OptionNativeExecution, OptionNativeFee, OptionNativeInterpretation, OptionNativeLeg, OptionRawEvent,
)

from .options_account_native import observe_callbacks, raw_bytes, raw_value
from .options_reads import native_decimal, now_wire

LOG = logging.getLogger(__name__)
SOURCE = "IB_OPTION_CALLBACK"
STATUS = {"Submitted": "ACKNOWLEDGED", "PreSubmitted": "ACKNOWLEDGED", "Filled": "FILLED",
          "PendingCancel": "CANCEL_PENDING", "Cancelled": "CANCELED", "ApiCancelled": "CANCELED"}


def execution_revision(exec_id):
    check(type(exec_id) is str and re.fullmatch(r"[^\s.]+(?:\.[^\s.]+)+\.[0-9]+", exec_id) is not None,
          "IB execution lacks a native revision sequence")
    family, _, suffix = exec_id.rpartition(".")
    revision = OptionExecutionRevision("IB_EXECUTION_ID", family + ".01", int(suffix))
    revision.check_native_id(exec_id)
    return revision


class IBOptionEventStream:
    def __init__(self, ib, bound, handler):
        self.ib, self.bound, self.handler = ib, bound, handler
        self.pending, self.task, self.failure = deque(), None, None
        self.executions = {}
        self.closed = False
        def observe(callback):
            def captured(*args):
                try:
                    callback(*args)
                except Exception as exc:
                    self.failure = exc
                    LOG.exception("IB native option callback needs recovery")
            return captured
        self.callbacks = observe_callbacks(ib.wrapper, {name: observe(callback) for name, callback in
            dict(openOrder=self.opened, completedOrder=self.completed, orderStatus=self.status,
                 execDetails=self.executed, commissionReport=self.commission).items()})
        self.callbacks.__enter__()

    def enqueue(self, kind, **payload):
        if self.closed:
            return
        try:
            # Serialize before the original wrapper may mutate the same objects.
            raw = raw_bytes(dict(kind=kind, native_account_ref=self.bound.native_account_ref,
                                 observed_at=now_wire(), **payload))
            native_id = payload.get("execution", {}).get("execId") if isinstance(payload.get("execution"), dict) else None
            self.pending.append(OptionRawEvent(self.bound.scope, SOURCE, raw, native_id))
            if self.failure is None and (self.task is None or self.task.done()):
                self.task = asyncio.create_task(self.drain())
        except Exception as exc:
            self.failure = exc
            LOG.exception("IB option callback could not be retained")

    async def drain(self):
        try:
            while self.pending:
                await self.handler(self.pending[0])
                self.pending.popleft()
        except Exception as exc:
            # Keep the failed bytes; further writes are fenced until recovery.
            self.failure = exc
            LOG.exception("IB option evidence persistence failed")

    async def flush(self):
        if self.task is not None:
            await self.task
        check(self.failure is None and not self.pending, "IB callback persistence requires recovery")

    def close(self):
        if not self.closed:
            self.closed = True
            self.callbacks.__exit__(None, None, None)
        # Already captured callbacks retain their original handler and scope.

    def order(self, order_id, client_id, perm_id):
        trade = self.ib.wrapper.permId2Trade.get(perm_id)
        if trade is None:
            trade = self.ib.wrapper.trades.get(self.ib.wrapper.orderKey(client_id, order_id, perm_id))
        if trade is None or trade.order.account != self.bound.native_account_ref or trade.contract.secType not in {"OPT", "BAG"}:
            return None
        return dict(order=raw_value(trade.order), contract=raw_value(trade.contract))

    def opened(self, order_id, contract, order, state):
        if contract.secType in {"OPT", "BAG"} and order.account == self.bound.native_account_ref and not order.whatIf:
            self.enqueue("openOrder", order_id=order_id, contract=contract, order=order, state=state)

    def completed(self, contract, order, state):
        if contract.secType in {"OPT", "BAG"} and order.account == self.bound.native_account_ref and not order.whatIf:
            self.enqueue("completedOrder", contract=contract, order=order, state=state)

    def status(self, order_id, status, filled, remaining, average, perm_id, parent_id, last_price, client_id, why_held, market_cap=0.0):
        native = self.order(order_id, client_id, perm_id)
        if native is not None:
            self.enqueue("orderStatus", **native, state=dict(status=status, filled=filled, remaining=remaining,
                avgFillPrice=average, permId=perm_id, parentId=parent_id, lastFillPrice=last_price,
                clientId=client_id, orderId=order_id, whyHeld=why_held, mktCapPrice=market_cap))

    def executed(self, req_id, contract, execution):
        if execution.acctNumber != self.bound.native_account_ref or contract.secType not in {"OPT", "BAG"}:
            return
        payload = dict(contract=raw_value(contract), execution=raw_value(execution),
                       parent=self.order(execution.orderId, execution.clientId, execution.permId))
        self.executions[execution.execId] = payload
        self.enqueue("execDetails", request_id=req_id, **payload)

    def commission(self, report):
        payload = self.executions.get(report.execId)
        if payload is None:
            fill = self.ib.wrapper.fills.get(report.execId)
            if fill is not None and fill.execution.acctNumber == self.bound.native_account_ref and fill.contract.secType in {"OPT", "BAG"}:
                payload = dict(contract=raw_value(fill.contract), execution=raw_value(fill.execution),
                    parent=self.order(fill.execution.orderId, fill.execution.clientId, fill.execution.permId))
        if payload is not None:
            self.enqueue("commissionReport", report=report, **payload)
        elif report.execId not in self.ib.wrapper.fills:
            # A commission with no execution association is still evidence,
            # but cannot become a fee for this account until reconciliation.
            self.enqueue("commissionReport", report=report, execution=None, contract=None, parent=None)


async def qualified_leg(event, contract, side, order_id, resolve):
    check(contract["secType"] == "OPT" and contract["currency"] == "USD" and side in {"BUY", "SELL"},
          "Only native OPT executions identify financial legs")
    native_id, symbol = str(contract["conId"]), contract["localSymbol"]
    result = await resolve(native_id, symbol)
    binding, economic = result.binding, result.contract
    check(result.status == "EXACT" and binding is not None and economic is not None
          and (binding.adapter_id, binding.environment, binding.account_scope, binding.broker_contract_id, binding.local_symbol)
          == ("ibkr_paper", event.scope.environment, event.scope.account, native_id, symbol)
          and result.canonical_id == binding.canonical_id == economic.canonical_id,
          "IB execution requires its retained exact qualification")
    return OptionNativeLeg(order_id, economic.canonical_id, side, economic.key.multiplier)


async def decode(event, resolve):
    try:
        return await interpret(event, resolve)
    except (ValueError, TypeError, KeyError, AttributeError, InvalidOperation) as exc:
        raise BrokerContractError("IB callback evidence remains unresolved") from exc


async def interpret(event, resolve):
    check(event.source == SOURCE and event.scope.environment == "paper", "Unsupported IB callback source")
    raw = json.loads(event.raw_payload)
    native_account = raw["native_account_ref"]
    check(type(native_account) is str and native_account.startswith("DU"), "IB callback lacks its Paper account")
    kind = raw["kind"]
    if kind in {"openOrder", "completedOrder", "orderStatus"}:
        order, contract, state = raw["order"], raw["contract"], raw["state"]
        check(order["account"] == native_account and contract["secType"] in {"OPT", "BAG"}
              and not order["whatIf"] and type(order["permId"]) is int and order["permId"] > 0,
              "IB parent status has no exact native account/order")
        status = STATUS.get(state["status"])
        check(status is not None, "IB parent status needs reconciliation")
        if kind == "orderStatus":
            check((state["orderId"], state["clientId"], state["permId"]) ==
                  (order["orderId"], order["clientId"], order["permId"]), "IB status changed native identity")
            if status == "ACKNOWLEDGED" and Decimal(str(state["filled"])) > 0:
                status = "PARTIALLY_FILLED"
        return OptionNativeInterpretation(event.scope, str(order["permId"]), order["orderRef"] or None,
            "IBKR_PERM_ID", status, raw["observed_at"], (), ())
    check(kind in {"execDetails", "commissionReport"}, "Unknown IB callback")
    execution, contract = raw["execution"], raw["contract"]
    check(execution is not None and execution["acctNumber"] == native_account
          and execution["modelCode"] == "" and type(execution["permId"]) is int and execution["permId"] > 0,
          "IB execution account/allocation is unresolved")
    exec_id = execution["execId"]
    revision = execution_revision(exec_id)
    check(not execution["pendingPriceRevision"], "IB execution price is still pending revision")
    parent = str(execution["permId"])
    native_parent = raw.get("parent")
    leg_id = parent
    if native_parent is None or native_parent["contract"]["secType"] == "BAG":
        leg_id += ":" + str(contract["conId"])
    if native_parent is not None:
        order = native_parent["order"]
        check((order["account"], order["permId"], order["orderRef"]) ==
              (native_account, execution["permId"], execution["orderRef"]),
              "IB execution changed its native parent")
        # completedOrder omits API client/order IDs; the execution itself
        # retains them. Do not replace those IDs with completed-order zeros.
        if order["orderId"] > 0:
            check((order["clientId"], order["orderId"]) == (execution["clientId"], execution["orderId"]),
                  "IB execution changed the observed API order identity")
    if contract["secType"] == "BAG":
        check(kind == "execDetails" and native_parent is not None
              and native_parent["contract"]["secType"] == "BAG", "IB BAG fee/allocation requires reconciliation")
        filled = Decimal(native_decimal(execution["cumQty"]))
        total = Decimal(str(native_parent["order"]["totalQuantity"]))
        check(0 < filled <= total, "IB BAG parent quantity is unresolved")
        return OptionNativeInterpretation(event.scope, parent, execution["orderRef"] or None, "IBKR_PERM_ID",
            "FILLED" if filled == total else "PARTIALLY_FILLED", execution["time"], (), ())
    side = {"BOT": "BUY", "SLD": "SELL"}.get(execution["side"])
    leg = await qualified_leg(event, contract, side, leg_id, resolve)
    effective = execution["time"]
    fills, fees = (), ()
    if kind == "execDetails":
        quantity = Decimal(native_decimal(execution["shares"]))
        check(quantity == quantity.to_integral_value() and 0 < quantity <= 2**53 - 1,
              "IB option executions need whole contracts")
        fills = (OptionNativeExecution(leg_id, exec_id, int(quantity), native_decimal(execution["price"]), effective,
            revision=revision if revision.revision > 1 else None),)
    else:
        report = raw["report"]
        check(report["execId"] == exec_id and report["currency"] == "USD", "IB commission changed execution or currency")
        amount = Decimal(str(report["commission"]))
        check(amount.is_finite(), "IB commission is unset")
        wire = format(amount, "f")
        wire = wire.rstrip("0").rstrip(".") if "." in wire else wire
        wire = "0" if amount == 0 else wire
        decimal_wire(wire)
        # The execution revision identifies this commission's version. A second
        # amount for the same execId has no new authority and stays a conflict.
        # Native reports do not establish finality or a separate fee timestamp.
        fees = (OptionNativeFee(leg_id, exec_id, wire, "USD", effective,
            revision=revision if revision.revision > 1 else None),)
    return OptionNativeInterpretation(event.scope, parent, execution["orderRef"] or None, "IBKR_PERM_ID",
        None, effective, (leg,), fills, fees=fees)
