"""IB account evidence for the shared Account projection and reconciliation."""

import asyncio
from collections.abc import Mapping
from dataclasses import asdict, replace
from datetime import timedelta
from decimal import Decimal, InvalidOperation, localcontext
from hashlib import sha256
from uuid import uuid4

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import ActivityQuery, check, decimal_wire, timestamp
from algo_trader_broker_sdk.options_account import (
    NativeBuyingPower, NativeOrderReference, OptionAccountPermissions, OptionAccountPosition,
    OptionAccountState, UnresolvedOptionPosition,
)

from .options_account_native import account_values, open_orders, completed_orders, executions, positions, raw_bytes, raw_value
from .options_reads import contract_from_details, native_decimal, now_wire


def signed_decimal(value):
    try:
        number = Decimal(str(value))
        absolute = native_decimal(number.copy_abs())
        return "-" + absolute if number < 0 else absolute
    except (ValueError, InvalidOperation) as exc:
        raise BrokerContractError("Invalid IB account amount") from exc


def money_values(snapshot):
    rows = snapshot["values"]
    values = {(row["tag"], row["currency"]): row["value"] for row in rows}
    # Aggregated totals with an explicit USD currency are usable as USD.
    # BASE-only values require an explicit native base-currency observation.
    base_usd = values.get(("Currency", "BASE")) == "USD"
    def read(tag, *, required=True):
        value = values.get((tag, "USD"))
        if value is None and base_usd:
            value = values.get((tag, "BASE"))
        check(value is not None or not required, f"Explicit USD {tag} is unavailable")
        return None if value is None else signed_decimal(value)
    raw = tuple(NativeBuyingPower(tag, value) for tag in (
        "AvailableFunds", "BuyingPower", "ExcessLiquidity", "FullAvailableFunds",
        "InitMarginReq", "MaintMarginReq") if (value := read(tag, required=False)) is not None)
    ready = [row["value"].lower() for row in rows if row["tag"] == "AccountReady" or row["tag"] == "accountReady"]
    return read, raw, bool(ready) and ready[-1] == "false"


def native_order_reference(row, scope):
    order = row["order"]
    perm_id, client_id, order_id = order["permId"], order["clientId"], row["order_id"]
    check(type(perm_id) is int and type(client_id) is int and type(order_id) is int, "Invalid native order identity")
    if perm_id > 0:
        return NativeOrderReference("IBKR_PERM_ID", str(perm_id)), False
    check(client_id >= 0 and order_id >= 0, "Unacknowledged order has no client/order identity")
    return NativeOrderReference(f"IBKR_CLIENT_{client_id}_GEN_{scope.expected_generation}", str(order_id)), True


def position_cost(row, portfolio, account, *, base_usd):
    """Verify the current position's unit against a native account sample.

    The existing updatePortfolio cache supplies market value and unrealized P/L
    from one callback. It is used only to verify carrying-cost units, never as a
    fresh quote or position snapshot. No subscription is created or replaced.
    """
    try:
        cost = signed_decimal(row["average_cost"])
    except BrokerContractError:
        return None, "UNKNOWN", None
    fallback = cost, "UNKNOWN", None
    if not base_usd or portfolio is None:
        return fallback
    try:
        native, contract = row["contract"], portfolio.contract
        check(portfolio.account == account and contract.secType == "OPT" and contract.currency == "USD" and
              all(str(getattr(contract, name)) == str(native[name]) for name in (
                  "conId", "localSymbol", "symbol", "right", "lastTradeDateOrContractMonth")) and
              native_decimal(contract.strike) == native_decimal(native["strike"]) and
              native_decimal(contract.multiplier) == native_decimal(native["multiplier"]),
              "Portfolio cost sample differs from the qualified position")
        quantity = Decimal(signed_decimal(row["quantity"]))
        average = Decimal(cost)
        check(Decimal(signed_decimal(portfolio.position)) == quantity and
              Decimal(signed_decimal(portfolio.averageCost)) == average,
              "Portfolio cost sample differs from current quantity or average cost")
        with localcontext() as context:
            context.prec = 160
            cash = Decimal(signed_decimal(portfolio.marketValue)) - Decimal(signed_decimal(portfolio.unrealizedPNL))
            check((quantity != 0 or cash == 0) and cash * quantity >= 0, "Invalid signed portfolio cost")
            candidates = {"CASH_TOTAL": average}
            if average >= 0:
                candidates.update(CASH_PER_CONTRACT=average * quantity,
                    PREMIUM_PER_UNIT=average * quantity * Decimal(native_decimal(contract.multiplier)))
            matches = [unit for unit, value in candidates.items() if value == cash]
        if not matches:
            return fallback
        # With one contract or zero cost, several units can yield the same
        # cash. Use the independently observed total for this position only;
        # do not certify an ambiguous avgCost unit for other positions.
        unit = matches[0] if len(matches) == 1 else "CASH_TOTAL"
        value = cost if len(matches) == 1 else signed_decimal(cash)
        proof = dict(source="IB_POSITION_COST_SAMPLE", rule="native-portfolio-cost-v1", account=account,
            position=row, portfolio=raw_value(portfolio._asdict()), retrieved_at=now_wire(),
            cost_cash=signed_decimal(cash), raw_cost_value=value, raw_cost_unit=unit,
            basis="AVG_COST_UNIT_MATCH" if len(matches) == 1 else "PORTFOLIO_VALUE_MINUS_UNREALIZED_PNL")
        return value, unit, proof
    except (BrokerContractError, InvalidOperation, ValueError, TypeError):
        return fallback


async def permissions(adapter, request):
    async def read(ib, bound):
        async with adapter._option_account_read_lock:
            values = await account_values(ib, bound.native_account_ref, timeout=adapter._qualification_timeout)
        _, raw_bp, not_ready = money_values(values)
        observed = values["completed_at"]
        settings = adapter._settings
        read_only = settings.get("ib_read_only") if isinstance(settings, Mapping) else settings.read_only
        read_only = read_only is True or str(read_only).lower() in {"1", "true", "yes", "on"}
        live = adapter._option_live_observed
        entitled = live is not None and live[0] == bound.scope and 0 <= (timestamp(observed) - timestamp(live[1])).total_seconds() < 30
        reasons = ["IB_OPTION_APPROVAL_UNOBSERVED", "LIFECYCLE_SOURCE_UNVERIFIED"]
        if not entitled:
            reasons.append("LIVE_DATA_ENTITLEMENT_UNOBSERVED")
        if not_ready:
            reasons.append("IB_ACCOUNT_NOT_READY")
        if read_only:
            reasons.append("IB_CONNECTION_READ_ONLY")
        # TWS account fields do not establish a broker option approval level.
        raw = raw_bytes(values)
        adapter._option_account_evidence.append(raw)
        return OptionAccountPermissions(bound.scope, "UNKNOWN", None, (),
            "DENIED" if read_only or not_ready else "UNKNOWN", "ALLOWED" if entitled else "UNKNOWN",
            "UNKNOWN", "AVAILABLE_FUNDS" if any(item.name == "AvailableFunds" for item in raw_bp) else "UNKNOWN",
            observed, (timestamp(observed) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z"),
            sha256(raw).hexdigest(), tuple(reasons))
    return await adapter._option_read(request, read)


async def account_state(adapter, request, *, retain_evidence=None, resolve_contract=None, flex_state=None):
    async def read(ib, bound):
        activities, lifecycle_reasons = [], []
        fees, fee_reasons = (), ("IB_COMMISSION_RECONCILIATION_REQUIRED",)
        if retain_evidence is not None:
            query = ActivityQuery(**asdict(bound.scope), since=None, cursor=None, limit=200)
            for _ in range(100):
                page = await adapter.option_lifecycle_events(query, retain_evidence=retain_evidence,
                    resolve_contract=resolve_contract, flex_state=flex_state)
                activities.extend(page.events)
                lifecycle_reasons.extend(page.unresolved_refs)
                if page.next_cursor is None:
                    break
                query = replace(query, cursor=page.next_cursor)
            else:
                lifecycle_reasons.append("IB_FLEX_PAGE_LIMIT")
            if flex_state is not None:
                raw = await flex_state.source_statement()
                if raw is not None:
                    from .options_cash_fees import parse
                    fees, cash_fee_reasons = await parse(raw, account=bound.native_account_ref, retain=retain_evidence)
                    fee_reasons += cash_fee_reasons
                else:
                    fee_reasons += ("IB_FLEX_CASH_SOURCE_UNAVAILABLE",)
        async with adapter._option_account_read_lock:
            # Funds/positions have separate IDs; open orders has a shared end
            # marker and is read only when no other download is in progress.
            inventory = await positions(ib, bound.native_account_ref, timeout=adapter._qualification_timeout)
            working = await open_orders(ib, bound.native_account_ref, timeout=adapter._qualification_timeout)
            completed = await completed_orders(ib, bound.native_account_ref, timeout=adapter._qualification_timeout)
            fills = await executions(ib, bound.native_account_ref, timeout=adapter._qualification_timeout)
            # Funds follow the execution observation used to prove cash booking.
            values = await account_values(ib, bound.native_account_ref, timeout=adapter._qualification_timeout)
        read_money, raw_bp, not_ready = money_values(values)
        base_usd = {(row["tag"], row["currency"]): row["value"] for row in values["values"]}.get(("Currency", "BASE")) == "USD"
        portfolio = {item.contract.conId: item for item in ib.portfolio(bound.native_account_ref)}
        cost_refs = []
        exact, unresolved, underlyings = [], [], {}
        reasons = ["EXECUTION_RECONCILIATION_REQUIRED", "LIFECYCLE_RECONCILIATION_REQUIRED",
                   "IB_MANUAL_ORDER_VISIBILITY_UNVERIFIED"]
        if lifecycle_reasons:
            reasons.append("LIFECYCLE_SOURCE_INCOMPLETE")
        if not_ready:
            reasons.append("IB_ACCOUNT_NOT_READY")

        async def retain(value):
            raw = raw_bytes(value)
            digest = sha256(raw).hexdigest()
            if retain_evidence is not None:
                check(await retain_evidence(raw) == digest, "Account evidence archive changed native hash")
            adapter._option_account_evidence.append(raw)
            return digest

        for row in inventory["positions"]:
            native = row["contract"]
            asset = native["secType"]
            if asset in {"STK", "FUT", "CASH", "BOND", "CRYPTO", "CFD", "CMDTY", "FUND", "IND"}:
                continue
            quantity = signed_decimal(row["quantity"])
            raw_ref = await retain(dict(source="IB_POSITION_MULTI", account=bound.native_account_ref, **row))
            con_id = native["conId"]
            position_ref = f"IBKR_CONID_{con_id}"
            try:
                from ib_async import Contract
                check(asset == "OPT" and type(con_id) is int and con_id > 0, "Unsupported option position")
                symbol = native["symbol"]
                if symbol not in underlyings:
                    underlyings[symbol] = await adapter._underlying(ib, symbol)
                details = await asyncio.wait_for(ib.reqContractDetailsAsync(Contract(conId=con_id, exchange="SMART")),
                                                 adapter._qualification_timeout)
                check(len(details) == 1 and details[0].contract.conId == con_id, "Position contract did not qualify uniquely")
                detail = details[0]
                contract = contract_from_details(detail, symbol, underlyings[symbol])
                check(native["localSymbol"] == detail.contract.localSymbol
                      and native["currency"] == contract.key.currency
                      and native_decimal(native["multiplier"]) == "100"
                      and native_decimal(native["strike"]) == contract.key.strike
                      and native["right"] == contract.key.right
                      and native["lastTradeDateOrContractMonth"] == contract.key.expiry.replace("-", ""),
                      "Position callback differs from qualified economics")
                number = decimal_wire(quantity)
                check(number == number.to_integral_value(), "Option inventory must be whole contracts")
                binding = adapter._remember_contract(bound, detail, contract)
                cost, unit, proof = position_cost(row, portfolio.get(con_id), bound.native_account_ref, base_usd=base_usd)
                if proof is not None:
                    raw_ref = await retain(proof)
                    cost_refs.append(raw_ref)
                exact.append(OptionAccountPosition(contract, binding, position_ref, int(number), cost, unit,
                    raw_ref, raw_ref, row["received_at"], row["received_at"]))
            except BrokerContractError:
                unresolved.append(UnresolvedOptionPosition(position_ref, str(con_id), asset or "UNKNOWN", quantity,
                    raw_ref, raw_ref, ("CONTRACT_METADATA_UNRESOLVED",)))
        refs = []
        for row in working["orders"]:
            ref, pending = native_order_reference(row, bound.scope)
            refs.append(ref)
            if pending:
                reasons.append("NATIVE_ORDER_PERM_ID_PENDING")
        if unresolved:
            reasons.append("UNRESOLVED_OPTION_POSITIONS")
        if any(item.raw_cost_unit == "UNKNOWN" for item in exact):
            reasons.append("IB_POSITION_COST_UNIT_UNVERIFIED")
        from .cash_account import collect
        cash = await collect(bound.native_account_ref, working, completed, fills, retain=retain)
        checkpoint = await retain(dict(source="IB_ACCOUNT_SNAPSHOT", values=values, positions=inventory, orders=working,
                                      cost_evidence_refs=cost_refs, completed_orders=completed, executions=fills,
                                      lifecycle_refs=[item.raw_ref for item in activities], lifecycle_unresolved=lifecycle_reasons,
                                      account_fee_refs=[item.raw_ref for item in fees], account_fee_unresolved=fee_reasons))
        return OptionAccountState(bound.scope, str(uuid4()), values["started_at"], now_wire(), "USD",
            read_money("NetLiquidation"), read_money("TotalCashValue"), read_money("AvailableFunds", required=False),
            raw_bp, "AVAILABLE_FUNDS" if any(item.name == "AvailableFunds" for item in raw_bp) else "UNKNOWN",
            checkpoint, tuple(exact), tuple(unresolved), tuple(refs), tuple(activities), True, False, False, False,
            tuple(sorted(set(reasons))), fees, False, fee_reasons, cash)
    return await adapter._option_read(request, read)
