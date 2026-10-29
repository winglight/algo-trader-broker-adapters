"""Project only proven option working orders into Option Orders ownership.

The enclosing account snapshot still archives the complete broker download.
Unknown assets and mixed combinations remain explicit blocking evidence.
"""
import asyncio

from algo_trader_broker_sdk.options import check

from .options_reads import now_wire


TERMINAL_STATES = frozenset({"Filled", "Cancelled", "ApiCancelled"})
NON_OPTION_ASSETS = frozenset({"STK", "FUT", "CASH", "BOND", "CRYPTO", "CFD", "CMDTY", "FUND", "IND"})


async def option_working_orders(adapter, ib, bound, working, *, retain):
    """Return option rows, blocking reasons and retained native asset proofs.

    Resolve BAG conIds before the final funds observation. A symbol or BAG
    label alone does not establish the asset class of its constituent legs.
    """
    from ib_async import Contract

    selected, reasons, proofs, types = [], set(), [], {}
    deadline = asyncio.get_running_loop().time() + adapter._qualification_timeout

    async def asset_type(con_id):
        if type(con_id) is not int or con_id <= 0:
            return None
        if con_id in types:
            return types[con_id]
        try:
            check(type(con_id) is int and con_id > 0, "BAG leg needs a native contract ID")
            cached = adapter._option_catalog.get(str(con_id))
            if cached is not None:
                binding, _, detail = cached
                native = detail.contract
                if (binding.account_scope == bound.scope.account
                        and binding.environment == bound.scope.environment
                        and binding.adapter_id == adapter.adapter_id
                        and binding.broker_contract_id == str(con_id)
                        and native.conId == con_id and native.secType == "OPT"):
                    proof = dict(source="IB_OPEN_ORDER_ASSET", basis="QUALIFIED_OPTION_CATALOG",
                        contract_id=con_id, contract=vars(native), qualified_at=binding.qualified_at)
                    proofs.append(await retain(proof))
                    types[con_id] = "OPT"
                    return "OPT"
            remaining = deadline - asyncio.get_running_loop().time()
            check(remaining > 0, "BAG asset qualification deadline elapsed")
            details = await asyncio.wait_for(ib.reqContractDetailsAsync(Contract(conId=con_id)), remaining)
            check(len(details) == 1 and details[0].contract.conId == con_id
                and bool(details[0].contract.secType), "BAG leg must qualify uniquely")
            native = details[0].contract
            proofs.append(await retain(dict(source="IB_OPEN_ORDER_ASSET", basis="IB_CONTRACT_DETAILS",
                contract_id=con_id, contract=vars(native), received_at=now_wire())))
            types[con_id] = native.secType
            return native.secType
        except Exception:
            # Metadata failure is not evidence of a non-option order. The
            # unknown classification below blocks admission; cancellation
            # still propagates as asyncio.CancelledError.
            types[con_id] = None
            return None

    for row in working["orders"]:
        if row["order"].get("whatIf") or row.get("state", {}).get("status") in TERMINAL_STATES:
            continue
        contract = row["contract"]
        asset = contract.get("secType")
        if asset == "OPT":
            selected.append(row)
            continue
        if asset in NON_OPTION_ASSETS:
            continue
        if asset != "BAG":
            reasons.add("NATIVE_OPEN_ORDER_ASSET_UNRESOLVED")
            continue
        legs = contract.get("comboLegs")
        if not isinstance(legs, list) or not legs:
            reasons.add("NATIVE_OPEN_ORDER_ASSET_UNRESOLVED")
            continue
        leg_types = {await asset_type(leg.get("conId")) for leg in legs if isinstance(leg, dict)}
        if len(leg_types) != 1 or len([leg for leg in legs if isinstance(leg, dict)]) != len(legs):
            reasons.add("NATIVE_OPEN_ORDER_ASSET_UNRESOLVED")
        elif leg_types == {"OPT"}:
            selected.append(row)
        elif not leg_types <= NON_OPTION_ASSETS:
            reasons.add("NATIVE_OPEN_ORDER_ASSET_UNRESOLVED")
    return selected, reasons, proofs
