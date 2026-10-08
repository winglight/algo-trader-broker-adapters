"""Account-scoped STK execution evidence for cash reservation reflection."""

from decimal import InvalidOperation

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import check, timestamp
from algo_trader_broker_sdk.options_account import NativeCashExecution

from .options_events import execution_revision
from .options_reads import native_decimal


async def collect(account, working, completed, executions, *, retain):
    orders, families = {}, {}
    for page in (working, completed):
        await retain(page)
        for row in page["orders"]:
            if row["contract"]["secType"] == "STK":
                orders.setdefault(row["order"]["permId"], []).append(row)
    await retain(executions)
    for row in executions["executions"]:
        if row["contract"]["secType"] != "STK":
            continue
        ref = await retain(dict(source="IB_ACCOUNT_EXECUTION", account=account, observation=row))
        revision = execution_revision(row["execution"]["execId"])
        families.setdefault(revision.original_execution_id, []).append((revision.revision, row, ref))

    result = []
    for versions in families.values():
        # Corrections supersede their original execution. An unresolved newest
        # revision must never fall back to an older, apparently valid fill.
        _, row, activity_ref = max(versions, key=lambda item: item[0])
        native, contract = row["execution"], row["contract"]
        try:
            identity = lambda item: (item["execution"]["permId"], item["execution"]["orderRef"],
                item["execution"]["side"], item["contract"]["conId"], item["contract"]["symbol"])
            check(all(identity(other) == identity(row) for _, other, _ in versions),
                  "Cash execution revision changed its original order or asset")
            check(all(other["execution"] == native and other["contract"] == contract
                      for version, other, _ in versions if version == max(v[0] for v in versions)),
                  "Conflicting native cash execution revision")
            check(native["acctNumber"] == account and not native["modelCode"]
                  and not native["pendingPriceRevision"] and native["side"] in {"BOT", "SLD"}
                  and type(native["permId"]) is int and native["permId"] > 0
                  and type(contract["conId"]) is int and contract["conId"] > 0
                  and contract["currency"] == "USD" and bool(native["orderRef"]),
                  "Cash execution has unresolved native economics")
            matches = orders.get(native["permId"], [])
            check(bool(matches), "Cash execution has no original native order")
            side = "BUY" if native["side"] == "BOT" else "SELL"
            for match in matches:
                order, asset = match["order"], match["contract"]
                check(order["account"] == account and order["orderRef"] == native["orderRef"]
                      and order["action"] == side and not order["modelCode"] and not order["whatIf"]
                      and asset["conId"] == contract["conId"] and asset["symbol"] == contract["symbol"]
                      and asset["currency"] == "USD", "Cash execution differs from its original order")
            timestamp(native["time"])
            order_ref = await retain(dict(source="IB_CASH_EXECUTION_ORDER", account=account, observation=matches[-1]))
            result.append(NativeCashExecution("IB_ACCOUNT_EXECUTION", native["execId"], native["execId"],
                "IBKR_PERM_ID:" + str(native["permId"]), "IBKR:" + str(contract["conId"]),
                contract["symbol"], side, native_decimal(native["shares"]), native_decimal(native["price"]),
                "USD", native["time"], activity_ref, order_ref))
        except (BrokerContractError, KeyError, ValueError, TypeError, InvalidOperation):
            # Raw observations remain archived, but cannot release a reservation.
            continue
    return tuple(result)
