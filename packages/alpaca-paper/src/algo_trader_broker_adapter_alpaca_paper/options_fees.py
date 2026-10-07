"""Native USD account fees; no invented per-fill allocation or zero fee."""

from decimal import Decimal, InvalidOperation
from hashlib import sha256

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import check
from algo_trader_broker_sdk.options_account import NativeAccountFee

from .options_codec import _time, _uuid
from .options_lifecycle import records
from .options_reads import signed_decimal


async def read(adapter, scope, *, retain_evidence):
    bound, _, _ = await adapter._option_bound(scope)
    fees, cursor, seen, reasons = [], None, set(), {"FEE_SOURCE_RECONCILIATION_REQUIRED"}
    async def retain(raw):
        reference = await retain_evidence(raw)
        check(reference == sha256(raw).hexdigest(), "Fee archive changed raw evidence")
        adapter._option_still_bound(bound)
        return reference
    for _ in range(100):
        params = dict(activity_types="FEE", direction="desc", page_size=100)
        if cursor is not None: params["page_token"] = cursor
        raw = await adapter._backend.get_option_resource("/v2/account/activities", params=params)
        await retain(raw)
        rows = records(raw)
        check(all(item["activity_type"] == "FEE" and item["id"] != cursor for item, _ in rows), "Fee page changed filter or cursor")
        for item, original in rows:
            reference = await retain(original)
            try:
                # FEE is the documented USD fee type. Keep unrelated account
                # fees too; an execution reference does not prove option ownership.
                check(item.get("currency", "USD") == "USD" and item.get("status", "executed") == "executed", "Unsupported fee currency or state")
                effective = _time(item["transaction_time"]) if item.get("transaction_time") is not None else None
                day = item["date"] if effective is None else None
                native_execution = _uuid(item["execution_id"]) if item.get("execution_id") is not None else None
                cash = signed_decimal(Decimal(signed_decimal(item["net_amount"])).copy_negate())
                fees.append(NativeAccountFee("ALPACA_ACCOUNT_FEE", item["id"], item.get("activity_sub_type"), cash,
                    "USD", effective, day, native_execution, reference))
            except (BrokerContractError, KeyError, ValueError, TypeError, InvalidOperation):
                reasons.add("FEE_RECORDS_UNRESOLVED")
        if len(rows) < 100:
            cursor = None
            break
        cursor = rows[-1][0]["id"]
        check(cursor not in seen, "Fee pagination did not advance")
        seen.add(cursor)
    if cursor is not None: reasons.add("FEE_PAGE_INCOMPLETE")
    adapter._option_still_bound(bound)
    return tuple(fees), tuple(sorted(reasons))
