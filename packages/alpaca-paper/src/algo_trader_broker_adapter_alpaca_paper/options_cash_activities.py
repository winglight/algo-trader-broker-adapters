"""Booked cash fills used to prove an admitted order's funds are reflected."""

import asyncio
from decimal import InvalidOperation
from hashlib import sha256
from collections import OrderedDict

from algo_trader_broker_sdk import BrokerConnectionError, BrokerContractError
from algo_trader_broker_sdk.options import check
from .account_identity import cash_execution

from .options_codec import _decimal, _time, _uuid
from .options_lifecycle import records
from .raw_stream import decode_native


# A current Account observation has a 30-second outer read deadline. Keep
# auxiliary history work separate from fresh funds/positions/open-order reads.
CASH_ACTIVITY_WORK_BUDGET_SECONDS = 5.0


async def read(adapter, scope, *, retain_evidence, advisory_reasons=None):
    bound, _, _ = await adapter._option_bound(scope)
    executions, seen, orders = [], set(), {}
    non_cash = getattr(adapter, '_option_non_cash_orders', None)
    if non_cash is None:
        non_cash = adapter._option_non_cash_orders = OrderedDict()
    identity = (bound.scope.account, bound.scope.profile_id, bound.scope.environment, bound.native_account_ref)

    async def retain(raw):
        reference = await retain_evidence(raw)
        check(reference == sha256(raw).hexdigest(), "Cash activity archive changed native evidence")
        adapter._option_still_bound(bound)
        return reference

    async def collect():
        cursor = None
        for _ in range(100):
            params = dict(activity_types="FILL", direction="desc", page_size=100)
            if cursor is not None:
                params["page_token"] = cursor
            raw = await adapter._backend.get_option_resource("/v2/account/activities", params=params)
            await retain(raw)
            rows = records(raw)
            check(all(item["activity_type"] == "FILL" and item["id"] not in seen for item, _ in rows),
                  "Cash activity pagination changed filter or repeated an identity")
            for item, original in rows:
                seen.add(item["id"])
                activity_ref = await retain(original)
                try:
                    order_id = _uuid(item["order_id"])
                except (BrokerContractError, KeyError, ValueError, TypeError):
                    continue
                known = (identity, order_id)
                if known in non_cash:
                    # An exact native order UUID cannot change from us_option to
                    # us_equity. This retained classification only excludes it
                    # from CASH fills; option execution/position evidence still
                    # comes from its own fresh journal and account observations.
                    non_cash.move_to_end(known)
                    continue
                if order_id not in orders:
                    try:
                        order_raw = await adapter._backend.get_option_resource("/v2/orders/" + order_id, params={"nested": "true"})
                    except BrokerConnectionError as exc:
                        if (exc.details.get('operation'), exc.details.get('resource_kind'),
                                exc.details.get('error_type'), exc.details.get('status_code')) != (
                                'option_trading_read', 'ORDER_DETAIL', 'HTTPStatusError', 404):
                            raise
                        # The original activity is already retained. A missing
                        # historical order leaves its asset/execution unknown;
                        # it cannot prove a cash fill or release a Risk claim.
                        # Do not cache this absence or classify it as an option.
                        if advisory_reasons is not None:
                            advisory_reasons.add('CASH_EXECUTION_ORDER_DETAIL_UNAVAILABLE')
                        continue
                    order_ref = await retain(order_raw)
                    orders[order_id] = decode_native(order_raw), order_ref
                order, order_ref = orders[order_id]
                try:
                    check(type(order) is dict and _uuid(order["id"]) == order_id
                          and order.get("account_id", bound.native_account_ref) == bound.native_account_ref,
                          "Cash activity order changed account or native identity")
                    # Option activities continue through the existing option journal.
                    if order.get("asset_class") != "us_equity":
                        if order.get('asset_class') == 'us_option':
                            non_cash[known] = True
                            if len(non_cash) > 1024:
                                non_cash.popitem(last=False)
                        continue
                    check(order.get("order_class") in {None, "", "simple"} and not order.get("legs")
                          and item.get("type") in {"fill", "partial_fill"}
                          and item.get("side") in {"buy", "sell"}
                          and item.get("symbol") == order.get("symbol") and item["side"] == order.get("side")
                          and item.get("currency", "USD") == "USD", "Cash activity economics are unresolved")
                    prefix, separator, execution = item["id"].rpartition("::")
                    check(bool(prefix) and separator == "::", "Retain the original FILL activity identity")
                    executions.append(cash_execution("ALPACA_ACCOUNT_FILL", item["id"], _uuid(execution),
                        order_id, "ALPACA:" + _uuid(order["asset_id"]), item["symbol"], item["side"].upper(),
                        _decimal(item["qty"]), _decimal(item["price"]), "USD", _time(item["transaction_time"]),
                        activity_ref, order_ref))
                except (BrokerContractError, KeyError, ValueError, TypeError, InvalidOperation):
                    # Missing/uninterpretable entries cannot satisfy Risk's exact
                    # per-fill match. Raw evidence remains available for reconciliation.
                    continue
            if len(rows) < 100:
                break
            cursor = rows[-1][0]["id"]

    budget = asyncio.timeout(CASH_ACTIVITY_WORK_BUDGET_SECONDS)
    try:
        async with budget:
            await collect()
    except TimeoutError:
        if not budget.expired():
            raise
        # Only this scan's deadline ends auxiliary history collection. Native
        # failures and caller cancellation retain their original semantics.
        if advisory_reasons is not None:
            advisory_reasons.add('CASH_EXECUTION_HISTORY_INCOMPLETE')
    adapter._option_still_bound(bound)
    # A bounded observation is not a claim of full native execution coverage.
    return tuple(executions)
