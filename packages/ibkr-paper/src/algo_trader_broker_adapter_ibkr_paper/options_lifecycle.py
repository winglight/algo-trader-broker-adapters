"""Explicit Flex lifecycle rows and their broker-linked stock delivery."""

from collections import Counter
from dataclasses import replace
from datetime import date
from decimal import Decimal, InvalidOperation, localcontext
from hashlib import sha256

from algo_trader_broker_sdk import BrokerCapabilityError, BrokerConnectionError, BrokerContractError
from algo_trader_broker_sdk.options import ActivityQuery, OptionActivityPage, OptionLifecycleEvent, QualificationResult, check

from .flex import xml
from .options_account_native import raw_bytes
from .options_reads import now_wire

PARSER = "ib-flex-eae-v1"
KINDS = {"Assignment": "ASSIGNMENT", "Exercise": "EXERCISE", "Expiration": "EXPIRATION"}


def day(value):
    check(type(value) is str, "Missing Flex date")
    if len(value) == 8 and value.isdecimal():
        value = value[:4] + "-" + value[4:6] + "-" + value[6:]
    check(date.fromisoformat(value).isoformat() == value, "Invalid Flex date")
    return value


def number(value):
    check(type(value) is str and 0 < len(value) <= 64, "Missing Flex numeric field")
    result = Decimal(value)
    check(result.is_finite() and abs(result) < Decimal("1e20") and result.as_tuple().exponent >= -12,
          "Invalid Flex numeric field")
    return result


def whole(value):
    result = number(value)
    check(result == result.to_integral_value(), "Flex lifecycle quantity must be whole")
    return int(result)


def amount(value):
    if value == 0:
        return "0"
    wire = format(value, "f")
    return wire.rstrip("0").rstrip(".") if "." in wire else wire


def document(raw, account, accounts):
    root = xml(raw)
    check(root.tag == "FlexQueryResponse" and root.get("type") == "AF", "An Activity Flex statement is required")
    containers = root.findall("FlexStatements")
    check(len(containers) == 1, "Missing FlexStatements section")
    statements = list(containers[0])
    check(str(len(statements)) == containers[0].get("count") and
          all(item.tag == "FlexStatement" and item.get("accountId") in accounts for item in statements),
          "Flex statement account or count differs from configuration")
    matches = [item for item in statements if item.get("accountId") == account]
    check(len(matches) == 1, "Flex account must have exactly one statement")
    statement = matches[0]
    start, end = day(statement.get("fromDate")), day(statement.get("toDate"))
    check(start <= end and bool(statement.get("whenGenerated")), "Invalid Flex statement coverage")
    sections = [item for item in statement if item.tag == "OptionEAE"]
    check(len(sections) == 1 and not sections[0].attrib and
          all(item.tag == "OptionEAE" and not len(item) for item in sections[0]), "Missing or invalid OptionEAE section")
    rows = [dict(item.attrib) for item in sections[0]]
    check(len(rows) <= 10000 and all(item.get("accountId") == account for item in rows), "Flex lifecycle row count or account is invalid")
    # A selected empty section is different from an omitted report section.
    required = {"Trades", "OpenPositions", "CashTransactions"}
    missing = required - {item.tag for item in statement}
    metadata = {**statement.attrib, "_query_name": root.get("queryName"), "_from_date": start, "_to_date": end}
    return metadata, start, end, rows, missing


async def parse(raw, *, account, accounts, resolve_contract, retain, observed=None, statement_store=None):
    file_ref = await retain(raw)
    check(file_ref == sha256(raw).hexdigest(), "Flex archive changed original statement")
    observed = observed or now_wire()
    try:
        metadata, start, end, rows, missing = document(raw, account, accounts)
    except (BrokerContractError, ValueError, KeyError, TypeError):
        return OptionActivityPage((), None, False, observed, ("LIFECYCLE_SOURCE_INCOMPLETE:" + file_ref,))
    unresolved = ["LIFECYCLE_SOURCE_INCOMPLETE:" + name for name in sorted(missing)]
    # Every original row remains addressable by file hash + ordinal. Requiring
    # a unique native trade ID prevents ambiguous linkage and double booking.
    ids = [row.get("tradeID") for row in rows]
    duplicate = {value for value, count in Counter(ids).items() if count > 1}
    deliveries, related = {}, {}
    for i, item in enumerate(rows):
        if item.get("assetCategory") == "STK":
            deliveries.setdefault(item.get("tradeID"), []).append(i)
            if item.get("relatedTradeID"):
                related.setdefault(item["relatedTradeID"], []).append(i)
    events, consumed = [], set()
    for index, row in enumerate(rows):
        if row.get("assetCategory") == "STK":
            continue
        try:
            identity = row["tradeID"]
            check(identity and len(identity) <= 128 and identity not in duplicate, "Ambiguous Flex trade ID")
            kind = KINDS[row["transactionType"]]
            effective = day(row["date"])
            check(start <= effective <= end and row["assetCategory"] == "OPT" and row["currency"] == "USD",
                  "Flex option event is outside report coverage")
            check(row["conid"].isdecimal() and int(row["conid"]) > 0 and
                  row["underlyingConid"].isdecimal() and int(row["underlyingConid"]) > 0, "Invalid Flex native contract IDs")
            # The locked EAE layout reports signed position changes. Do not
            # take abs(quantity) or infer direction from the display symbol.
            delta = whole(row["quantity"])
            check(delta != 0 and (kind != "ASSIGNMENT" or delta > 0) and (kind != "EXERCISE" or delta < 0),
                  "Flex option position change has an invalid direction")
            check(number(row["tradePrice"]) == number(row["proceeds"]) == 0,
                  "Unexpected option lifecycle premium")
            number(row["commisionsAndTax"])
            underlying, expiry, right, strike = row["underlyingSymbol"], day(row["expiry"]), row["putCall"], number(row["strike"])
            strike_code = strike * 1000
            check(underlying in {"SPY", "QQQ"} and right in {"C", "P"} and strike_code == int(strike_code)
                  and 0 < strike_code < 100000000 and whole(row["multiplier"]) == 100, "Nonstandard Flex option contract")
            symbol = f"{underlying:<6}{expiry.replace('-', '')[2:]}{right}{int(strike_code):08d}"
            resolved = await resolve_contract(row["conid"], symbol)
            check(type(resolved) is QualificationResult and resolved.status == "EXACT" and resolved.contract is not None
                  and resolved.binding is not None, "Flex lifecycle needs retained exact qualification")
            contract, binding = resolved.contract, resolved.binding
            key = contract.key
            check(binding.broker_contract_id == row["conid"] and binding.local_symbol == symbol
                  and row["symbol"].replace(" ", "") == symbol.replace(" ", "")
                  and (key.underlying, key.expiry, key.right, Decimal(key.strike), key.multiplier, key.currency) ==
                  (underlying, expiry, right, strike, 100, "USD"), "Flex contract differs from exact qualification")
            check(kind != "EXPIRATION" or effective == expiry, "Expiration date differs from contract")
            shares, cash, delivery_index = 0, Decimal(0), None
            if kind != "EXPIRATION":
                linked = set(related.get(identity, ()))
                if row.get("relatedTradeID"):
                    linked.update(deliveries.get(row["relatedTradeID"], ()))
                check(len(linked) == 1, "An explicit unique Flex delivery link is required")
                delivery_index = next(iter(linked))
                delivery = rows[delivery_index]
                check(delivery_index not in consumed and delivery["tradeID"] not in duplicate and
                      delivery.get("relatedTradeID", identity) == identity and
                      row.get("relatedTradeID", delivery["tradeID"]) == delivery["tradeID"] and
                      delivery["accountId"] == account and delivery["currency"] == "USD" and
                      delivery["conid"] == row["underlyingConid"] and delivery["symbol"] == underlying and
                      day(delivery["date"]) == effective, "Flex delivery identity differs")
                shares, cash = whole(delivery["quantity"]), number(delivery["proceeds"])
                check(delivery["transactionType"] == ("Buy" if shares > 0 else "Sell") and
                      shares == -delta * 100 * (1 if right == "C" else -1) and number(delivery["tradePrice"]) == strike,
                      "Flex delivery direction, quantity or price differs")
                with localcontext() as context:
                    context.prec = 100
                    check(cash == -Decimal(shares) * strike, "Flex delivery proceeds differ from strike economics")
                number(delivery["commisionsAndTax"])
            proof = raw_bytes(dict(source="IB_FLEX_EAE", parser=PARSER, account=account, option_row=row,
                delivery_row=None if delivery_index is None else rows[delivery_index]))
            raw_ref = await retain(proof)
            check(raw_ref == sha256(proof).hexdigest(), "Flex archive changed row proof")
            # Repeated rows in a rolling report keep their first-class raw
            # identity. A separate manifest links every observation to its file.
            manifest = raw_bytes(dict(parser=PARSER, file_ref=file_ref, statement=metadata, raw_ref=raw_ref,
                option_row_index=index, delivery_row_index=delivery_index))
            check(await retain(manifest) == sha256(manifest).hexdigest(), "Flex archive changed statement manifest")
            event_id = "IB_FLEX:" + sha256((account + ":" + identity).encode()).hexdigest()
            events.append(OptionLifecycleEvent(event_id, kind, contract.canonical_id,
                delta, underlying, shares, amount(cash), "USD", None, observed, "IB_FLEX_EAE", 1, None, raw_ref,
                effective_date=effective))
            if delivery_index is not None:
                consumed.add(delivery_index)
        except (BrokerContractError, KeyError, ValueError, TypeError, InvalidOperation):
            unresolved.append(file_ref + ":" + str(index))
    unresolved.extend(file_ref + ":" + str(i) for i, row in enumerate(rows)
        if row.get("assetCategory") == "STK" and i not in consumed)
    page = OptionActivityPage(tuple(events), None, not unresolved, observed, tuple(unresolved))
    if statement_store is not None:
        from .options_flex_trades import cancellations
        try:
            proofs, trade_unresolved = cancellations(raw, account)
        except (BrokerContractError, ValueError, KeyError, TypeError):
            proofs, trade_unresolved = (), ("FLEX_TRADES_INVALID:" + file_ref,)
        if trade_unresolved:
            page = replace(page, complete=False,
                unresolved_refs=tuple(sorted(set(page.unresolved_refs + trade_unresolved))))
        result = await statement_store.record(raw, metadata, page)
        trade_unresolved = ()
        if proofs:
            trade_unresolved += tuple(await statement_store.trade_cancellations(raw, proofs))
        if trade_unresolved:
            return replace(result, complete=False,
                unresolved_refs=tuple(sorted(set(result.unresolved_refs + trade_unresolved))))
        return result
    return page


async def read(adapter, request, *, retain_evidence, resolve_contract=None, state_store=None):
    check(type(request) is ActivityQuery and retain_evidence is not None,
          "Flex lifecycle requires a scoped query and durable archive")
    async def operation(ib, bound):
        reader = adapter._flex
        async def resolve(native, symbol):
            if resolve_contract is not None:
                return await resolve_contract(native, symbol)
            entry = adapter._option_catalog.get(native)
            check(entry is not None and entry[0].local_symbol == symbol, "Flex contract qualification is unavailable")
            return QualificationResult(entry[1].canonical_id, "EXACT", entry[0], entry[1], ())
        # Cursor pages refer to immutable parsed statements within this bound
        # connection, not to a new HTTP query with a moving report window.
        if state_store is not None and request.cursor is not None:
            page = await state_store.page(request.cursor, request.limit)
            check(page is not None, "Flex cursor has no persisted report")
            return page
        if request.cursor is not None:
            saved = adapter._option_flex_pages.get(request.cursor)
            check(saved is not None and saved[0] == (bound.scope, request.since), "Flex cursor expired or changed scope")
            page, offset = saved[1:]
        else:
            try:
                raw = await reader.fetch(bound.native_account_ref, retain_evidence, state_store=state_store)
            except BrokerCapabilityError as exc:
                if state_store is not None and exc.code == "IB_FLEX_UNCONFIGURED":
                    persisted = await state_store.page(None, request.limit)
                    if persisted is not None:
                        return persisted
                return OptionActivityPage((), None, False, now_wire(), (exc.code,))
            except BrokerConnectionError:
                return OptionActivityPage((), None, False, now_wire(), ("IB_FLEX_UNAVAILABLE",))
            if raw is None:
                return OptionActivityPage((), None, False, now_wire(), ("IB_FLEX_PENDING",))
            page = await parse(raw, account=bound.native_account_ref, accounts=reader.accounts,
                resolve_contract=resolve, retain=retain_evidence, statement_store=state_store)
            if state_store is not None:
                persisted = await state_store.page(None, request.limit)
                if persisted is not None:
                    return replace(persisted, complete=persisted.complete and page.complete,
                        unresolved_refs=tuple(sorted(set(persisted.unresolved_refs + page.unresolved_refs))))
            # since is a creation-time query in this API. Flex supplies economic
            # dates; replay the bounded statement rather than silently lose late rows.
            offset = 0
        events = page.events[offset:offset + request.limit]
        cursor = None
        if offset + len(events) < len(page.events):
            from uuid import uuid4
            cursor = str(uuid4())
            adapter._option_flex_pages[cursor] = ((bound.scope, request.since), page, offset + len(events))
            while len(adapter._option_flex_pages) > 64:
                adapter._option_flex_pages.popitem(last=False)
        return OptionActivityPage(events, cursor, page.complete and cursor is None, page.observed_at, page.unresolved_refs)
    return await adapter._option_read(request, operation)


async def import_statement(adapter, scope, raw, *, retain_evidence, resolve_contract, state_store):
    check(retain_evidence is not None and resolve_contract is not None and state_store is not None,
          "Flex import requires durable evidence, qualifications and statement storage")
    async def operation(ib, bound):
        accounts = adapter._flex.accounts or frozenset({bound.native_account_ref})
        check(bound.native_account_ref in accounts, "Flex account is outside the configured allowlist")
        # Invalid XML is retained for audit but never becomes a financial event.
        check(await retain_evidence(raw) == sha256(raw).hexdigest(), "Flex archive changed source file")
        try:
            document(raw, bound.native_account_ref, accounts)
        except (ValueError, KeyError, TypeError):
            raise BrokerContractError("Invalid Flex statement metadata") from None
        return await parse(raw, account=bound.native_account_ref, accounts=accounts,
            resolve_contract=resolve_contract, retain=retain_evidence, statement_store=state_store)
    return await adapter._option_read(scope, operation)
