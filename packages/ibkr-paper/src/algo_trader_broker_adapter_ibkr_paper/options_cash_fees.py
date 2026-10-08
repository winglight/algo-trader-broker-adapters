"""Explicit account fees/credits in the selected Activity Flex cash section."""

from collections import Counter
from decimal import InvalidOperation
from hashlib import sha256

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import check
from algo_trader_broker_sdk.options_account import NativeAccountFee

from .flex import xml
from .options_account_native import raw_bytes
from .options_lifecycle import amount, day, number

SOURCE = "IB_FLEX_ACCOUNT_FEE"
FEE_TYPES = frozenset({"Other Fees", "Advisor Fees", "Client Fees", "Commission Adjustments", "Transaction Fees"})


async def parse(raw, *, account, retain):
    file_ref = await retain(raw)
    check(file_ref == sha256(raw).hexdigest(), "Flex archive changed account fee source")
    try:
        root = xml(raw)
        check(root.tag == "FlexQueryResponse" and root.get("type") == "AF", "Account fees require Activity Flex")
        statements = root.findall("FlexStatements/FlexStatement")
        matches = [item for item in statements if item.get("accountId") == account]
        check(len(matches) == 1, "Account fee statement is ambiguous")
        statement = matches[0]
        start, end = day(statement.get("fromDate")), day(statement.get("toDate"))
        check(start <= end, "Invalid account fee coverage")
        sections = statement.findall("CashTransactions")
        check(len(sections) == 1 and not sections[0].attrib
              and all(item.tag == "CashTransaction" and not len(item) for item in sections[0]),
              "Missing or invalid CashTransactions section")
        rows = [dict(item.attrib) for item in sections[0]]
        check(len(rows) <= 10000, "Cash transaction limit exceeded")
    except (BrokerContractError, ValueError, KeyError, TypeError):
        return (), ("IB_FLEX_CASH_SOURCE_INCOMPLETE:" + file_ref,)
    ids = Counter(row.get("transactionID") for row in rows)
    fees, reasons = [], []
    for index, row in enumerate(rows):
        if row.get("type") not in FEE_TYPES:
            continue
        try:
            identity = row["transactionID"]
            check(identity and len(identity) <= 128 and ids[identity] == 1,
                  "Account fees require a unique original transaction ID")
            check(row["accountId"] == account and row["currency"] == "USD", "Account fee scope or currency differs")
            # Flex dateTime is report-local. Preserve its broker date rather
            # than inventing a timezone or using file generation/receipt time.
            effective = day(row["dateTime"].split(";")[0].split("T")[0])
            check(start <= effective <= end, "Account fee is outside report coverage")
            cash = amount(number(row["amount"]).copy_negate())
            proof = raw_bytes(dict(source=SOURCE, account=account, cash_transaction=row))
            reference = await retain(proof)
            check(reference == sha256(proof).hexdigest(), "Flex archive changed cash fee proof")
            manifest = raw_bytes(dict(source=SOURCE, file_ref=file_ref, row=index, raw_ref=reference))
            check(await retain(manifest) == sha256(manifest).hexdigest(), "Flex archive changed cash fee manifest")
            # tradeID/conid are not a TWS execId. Preserve those in raw proof;
            # they must never create an automatic execution attribution.
            fees.append(NativeAccountFee(SOURCE, identity, row["type"], cash, "USD", None,
                                         effective, None, reference))
        except (BrokerContractError, ValueError, KeyError, TypeError, InvalidOperation):
            reasons.append("IB_FLEX_CASH_FEE_UNRESOLVED:" + file_ref + ":" + str(index))
    return tuple(fees), tuple(reasons)
