"""Explicit Activity Flex cancellations; absence never cancels an execution.

IB's Trades reference defines origTradeID as the original cancelled trade.
Only execution detail with a unique original row and reversing quantity is
supported here. Retained TWS evidence must independently match before posting.
"""

from decimal import InvalidOperation
from hashlib import sha256

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import check

from .flex import xml
from .options_events import execution_revision
from .options_lifecycle import amount, day, number, whole


def cancellations(raw, account):
    statement = next(item for item in xml(raw).find("FlexStatements") if item.get("accountId") == account)
    sections = statement.findall("Trades")
    if not sections:
        return (), ()
    check(len(sections) == 1 and not sections[0].attrib and
          len(sections[0]) <= 10000, "Invalid Flex Trades section")
    rows = list(sections[0])
    proofs, unresolved = [], []
    file_hash = sha256(raw).hexdigest()
    for index, item in enumerate(rows):
        row = item.attrib
        if row.get("assetCategory") != "OPT" or row.get("origTradeID", "") in {"", "0"}:
            continue
        try:
            check(item.tag == "Trade" and not len(item) and row["accountId"] == account
                  and row["currency"] == "USD" and row["levelOfDetail"] == "EXECUTION",
                  "Cancellation requires original option execution detail")
            matches = [(i, other.attrib) for i, other in enumerate(rows)
                if other.tag == "Trade" and other.get("tradeID") == row["origTradeID"]
                and other.get("levelOfDetail") == "EXECUTION"
                and other.get("origTradeID", "") in {"", "0"}
                and (not row.get("origTransactionID") or other.get("transactionID") == row["origTransactionID"])]
            check(len(matches) == 1, "Original cancelled trade is missing or ambiguous")
            original_index, original = matches[0]
            check(original["accountId"] == account and original["assetCategory"] == "OPT"
                  and original["currency"] == "USD" and original["conid"] == row["conid"]
                  and original["symbol"] == row["symbol"]
                  and whole(original["multiplier"]) == whole(row["multiplier"]) == 100
                  and original["conid"].isascii() and original["conid"].isdecimal()
                  and int(original["conid"]) > 0, "Cancelled economic contract differs")
            quantity = whole(original["quantity"])
            check(quantity != 0 and whole(row["quantity"]) == -quantity
                  and original["buySell"] == ("BUY" if quantity > 0 else "SELL"),
                  "Cancellation must explicitly reverse the original quantity")
            price = number(original["tradePrice"])
            check(price >= 0 and number(row["origTradePrice"]) == price
                  and day(row["origTradeDate"]) == day(original["tradeDate"]),
                  "Cancellation changed original price or trade date")
            execution = original["ibExecID"]
            revision = execution_revision(execution)
            revision.check_native_id(execution)
            proofs.append(dict(original_execution_id=execution,
                execution_family=revision.original_execution_id, native_revision=revision.revision,
                native_contract_id=original["conid"], side=original["buySell"], contracts=abs(quantity),
                price=amount(price), multiplier=100, original_trade_date=day(original["tradeDate"]),
                original_trade_id=row["origTradeID"], cancellation_row=index, original_row=original_index))
        except (BrokerContractError, KeyError, ValueError, TypeError, InvalidOperation):
            unresolved.append("FLEX_TRADE_CANCEL_REVIEW:" + file_hash + ":" + str(index))
    return tuple(proofs), tuple(unresolved)
