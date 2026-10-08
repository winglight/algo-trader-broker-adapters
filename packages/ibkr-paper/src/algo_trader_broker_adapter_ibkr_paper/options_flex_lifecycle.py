"""Explicit Trades reversals of the exact EAE option and delivery rows."""

from dataclasses import replace
from hashlib import sha256

from algo_trader_broker_sdk import dataclass_to_payload
from algo_trader_broker_sdk.options import check

from .options_account_native import raw_bytes
from .options_lifecycle import day, number, whole


async def reverse(raw, account, event, option, delivery, retain, statement_store, sections):
    if not sections:
        return event, set()
    check(len(sections) == 1 and not sections[0].attrib and len(sections[0]) <= 10000,
          "Lifecycle reversal requires selected execution detail")
    trades = list(sections[0])
    originals = [option] + ([] if delivery is None else [delivery])
    ids = {item["tradeID"] for item in originals}
    reversals = [item for item in trades if item.get("origTradeID") in ids]
    if not reversals:
        return event, set()
    check(statement_store is not None, "Lifecycle reversal requires retained original revisions")
    check(len(reversals) == len(originals), "Lifecycle reversal must include the option and its delivery")
    proof_rows = []
    for original in originals:
        matches = [item for item in reversals if item.get("origTradeID") == original["tradeID"]]
        check(len(matches) == 1, "Lifecycle reversal reference is ambiguous")
        item = matches[0]
        row = item.attrib
        check(item.tag == "Trade" and not len(item) and row["accountId"] == account
              and row["currency"] == original["currency"] == "USD"
              and row["levelOfDetail"] == "EXECUTION" and row["assetCategory"] == original["assetCategory"]
              and row["conid"] == original["conid"] and row["symbol"] == original["symbol"]
              and row["tradeID"] not in ids and bool(row["tradeID"])
              and sum(other.get("tradeID") == row["tradeID"] for other in trades) == 1,
              "Lifecycle reversal changed economic identity")
        quantity = whole(original["quantity"])
        check(whole(row["quantity"]) == -quantity
              and row["buySell"] == ("BUY" if quantity < 0 else "SELL")
              and day(row["origTradeDate"]) == day(original["date"])
              and number(row["origTradePrice"]) == number(original["tradePrice"])
              and number(row["tradePrice"]) == number(original["tradePrice"])
              and number(row["proceeds"]) == -number(original["proceeds"]),
              "Lifecycle reversal must exactly reverse the original quantity and proceeds")
        if original["assetCategory"] == "OPT":
            check(whole(row["multiplier"]) == whole(original["multiplier"]) == 100,
                  "Lifecycle reversal changed option multiplier")
        proof_rows.append(dict(row=row, index=trades.index(item)))
    proof = raw_bytes(dict(source="IB_FLEX_EAE_REVERSAL", file_ref=sha256(raw).hexdigest(),
        account=account, original_raw_ref=event.raw_ref, reversals=proof_rows,
        original_economics={key: value for key, value in dataclass_to_payload(event).items()
            if key not in {"revision", "correction_of_revision", "observed_at", "raw_ref"}}))
    raw_ref = await retain(proof)
    check(raw_ref == sha256(proof).hexdigest(), "Flex archive changed reversal proof")
    return replace(event, signed_option_contracts_delta=0, delivered_shares=0, cash_delta="0", raw_ref=raw_ref), ids
