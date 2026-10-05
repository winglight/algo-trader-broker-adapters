"""Index original account-activities bytes without float or JSON reserialization."""

from decimal import Decimal
import json

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import check
from algo_trader_broker_sdk.options_backfill import OptionActivityPageIndex, OptionActivitySpan

from .raw_stream import _constant, _object

SOURCE = "ALPACA_ACCOUNT_ACTIVITIES"


def index_activity_page(page):
    try:
        check(page.source == SOURCE and page.request.environment == "paper", "Unsupported activity source")
        text = page.raw_payload.decode("utf-8")
        decoder = json.JSONDecoder(parse_float=Decimal, parse_constant=_constant, object_pairs_hook=_object)
        size, pos, byte_pos, byte_pos_char, items = len(text), 0, 0, 0, []

        def whitespace(at):
            while at < size and text[at] in " \t\r\n": at += 1
            return at

        pos = whitespace(pos)
        check(pos < size and text[pos] == "[", "Native activity page must be an array")
        pos = whitespace(pos + 1)
        while pos < size and text[pos] != "]":
            check(len(items) < 100, "Native activity page exceeds requested limit")
            item, end = decoder.raw_decode(text, pos)
            check(type(item) is dict, "Native activity must be an object")
            # Advance UTF-8 offsets once, including separators/whitespace. This
            # retains the exact native substring, including numeric spelling.
            start_byte = byte_pos + len(text[byte_pos_char:pos].encode("utf-8"))
            end_byte = start_byte + len(text[pos:end].encode("utf-8"))
            items.append(OptionActivitySpan(item["id"], item["activity_type"], start_byte, end_byte))
            check(item["id"] != page.request.cursor, "Activity page echoed its exclusive cursor")
            byte_pos, byte_pos_char = end_byte, end
            pos = whitespace(end)
            check(pos < size, "Truncated native activity array")
            if text[pos] == "]": break
            check(text[pos] == ",", "Invalid native activity separator")
            pos = whitespace(pos + 1)
            check(pos < size and text[pos] != "]", "Trailing native activity separator")
        check(pos < size and text[pos] == "]" and whitespace(pos + 1) == size, "Trailing or truncated native activity bytes")
        return OptionActivityPageIndex(tuple(items), items[-1].activity_id if items else None)
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError) as exc:
        raise BrokerContractError("Native activity page remains unresolved") from exc
