"""Native option catalog, quotes and account reads on the existing Paper client.

The P0 catalog is limited to standard SPY/QQQ equity deliverables. Successful
reads are evidence, not Paper certification or an execution authorization.
"""

from collections import OrderedDict, deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from uuid import uuid4

from algo_trader_broker_sdk import BrokerContractError
from algo_trader_broker_sdk.options import (
    BrokerOptionBinding, ContractPage, OptionContract, OptionContractKey, OptionContractQuery,
    OptionMarketSnapshot, OptionQuote, OptionScope, QualificationBatch, QualificationRequest,
    QualificationResult, SnapshotRequest, check, option_contract_id, timestamp,
)
from algo_trader_broker_sdk.options_account import (
    NativeBuyingPower, OptionAccountPermissions, OptionAccountPosition, OptionAccountState, UnresolvedOptionPosition,
)
from algo_trader_broker_sdk.options_capabilities import OptionCapabilities, OptionCapability, OptionShapeCapability

from .options_codec import _decimal, _time, _uuid
from .raw_stream import decode_native

ADAPTER_VERSION = "0.1.0"
STRUCTURES = ("LONG_CALL", "LONG_PUT", "CALL_DEBIT_VERTICAL", "PUT_DEBIT_VERTICAL", "CALL_CREDIT_VERTICAL", "PUT_CREDIT_VERTICAL")


def now_wire():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def signed_decimal(value):
    check(type(value) in (str, int, Decimal), "Native amount must retain exact precision")
    number = Decimal(value)
    absolute = _decimal(number.copy_abs())
    return "-" + absolute if number < 0 else absolute


def whole(value):
    number = Decimal(_decimal(value))
    check(number == number.to_integral_value() and number <= 2**53 - 1, "Native quantity must be whole")
    return int(number)


def contract_from_native(item):
    """Use explicit deliverables and multiplier, never an OCC-symbol guess."""
    check(type(item) is dict, "Missing native contract metadata")
    _uuid(item["id"])
    underlying = item["underlying_symbol"]
    check(underlying in {"SPY", "QQQ"} and item.get("root_symbol") == underlying, "Unsupported underlying or adjusted root")
    check(item["style"] == "american" and whole(item["multiplier"]) == 100 and whole(item["size"]) == 100,
          "Unsupported option economics")
    deliverables = item.get("deliverables")
    check(type(deliverables) is list and len(deliverables) == 1, "Complete standard deliverable evidence is required")
    delivery = deliverables[0]
    check(delivery["type"] == "equity" and delivery["symbol"] == underlying
        and _uuid(delivery["asset_id"]) == _uuid(item["underlying_asset_id"])
        and _decimal(delivery["amount"]) == "100" and _decimal(delivery["allocation_percentage"]) == "100"
        and delivery["delayed_settlement"] is False, "Contract is not a standard 100-share deliverable")
    check(item["type"] in {"call", "put"}, "Unsupported option right")
    check(type(item["symbol"]) is str and 0 < len(item["symbol"]) <= 64, "Native option symbol is missing")
    key = OptionContractKey(underlying, item["expiration_date"], "C" if item["type"] == "call" else "P",
        _decimal(item["strike_price"]), "USD", 100, "AMERICAN", "PHYSICAL", "STANDARD_100_SHARES")
    status = "ACTIVE" if item.get("status") == "active" and item.get("tradable") is True else "INACTIVE"
    return OptionContract(option_contract_id(key), key, None, None, None, status)


class AlpacaOptionReads:
    async def _option_bound(self, request):
        scope = OptionScope(**{name: getattr(request, name) for name in OptionScope.__dataclass_fields__})
        bound = self._option_account_binding
        check(bound is not None and bound.scope == scope and bound.native_account_ref == self._account_id
              and self._connected, "Option read requires the current verified Paper account")
        raw = await self._backend.get_option_resource("/v2/account")
        account = decode_native(raw)
        check(type(account) is dict and account.get("id") == bound.native_account_ref, "Native option account changed")
        self._option_still_bound(bound)
        self._remember_option_read(raw)
        return bound, account, raw

    def _option_still_bound(self, bound):
        check(self._option_account_binding == bound and self._connected and self._account_id == bound.native_account_ref,
              "Option account connection changed during native read")

    def _remember_option_read(self, raw):
        if not hasattr(self, "_option_read_evidence"):
            self._option_read_evidence = deque(maxlen=32)
        self._option_read_evidence.append(raw)
        return sha256(raw).hexdigest()

    def _remember_contract(self, scope, item, contract, raw, observed):
        binding = BrokerOptionBinding(self.adapter_id, ADAPTER_VERSION, scope.environment, scope.account,
            contract.canonical_id, item["id"], item["symbol"], None, observed, sha256(raw).hexdigest())
        if not hasattr(self, "_option_catalog"):
            self._option_catalog = OrderedDict()
        self._option_catalog[item["id"]] = (binding, contract, item)
        self._option_catalog.move_to_end(item["id"])
        while len(self._option_catalog) > 1000:
            self._option_catalog.popitem(last=False)
        return binding

    async def _contract_page(self, params):
        raw = await self._backend.get_option_resource("/v2/options/contracts", params={**params, "show_deliverables": "true"})
        page = decode_native(raw)
        check(type(page) is dict and type(page.get("option_contracts")) is list, "Malformed native contract page")
        cursor = page.get("next_page_token")
        check(cursor is None or (type(cursor) is str and 0 < len(cursor) <= 4096), "Invalid native catalog cursor")
        self._remember_option_read(raw)
        return page["option_contracts"], cursor, raw

    async def list_option_contracts(self, request):
        check(type(request) is OptionContractQuery, "Expected an exact contract query")
        bound, _, _ = await self._option_bound(request)
        check(request.underlying in {"SPY", "QQQ"}, "P0 supports SPY/QQQ contracts")
        params = dict(underlying_symbols=request.underlying, status="active", expiration_date_gte=request.expiry_from,
            expiration_date_lte=request.expiry_to, limit=request.limit)
        for key, value in (("type", {"C": "call", "P": "put"}.get(request.right)), ("strike_price_gte", request.strike_min),
                           ("strike_price_lte", request.strike_max), ("page_token", request.cursor)):
            if value is not None: params[key] = value
        items, cursor, raw = await self._contract_page(params)
        contracts, unsupported, observed = [], False, now_wire()
        for item in items:
            try:
                contract = contract_from_native(item)
            except (BrokerContractError, KeyError, ValueError, TypeError, InvalidOperation):
                unsupported = True
                continue
            check(contract.key.underlying == request.underlying and request.expiry_from <= contract.key.expiry <= request.expiry_to,
                  "Native catalog changed requested bounds")
            self._remember_contract(bound.scope, item, contract, raw, observed)
            contracts.append(contract)
        self._option_still_bound(bound)
        return ContractPage(tuple(contracts), cursor, cursor is None and not unsupported, observed)

    async def qualify_option_contracts(self, request):
        check(type(request) is QualificationRequest, "Expected an exact qualification request")
        bound, _, _ = await self._option_bound(request)
        results = []
        for expected in request.contracts:
            check(expected.canonical_id == option_contract_id(expected.key), "Requested contract identity does not match its economics")
            key = expected.key
            if (key.underlying not in {"SPY", "QQQ"} or (key.currency, key.multiplier, key.exercise_style, key.settlement, key.deliverable_id) !=
                    ("USD", 100, "AMERICAN", "PHYSICAL", "STANDARD_100_SHARES")):
                results.append(QualificationResult(expected.canonical_id, "UNSUPPORTED", None, None, ("NONSTANDARD_CONTRACT",)))
                continue
            matches, unresolved = [], False
            for status in ("active", "inactive"):
                cursor, seen = None, set()
                while True:
                    params = dict(underlying_symbols=key.underlying, expiration_date=key.expiry,
                        type="call" if key.right == "C" else "put", strike_price_gte=key.strike, strike_price_lte=key.strike,
                        status=status, limit=200)
                    if cursor is not None: params["page_token"] = cursor
                    items, cursor, raw = await self._contract_page(params)
                    for item in items:
                        try:
                            contract = contract_from_native(item)
                        except (BrokerContractError, KeyError, ValueError, TypeError, InvalidOperation):
                            unresolved = True
                            continue
                        if contract.key == key:
                            matches.append((item, contract, raw))
                    if cursor is None: break
                    check(cursor not in seen and len(seen) < 100, "Native qualification pagination did not converge")
                    seen.add(cursor)
                if matches: break
            if len(matches) == 1:
                item, contract, raw = matches[0]
                binding = self._remember_contract(bound.scope, item, contract, raw, now_wire())
                results.append(QualificationResult(expected.canonical_id, "EXACT", binding, contract, ()))
            else:
                result = "AMBIGUOUS" if matches else "UNSUPPORTED" if unresolved else "NOT_FOUND"
                results.append(QualificationResult(expected.canonical_id, result, None, None, ("CONTRACT_" + result,)))
        self._option_still_bound(bound)
        return QualificationBatch(tuple(results), now_wire())

    def _validate_option_bindings(self, bound, bindings):
        catalog = getattr(self, "_option_catalog", {})
        for binding in bindings:
            current = catalog.get(binding.broker_contract_id)
            check(current is not None and (current[0].canonical_id, current[0].local_symbol, current[0].adapter_id,
                  current[0].adapter_version, current[0].account_scope, current[0].environment) ==
                  (binding.canonical_id, binding.local_symbol, binding.adapter_id, binding.adapter_version, bound.scope.account, bound.scope.environment)
                  and 0 <= (datetime.now(timezone.utc) - timestamp(binding.qualified_at)).total_seconds() < 30,
                  "Option data requires the current exact native qualification")

    async def option_history(self, request):
        from .options_history import read_history
        return await read_history(self, request)

    def stream_option_quotes(self, request):
        from .options_stream import stream_quotes
        return stream_quotes(self, request)

    async def option_snapshot(self, request):
        check(type(request) is SnapshotRequest, "Expected an exact snapshot request")
        bound, _, _ = await self._option_bound(request)
        check(request.feed in {"OPRA", "INDICATIVE"}, "Options require an explicit OPRA or indicative feed")
        self._validate_option_bindings(bound, request.bindings)
        raw = await self._backend.get_option_resource("/v1beta1/options/snapshots", data=True,
            params=dict(symbols=",".join(binding.local_symbol for binding in request.bindings), feed=request.feed.lower(), limit=1000))
        data = decode_native(raw)
        check(type(data) is dict and type(data.get("snapshots")) is dict, "Malformed native option snapshot")
        self._remember_option_read(raw)
        received, quotes, missing = now_wire(), [], []
        def amount(value):
            try: return _decimal(value)
            except (BrokerContractError, ValueError, TypeError, InvalidOperation): return None
        def size(value):
            try: return whole(value)
            except (BrokerContractError, ValueError, TypeError, InvalidOperation): return None
        for binding in request.bindings:
            item = data["snapshots"].get(binding.local_symbol, {})
            quote = item.get("latestQuote") if type(item) is dict else None
            if type(quote) is not dict or not quote.get("t"):
                missing.append(binding.canonical_id)
                continue
            at = _time(quote["t"])
            bid, ask, bs, az = amount(quote.get("bp")), amount(quote.get("ap")), size(quote.get("bs")), size(quote.get("as"))
            age = (timestamp(received) - timestamp(at)).total_seconds() * 1000
            quality = "EXECUTABLE"
            if None in (bid, ask, bs, az): quality = "MISSING"
            elif Decimal(bid) > Decimal(ask): quality = "CROSSED"
            elif age < 0 or age > request.max_age_ms: quality = "STALE"
            elif request.feed == "INDICATIVE": quality = "RESEARCH_ONLY"
            # Native snapshot Greeks have no independent input timestamp or
            # verified unit evidence. Retain them raw, without fabricating one.
            quotes.append(OptionQuote(binding.canonical_id, bid, ask, bs, az, at, received, "ALPACA", request.feed,
                quality, bound.scope.account, None, None, None, None, None, None, None, None, None))
        times = [timestamp(quote.quote_at) for quote in quotes]
        skew = int((max(times) - min(times)).total_seconds() * 1000) if times else 0
        self._option_still_bound(bound)
        if request.feed == "OPRA": self._option_opra_observed = (bound.scope, received)
        return OptionMarketSnapshot(str(uuid4()), tuple(quotes), tuple(missing), not missing and not data.get("next_page_token"), skew, received)

    async def option_account_permissions(self, request):
        bound, account, raw = await self._option_bound(request)
        observed = now_wire()
        levels = (account.get("options_approved_level"), account.get("options_trading_level"))
        known = all(type(level) is int and level >= 0 for level in levels)
        level = min(levels) if known else None
        approval = "UNKNOWN" if level is None else "ALLOWED" if level > 0 else "DENIED"
        flags = [account.get(name) for name in ("trading_blocked", "account_blocked", "trade_suspended_by_user")]
        trading = "DENIED" if account.get("status") != "ACTIVE" or any(flag is True for flag in flags) else "ALLOWED" if all(flag is False for flag in flags) else "UNKNOWN"
        opra = getattr(self, "_option_opra_observed", None)
        entitled = opra is not None and opra[0] == bound.scope and 0 <= (timestamp(observed) - timestamp(opra[1])).total_seconds() < 30
        structures = STRUCTURES if level is not None and level >= 3 else STRUCTURES[:2] if level == 2 else ()
        bp = "OPTIONS_BUYING_POWER" if account.get("options_buying_power") is not None else "UNKNOWN"
        return OptionAccountPermissions(bound.scope, approval, None if level is None else str(level), structures,
            trading, "ALLOWED" if entitled else "UNKNOWN", "UNKNOWN", bp, observed,
            (timestamp(observed) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z"), sha256(raw).hexdigest(),
            ("LIFECYCLE_NOT_CERTIFIED",) + (() if entitled else ("OPRA_ENTITLEMENT_UNOBSERVED",)))

    async def option_capabilities(self, request):
        bound, _, _ = await self._option_bound(request)
        observed = now_wire()
        implemented = OptionCapability("IMPLEMENTED", ("ACCOUNT_CERTIFICATION_REQUIRED",), ())
        unavailable = OptionCapability("UNSUPPORTED", ("NOT_IMPLEMENTED",), ())
        no_history_quotes = OptionCapability("UNSUPPORTED", ("PROVIDER_HISTORY_QUOTES_UNAVAILABLE",), ())
        return OptionCapabilities(bound.scope, self.adapter_id, ADAPTER_VERSION, "alpaca-options-reads-2", observed,
            (timestamp(observed) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z"), implemented, implemented,
            unavailable, implemented, no_history_quotes, implemented, unavailable, unavailable, unavailable, implemented,
            unavailable, ("OPRA", "INDICATIVE"), tuple(OptionShapeCapability(shape, "NATIVE", implemented,
                1 if shape.startswith("LONG_") else 2, 1, None, False, not shape.startswith("LONG_"), False, False) for shape in STRUCTURES))

    async def option_account_state(self, request):
        bound, account, account_raw = await self._option_bound(request)
        check(account.get("currency") == "USD", "Option account currency must be explicit USD")
        position_raw = await self._backend.get_option_resource("/v2/positions")
        order_raw = await self._backend.get_option_resource("/v2/orders", params=dict(status="open", limit=500, nested="true"))
        positions, orders = decode_native(position_raw), decode_native(order_raw)
        check(type(positions) is list and type(orders) is list, "Native account collections are invalid")
        pos_hash = self._remember_option_read(position_raw)
        self._remember_option_read(order_raw)
        exact, unresolved, observed = [], [], now_wire()
        for position in positions:
            asset_class = position.get("asset_class")
            if asset_class in {"us_equity", "crypto"}: continue
            native_id, symbol = _uuid(position["asset_id"]), position["symbol"]
            quantity = signed_decimal(position["qty"])
            if position.get("side") == "short" and Decimal(quantity) > 0: quantity = "-" + quantity
            reason = "CONTRACT_METADATA_UNRESOLVED"
            try:
                check(asset_class == "us_option", "Unclassified native asset")
                raw = await self._backend.get_option_resource("/v2/options/contracts/" + native_id)
                self._remember_option_read(raw)
                item = decode_native(raw)
                contract = contract_from_native(item)
                check(item["id"] == native_id and item["symbol"] == symbol, "Position and native contract disagree")
                contracts = whole(Decimal(quantity).copy_abs()) * (-1 if Decimal(quantity) < 0 else 1)
                check(position.get("side") == ("short" if contracts < 0 else "long"), "Position direction is invalid")
                binding = self._remember_contract(bound.scope, item, contract, raw, observed)
                cost = None if position.get("cost_basis") is None else signed_decimal(position["cost_basis"])
                unit = "CASH_TOTAL" if cost is not None and Decimal(cost) * contracts >= 0 else "UNKNOWN"
                exact.append(OptionAccountPosition(contract, binding, native_id, contracts, cost, unit, pos_hash, pos_hash, observed, observed))
            except (BrokerContractError, KeyError, ValueError, TypeError, InvalidOperation):
                unresolved.append(UnresolvedOptionPosition(native_id, native_id, asset_class or "UNKNOWN", quantity,
                    pos_hash, pos_hash, (reason,)))
        raw_bp = tuple(NativeBuyingPower(name, signed_decimal(account[name])) for name in (
            "buying_power", "regt_buying_power", "daytrading_buying_power", "non_marginable_buying_power", "options_buying_power") if account.get(name) is not None)
        bp = None if account.get("options_buying_power") is None else signed_decimal(account["options_buying_power"])
        open_refs = tuple(_uuid(order["id"]) for order in orders)
        received = now_wire()
        self._option_still_bound(bound)
        checkpoint = sha256(account_raw + position_raw + order_raw).hexdigest()
        reasons = ("EXECUTION_RECONCILIATION_REQUIRED", "LIFECYCLE_RECONCILIATION_REQUIRED")
        if len(orders) >= 500: reasons += ("OPEN_ORDER_PAGE_INCOMPLETE",)
        if unresolved: reasons += ("UNRESOLVED_OPTION_POSITIONS",)
        return OptionAccountState(bound.scope, str(uuid4()), observed, received, "USD", signed_decimal(account["equity"]),
            signed_decimal(account["cash"]), bp, raw_bp, "OPTIONS_BUYING_POWER" if bp is not None else "UNKNOWN",
            checkpoint, tuple(exact), tuple(unresolved), open_refs, (), True, len(orders) < 500, False, False, reasons)
