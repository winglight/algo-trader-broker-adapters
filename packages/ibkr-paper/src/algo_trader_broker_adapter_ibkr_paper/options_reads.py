"""Scoped IB option discovery and quotes on the adapter's existing connection.

Economics use exact native standard OCC identities. IB does not
return a full deliverable record; adjusted roots and nonstandard classes are
excluded. A read is not account permission, calendar or trading certification.
"""

import asyncio
from collections import OrderedDict, deque, namedtuple
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import re
from uuid import uuid4

from algo_trader_broker_sdk import BrokerConnectionError, BrokerContractError
from algo_trader_broker_sdk.options import (
    BrokerOptionBinding, ContractPage, OptionContract, OptionContractKey, OptionContractQuery,
    OptionScope, OptionVerifiedAccount, QualificationBatch,
    QualificationRequest, QualificationResult, SnapshotRequest, check, decimal_wire,
    option_contract_id, timestamp,
)
from algo_trader_broker_sdk.options_capabilities import OptionCapabilities, OptionCapability, OptionShapeCapability

from ._version import VERSION
STANDARD_RULE = "ib-spy-qqq-standard-occ-v1"
STANDARD_SOURCE = "https://www.cboe.com/exchange-traded-stock/etp-options-spec"
STANDARD_CHECKED_AT = "2026-10-08"
FEED = "IBKR_LIVE"
# Native OPT/BAG support is selected by exact standard contract metadata.
# Software support does not attest account approval or guarantee acceptance.
# Penny prices: https://listingcenter.nasdaq.com/RuleBook/Nasdaq/rules/nasdaq-options-3
NATIVE_SHAPES = {
    "LONG_CALL": (1, 1), "LONG_PUT": (1, 1),
    "CASH_SECURED_PUT": (1, 1),
    "CALL_DEBIT_VERTICAL": (2, 1), "PUT_DEBIT_VERTICAL": (2, 1),
    "CALL_CREDIT_VERTICAL": (2, 1), "PUT_CREDIT_VERTICAL": (2, 1),
    "CALL_BUTTERFLY": (3, 2), "IRON_CONDOR": (4, 1), "IRON_BUTTERFLY": (4, 1),
    "LONG_STRADDLE": (2, 1), "LONG_STRANGLE": (2, 1),
}
CatalogCursor = namedtuple("CatalogCursor", "query under_id expiries strikes pending unresolved expires_at")


def now_wire():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def native_decimal(value):
    # IB's API supplies doubles. Preserve their decimal representation without
    # rounding into a tick or accepting NaN, infinity or unset sentinel values.
    check(type(value) in (float, int, str, Decimal), "Invalid IB numeric value")
    try:
        number = Decimal(str(value))
        check(number.is_finite(), "Non-finite IB numeric value")
        wire = format(number, "f")
        if "." in wire:
            wire = wire.rstrip("0").rstrip(".")
        decimal_wire(wire, nonnegative=True)
        return wire
    except (InvalidOperation, ValueError) as exc:
        raise BrokerContractError("Invalid IB numeric value") from exc


def native_contract(key):
    from ib_async import Option
    strike = float(Decimal(key.strike))
    check(Decimal(str(strike)) == Decimal(key.strike), "Strike loses precision at IB boundary")
    return Option(key.underlying, key.expiry.replace("-", ""), strike, key.right,
                  "SMART", multiplier="100", currency="USD", tradingClass=key.underlying)


def standard_key(key):
    return (bool(re.fullmatch(r"[A-Z][A-Z0-9.]{0,5}", key.underlying))
            and (key.currency, key.multiplier, key.exercise_style, key.settlement, key.deliverable_id)
            == ("USD", 100, "AMERICAN", "PHYSICAL", "STANDARD_100_SHARES"))


def contract_from_details(detail, underlying, under_con_id):
    c = detail.contract
    check(c.secType == "OPT" and c.symbol == underlying and c.tradingClass == underlying
          and c.currency == "USD" and native_decimal(c.multiplier) == "100"
          and detail.underConId == under_con_id and detail.underSymbol == underlying
          and detail.underSecType == "STK" and type(c.conId) is int and c.conId > 0,
          "IB contract is outside the standard equity option universe")
    check(c.exchange == "SMART" and "SMART" in detail.validExchanges.split(","), "SMART option route missing")
    expiry = c.lastTradeDateOrContractMonth
    check(bool(re.fullmatch(r"\d{8}", expiry)), "IB option expiry must be an exact date")
    key = OptionContractKey(underlying, f"{expiry[:4]}-{expiry[4:6]}-{expiry[6:]}", c.right,
                           native_decimal(c.strike), "USD", 100, "AMERICAN", "PHYSICAL", "STANDARD_100_SHARES")
    strike = Decimal(key.strike) * 1000
    check(strike == strike.to_integral_value() and strike < 100000000, "Unsupported OCC strike")
    occ = f"{underlying:<6}{expiry[2:]}{c.right}{int(strike):08d}"
    check(c.localSymbol == occ, "Adjusted or mismatched native OCC symbol")
    # ContractDetails is not an active/tradable permission nor a full calendar.
    return OptionContract(option_contract_id(key), key, None, None, None, "UNKNOWN")


class IBOptionReads:
    async def option_account_identities(self):
        """Handshake account identities; no summary subscription or reconnect."""
        from .exceptions import IBConnectionError
        async def read(ib):
            return tuple(sorted(set(ib.managedAccounts())))
        try:
            return await self._client.read_options(read)
        except IBConnectionError as exc:
            raise BrokerConnectionError('IB account identity requires the current connection') from exc

    def _init_option_reads(self):
        self._option_account_binding = None
        self._option_connection = None
        self._option_catalog = OrderedDict()
        self._option_pages = OrderedDict()
        self._option_flex_pages = OrderedDict()
        self._option_history_pages = OrderedDict()
        self._option_history_lock = asyncio.Lock()
        self._option_history_next = 0.0
        self._option_account_read_lock = asyncio.Lock()
        self._option_account_evidence = deque(maxlen=32)
        self._option_live_observed = None

    def bind_option_account(self, verified):
        from .settings import native_account_is_exact
        check(type(verified) is OptionVerifiedAccount and verified.broker == "IBKR"
              and verified.scope.environment == self._option_environment
              and native_account_is_exact(verified.native_account_ref)
              and (self._configured_account is None or verified.native_account_ref == self._configured_account),
              "Option read requires the configured verified IB account and environment")
        state = self._client.connection_state_snapshot()
        check(state.get("connected") and state.get("connected_since") is not None,
              "Option read requires an observed IB connection")
        if self._option_account_binding != verified or self._option_connection != state["connected_since"]:
            self._option_catalog.clear()
            self._option_pages.clear()
            self._option_flex_pages.clear()
            self._option_history_pages.clear()
            self._option_live_observed = None
        self._option_account_binding = verified
        self._option_connection = state["connected_since"]

    async def _option_read(self, request, operation):
        scope = OptionScope(**{name: getattr(request, name) for name in OptionScope.__dataclass_fields__})
        bound, connected = self._option_account_binding, self._option_connection
        check(bound is not None and bound.scope == scope, "Option read differs from verified account scope")

        def current(ib):
            state = self._client.connection_state_snapshot()
            check(self._option_account_binding == bound and state.get("connected")
                  and state.get("connected_since") == connected and ib.isConnected()
                  and bound.native_account_ref in ib.managedAccounts(),
                  "Verified option account or IB connection changed")

        async def read(ib):
            current(ib)
            result = await operation(ib, bound)
            current(ib)
            return result

        try:
            return await self._client.read_options(read)
        except (ConnectionError, asyncio.TimeoutError) as exc:
            raise BrokerConnectionError("IB option read did not complete on its bound connection") from exc

    async def _underlying(self, ib, symbol):
        from ib_async import Stock
        check(bool(re.fullmatch(r"[A-Z][A-Z0-9.]{0,5}", symbol)), "Standard OCC underlying symbol required")
        details = await asyncio.wait_for(ib.reqContractDetailsAsync(Stock(symbol, "SMART", "USD")),
                                         self._qualification_timeout)
        check(len(details) == 1, "Underlying must qualify uniquely")
        c = details[0].contract
        check(c.secType == "STK" and c.symbol == symbol and c.currency == "USD" and c.conId > 0,
              "Underlying qualification changed economics")
        return c.conId

    async def _details(self, ib, key):
        return await asyncio.wait_for(ib.reqContractDetailsAsync(native_contract(key)), self._qualification_timeout)

    def _remember_contract(self, bound, detail, contract):
        c = detail.contract
        metadata = dict(rule=STANDARD_RULE, source_url=STANDARD_SOURCE, checked_at=STANDARD_CHECKED_AT,
                        conId=c.conId, localSymbol=c.localSymbol, key=asdict(contract.key),
                        underConId=detail.underConId, tradingClass=c.tradingClass, validExchanges=detail.validExchanges,
                        minTick=native_decimal(detail.minTick), marketRuleIds=detail.marketRuleIds)
        if contract.key.underlying not in {"SPY", "QQQ"}:
            # Preserve historical SPY/QQQ qualification metadata hashes.
            metadata.update(rule="ib-standard-equity-occ-v1",
                source_url="https://www.theocc.com/clearance-and-settlement/clearing/equity-options-product-specifications",
                checked_at="2026-11-04")
        version = sha256(json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        binding = BrokerOptionBinding(self.adapter_id, VERSION, bound.scope.environment, bound.scope.account,
            contract.canonical_id, str(c.conId), c.localSymbol, "SMART", now_wire(), version)
        self._option_catalog[str(c.conId)] = (binding, contract, detail)
        self._option_catalog.move_to_end(str(c.conId))
        while len(self._option_catalog) > 20000:
            self._option_catalog.popitem(last=False)
        return binding

    async def list_option_contracts(self, request):
        check(type(request) is OptionContractQuery, "Expected an exact option contract query")

        async def read(ib, bound):
            query = asdict(request)
            query.pop("cursor")
            observed = now_wire()
            if request.cursor is not None:
                page = self._option_pages.get(request.cursor)
                check(page is not None and page.query == query and timestamp(observed) < page.expires_at,
                      "Invalid or expired IB catalog cursor")
                _, under_id, expiries, strikes, pending, unresolved, _ = page
            else:
                under_id = await self._underlying(ib, request.underlying)
                chains = await asyncio.wait_for(ib.reqSecDefOptParamsAsync(request.underlying, "", "STK", under_id),
                                               self._qualification_timeout)
                # ib_async's secdef decoder retains underlyingConId as wire text;
                # ContractDetails supplies the same identity as an integer.
                chains = [chain for chain in chains if chain.exchange == "SMART" and str(chain.underlyingConId) == str(under_id)
                          and chain.tradingClass == request.underlying and str(chain.multiplier) == "100"]
                check(len(chains) == 1, "Standard SMART option parameters must be unique")
                chain = chains[0]
                expiries = tuple(sorted(expiry for expiry in chain.expirations if re.fullmatch(r"\d{8}", expiry)
                                        and request.expiry_from.replace("-", "") <= expiry <= request.expiry_to.replace("-", "")))
                lower = Decimal(request.strike_min) if request.strike_min is not None else Decimal(0)
                upper = Decimal(request.strike_max) if request.strike_max is not None else Decimal("Infinity")
                strikes = frozenset(strike for value in chain.strikes if (strike := Decimal(native_decimal(value))) > 0
                                    and lower <= strike <= upper)
                check(len(expiries) * len(strikes) * (1 if request.right else 2) <= 10000,
                      "Narrow the IB contract query bounds")
                pending, unresolved = (), False
            if not pending and expiries and strikes:
                pending, ambiguous = await self._catalog_expiry(ib, request, under_id, expiries[0], strikes)
                expiries = expiries[1:]
                unresolved = unresolved or ambiguous
            contracts, bindings = [], []
            for contract, detail in pending[:request.limit]:
                try:
                    binding = self._remember_contract(bound, detail, contract)
                except BrokerContractError:
                    unresolved = True
                    continue
                contracts.append(contract)
                bindings.append(binding)
            pending = pending[request.limit:]
            cursor = None
            if pending or (expiries and strikes):
                cursor = str(uuid4())
                self._option_pages[cursor] = CatalogCursor(query, under_id, expiries, strikes, pending,
                                                          unresolved, timestamp(observed) + timedelta(minutes=2))
                while len(self._option_pages) > 32:
                    self._option_pages.popitem(last=False)
            return ContractPage(tuple(contracts), cursor, cursor is None and not unresolved, now_wire(), bindings=tuple(bindings))

        return await self._option_read(request, read)

    async def _catalog_expiry(self, ib, request, under_id, expiry, strikes):
        from ib_async import Option

        # Security-definition strikes and expiries are independent sets. Ask
        # for listed contracts for one expiry before paginating exact matches.
        probe = Option(request.underlying, expiry, 0.0, request.right or "", "SMART",
                       multiplier="100", currency="USD", tradingClass=request.underlying)
        details = await asyncio.wait_for(ib.reqContractDetailsAsync(probe), self._qualification_timeout)
        matches, duplicates = {}, set()
        unresolved = False
        for detail in details:
            native = detail.contract
            try:
                if Decimal(native_decimal(native.strike)) not in strikes:
                    continue
                check(native.lastTradeDateOrContractMonth == expiry and
                      (request.right is None or native.right == request.right), "IB returned contracts outside the query")
                contract = contract_from_details(detail, request.underlying, under_id)
            except BrokerContractError:
                unresolved = True
                continue
            if contract.canonical_id in matches:
                duplicates.add(contract.canonical_id)
            matches[contract.canonical_id] = (contract, detail)
        exact = [item for identity, item in matches.items() if identity not in duplicates]
        exact.sort(key=lambda item: (Decimal(item[0].key.strike), item[0].key.right))
        return tuple(exact), unresolved or bool(duplicates)

    async def qualify_option_contracts(self, request):
        check(type(request) is QualificationRequest, "Expected an exact option qualification request")

        async def read(ib, bound):
            results, underlyings = [], {}
            now = datetime.now(timezone.utc)
            for expected in request.contracts:
                check(expected.canonical_id == option_contract_id(expected.key), "Contract economics differ from canonical identity")
                key = expected.key
                cached = next((value for value in self._option_catalog.values()
                    if value[1].canonical_id == expected.canonical_id and value[1].key == key
                    and timedelta(0) <= now - timestamp(value[0].qualified_at) < timedelta(days=1)), None)
                if cached is not None:
                    results.append(QualificationResult(expected.canonical_id, 'EXACT', cached[0], cached[1], ()))
                    continue
                status, contract, binding = "UNSUPPORTED", None, None
                if standard_key(key):
                    if key.underlying not in underlyings:
                        underlyings[key.underlying] = await self._underlying(ib, key.underlying)
                    details = await self._details(ib, key)
                    status = "NOT_FOUND" if not details else "AMBIGUOUS" if len(details) != 1 else "UNSUPPORTED"
                    if len(details) == 1:
                        try:
                            actual = contract_from_details(details[0], key.underlying, underlyings[key.underlying])
                            check(actual.key == key, "IB qualification changed requested economics")
                            binding = self._remember_contract(bound, details[0], actual)
                            contract, status = actual, "EXACT"
                        except BrokerContractError:
                            pass
                results.append(QualificationResult(expected.canonical_id, status, binding, contract,
                                                   () if status == "EXACT" else ("CONTRACT_" + status,)))
            return QualificationBatch(tuple(results), now_wire())

        return await self._option_read(request, read)

    async def option_certification_context(self, request):
        async def read(ib, bound):
            version = ib.client.serverVersion()
            check(type(version) is int and version > 0, "IB server version is unavailable")
            return version
        return await self._option_read(request, read)

    async def option_capabilities(self, request):
        async def read(ib, bound):
            observed = now_wire()
            implemented = OptionCapability("IMPLEMENTED", (), ())
            unavailable = OptionCapability("UNSUPPORTED", ("NOT_IMPLEMENTED",), ())
            lifecycle = OptionCapability("IMPLEMENTED", ("LIFECYCLE_SOURCE_UNVERIFIED",), ())
            shapes = tuple(OptionShapeCapability(name, "NATIVE", implemented, legs, ratio, "0.01",
                False, legs > 1, False, False) for name, (legs, ratio) in NATIVE_SHAPES.items())
            return OptionCapabilities(bound.scope, self.adapter_id, VERSION, "ib-options-reads-9", observed,
                (timestamp(observed) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z"),
                implemented, implemented, implemented, implemented, implemented, implemented, lifecycle,
                unavailable, unavailable, implemented, implemented, (FEED, "provider_native"), shapes, default_feed=FEED,
                history_feeds=("provider_native",), default_history_feed="provider_native")
        return await self._option_read(request, read)

    async def option_account_permissions(self, request):
        from .options_account import permissions
        return await permissions(self, request)

    async def option_account_state(self, request, *, retain_lifecycle=None, resolve_contract=None, flex_state=None):
        from .options_account import account_state
        return await account_state(self, request, retain_evidence=retain_lifecycle,
            resolve_contract=resolve_contract, flex_state=flex_state)

    async def option_snapshot(self, request):
        check(type(request) is SnapshotRequest and request.feed == FEED, "IB snapshot requires explicit IBKR_LIVE feed")
        from .options_quotes import read_snapshot

        async def read(ib, bound):
            contracts = self._option_bound_contracts(request, fresh=True)
            result = await read_snapshot(ib, request, contracts, timeout=self._qualification_timeout)
            if result.complete and all(quote.quality == "EXECUTABLE" for quote in result.quotes):
                self._option_live_observed = (bound.scope, result.observed_at)
            return result

        return await self._option_read(request, read)

    def _option_bound_contracts(self, request, *, fresh=False):
        contracts = []
        for binding in request.bindings:
            current = self._option_catalog.get(binding.broker_contract_id)
            check(current is not None and replace(current[0], qualified_at=binding.qualified_at) == binding,
                  "IB read requires the current exact native qualification")
            # Re-reading account positions may refresh qualified_at for the
            # same economics. Quote freshness belongs to native ticks, not to
            # the timestamp of an unchanged contract qualification.
            contracts.append(current[2].contract)
        return contracts

    async def stream_option_quotes(self, request):
        from .options_stream import stream_quotes
        source = stream_quotes(self, request)
        try:
            async for quote in source:
                yield quote
        finally:
            await source.aclose()

    async def option_history(self, request):
        from .options_history import read_history
        return await read_history(self, request)

    async def option_calendar(self, request):
        from .options_calendar import read_calendar
        return await read_calendar(self, request)
