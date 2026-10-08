"""Scoped IB option discovery and quotes on the adapter's existing connection.

P0 economics use the versioned standard SPY/QQQ OCC identity below. IB does not
return a full deliverable record; adjusted roots and nonstandard classes are
excluded. A read is not account permission, calendar or trading certification.
"""

import asyncio
from collections import OrderedDict, deque
from dataclasses import asdict
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
from algo_trader_broker_sdk.options_capabilities import OptionCapabilities, OptionCapability

VERSION = "0.2.0"
STANDARD_RULE = "ib-spy-qqq-standard-occ-v1"
STANDARD_SOURCE = "https://www.cboe.com/exchange-traded-stock/etp-options-spec"
STANDARD_CHECKED_AT = "2026-10-08"
FEED = "IBKR_LIVE"


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
    return (key.underlying in {"SPY", "QQQ"}
            and (key.currency, key.multiplier, key.exercise_style, key.settlement, key.deliverable_id)
            == ("USD", 100, "AMERICAN", "PHYSICAL", "STANDARD_100_SHARES"))


def contract_from_details(detail, underlying, under_con_id):
    c = detail.contract
    check(c.secType == "OPT" and c.symbol == underlying and c.tradingClass == underlying
          and c.currency == "USD" and native_decimal(c.multiplier) == "100"
          and detail.underConId == under_con_id and detail.underSymbol == underlying
          and detail.underSecType == "STK" and type(c.conId) is int and c.conId > 0,
          "IB contract is outside the standard ETF option universe")
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
        check(type(verified) is OptionVerifiedAccount and verified.broker == "IBKR"
              and verified.scope.environment == "paper" and verified.native_account_ref.startswith("DU"),
              "Option read requires an explicit verified IB Paper account")
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
        check(symbol in {"SPY", "QQQ"}, "P0 supports SPY/QQQ options")
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
        version = sha256(json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        binding = BrokerOptionBinding(self.adapter_id, VERSION, bound.scope.environment, bound.scope.account,
            contract.canonical_id, str(c.conId), c.localSymbol, "SMART", now_wire(), version)
        self._option_catalog[str(c.conId)] = (binding, contract, detail)
        self._option_catalog.move_to_end(str(c.conId))
        while len(self._option_catalog) > 1000:
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
                check(page is not None and page[0] == query and timestamp(observed) < page[4], "Invalid or expired IB catalog cursor")
                _, under_id, candidates, offset, _, unresolved = page
            else:
                under_id = await self._underlying(ib, request.underlying)
                chains = await asyncio.wait_for(ib.reqSecDefOptParamsAsync(request.underlying, "", "STK", under_id),
                                               self._qualification_timeout)
                chains = [chain for chain in chains if chain.exchange == "SMART" and chain.underlyingConId == under_id
                          and chain.tradingClass == request.underlying and str(chain.multiplier) == "100"]
                check(len(chains) == 1, "Standard SMART option parameters must be unique")
                chain = chains[0]
                expiries = sorted(expiry for expiry in chain.expirations if re.fullmatch(r"\d{8}", expiry)
                                  and request.expiry_from.replace("-", "") <= expiry <= request.expiry_to.replace("-", ""))
                strikes = sorted({native_decimal(strike) for strike in chain.strikes}, key=Decimal)
                strikes = [strike for strike in strikes if Decimal(strike) > 0
                           and (request.strike_min is None or Decimal(strike) >= Decimal(request.strike_min))
                           and (request.strike_max is None or Decimal(strike) <= Decimal(request.strike_max))]
                rights = (request.right,) if request.right else ("C", "P")
                check(len(expiries) * len(strikes) * len(rights) <= 10000, "Narrow the IB contract query bounds")
                candidates = [(expiry, strike, right) for expiry in expiries for strike in strikes for right in rights]
                offset, unresolved = 0, False
            contracts = []
            for expiry, strike, right in candidates[offset:offset + request.limit]:
                key = OptionContractKey(request.underlying, f"{expiry[:4]}-{expiry[4:6]}-{expiry[6:]}", right,
                                       strike, "USD", 100, "AMERICAN", "PHYSICAL", "STANDARD_100_SHARES")
                details = await self._details(ib, key)
                # IB's parameter cross-product includes nonexistent contracts.
                if not details:
                    continue
                if len(details) != 1:
                    unresolved = True
                    continue
                try:
                    contract = contract_from_details(details[0], request.underlying, under_id)
                    check(contract.key == key, "IB qualification changed requested economics")
                    self._remember_contract(bound, details[0], contract)
                except BrokerContractError:
                    unresolved = True
                    continue
                contracts.append(contract)
            offset += request.limit
            cursor = None
            if offset < len(candidates):
                cursor = str(uuid4())
                self._option_pages[cursor] = (query, under_id, candidates, offset,
                                             timestamp(observed) + timedelta(minutes=2), unresolved)
                while len(self._option_pages) > 32:
                    self._option_pages.popitem(last=False)
            return ContractPage(tuple(contracts), cursor, cursor is None and not unresolved, now_wire())

        return await self._option_read(request, read)

    async def qualify_option_contracts(self, request):
        check(type(request) is QualificationRequest, "Expected an exact option qualification request")

        async def read(ib, bound):
            results, underlyings = [], {}
            for expected in request.contracts:
                check(expected.canonical_id == option_contract_id(expected.key), "Contract economics differ from canonical identity")
                key = expected.key
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
            implemented = OptionCapability("IMPLEMENTED", ("ACCOUNT_CERTIFICATION_REQUIRED",), ())
            unavailable = OptionCapability("UNSUPPORTED", ("NOT_IMPLEMENTED",), ())
            native_greeks = OptionCapability("IMPLEMENTED",
                ("GREEKS_INPUT_TIME_UNAVAILABLE", "GREEKS_MODEL_VERSION_UNAVAILABLE", "GREEKS_UNITS_UNVERIFIED"), ())
            lifecycle = OptionCapability("IMPLEMENTED", ("LIFECYCLE_SOURCE_UNVERIFIED",), ())
            return OptionCapabilities(bound.scope, self.adapter_id, VERSION, "ib-options-reads-5", observed,
                (timestamp(observed) + timedelta(seconds=30)).isoformat().replace("+00:00", "Z"),
                implemented, implemented, native_greeks, implemented, implemented, implemented, lifecycle,
                unavailable, unavailable, implemented, implemented, (FEED, "provider_native"), ())
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
            check(current is not None and current[0] == binding,
                  "IB read requires the current exact native qualification")
            if fresh:
                check(0 <= (datetime.now(timezone.utc) - timestamp(binding.qualified_at)).total_seconds() < 30,
                      "IB quote requires a fresh native qualification")
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
