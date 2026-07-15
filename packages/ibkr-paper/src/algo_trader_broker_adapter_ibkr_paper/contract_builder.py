"""Utilities for constructing IB contract objects from flexible inputs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from ib_async import (
    Bond,
    CFD,
    Commodity,
    Contract,
    Crypto,
    Forex,
    Future,
    FuturesOption,
    Index,
    Option,
    Stock,
)

INDEX_FUTURE_SYMBOLS: frozenset[str] = frozenset(
    {
        "ES",
        "MES",
        "NQ",
        "MNQ",
        "YM",
        "MYM",
        "RTY",
        "M2K",
    }
)

INDEX_FUTURE_DEFAULT_EXCHANGES: dict[str, str] = {
    "ES": "CME",
    "MES": "CME",
    "NQ": "CME",
    "MNQ": "CME",
    "YM": "CBOT",
    "MYM": "CBOT",
    "RTY": "CME",
    "M2K": "CME",
}

INDEX_FUTURE_EXCHANGE_OVERRIDES: dict[str, str | None] = {
    "SMART": None,
    "NYBOT": "CME",
    "ICE": "CME",
    "ICEUS": "CME",
}

INDEX_FUTURE_ROOTS: tuple[str, ...] = tuple(sorted(INDEX_FUTURE_SYMBOLS, key=len, reverse=True))

VIX_FUTURE_ROOTS: frozenset[str] = frozenset({"VX", "VIX"})

# 特殊月度期货根符号（目前覆盖 MET），用于到期推断；交易所仍使用 CME
CRYPTO_FUTURE_ROOTS: frozenset[str] = frozenset({"MET"})

FUTURE_EXCHANGE_FALLBACKS: dict[str, tuple[str, ...]] = {
    "GLOBEX": ("GLOBEX", "CME", "CFE"),
    "CME": ("CME", "GLOBEX", "CFE"),
    "ECBOT": ("ECBOT", "CBOT"),
    "CBOT": ("CBOT", "ECBOT"),
    "CBOE": ("CBOE", "CFE"),
    "NYBOT": ("NYBOT", "CME", "ICEUS", "ICE"),
    "ICEUS": ("ICEUS", "CME", "NYBOT"),
    "ICE": ("ICE", "CME", "NYBOT"),
    "SMART": ("SMART", "CFE"),
    "CFE": ("CFE", "CBOE"),
}

FUTURE_SYMBOL_CODE_PATTERN = re.compile(r"^[A-Z]{1,3}[FGHJKMNQUVXZ]\d{1,4}$")

CONTRACT_MONTH_CODES: dict[str, int] = {
    "F": 1,
    "G": 2,
    "H": 3,
    "J": 4,
    "K": 5,
    "M": 6,
    "N": 7,
    "Q": 8,
    "U": 9,
    "V": 10,
    "X": 11,
    "Z": 12,
}

QUARTERLY_CONTRACT_MONTHS: tuple[int, ...] = (3, 6, 9, 12)
NEW_YORK_TZ = ZoneInfo("America/New_York")


@dataclass(slots=True)
class ContractDefaults:
    """Default exchanges and currencies used when values are not supplied."""

    stock_exchange: str | None = None
    stock_currency: str | None = None
    future_exchange: str | None = None
    future_currency: str | None = None
    index_exchange: str | None = None
    index_currency: str | None = None
    forex_exchange: str | None = None
    forex_currency: str | None = None


@dataclass(slots=True)
class ContractParams:
    """Normalized inputs for building a contract."""

    symbol: str
    sec_type: str | None = None
    exchange: str | None = None
    currency: str | None = None
    local_symbol: str | None = None
    primary_exchange: str | None = None
    trading_class: str | None = None
    last_trade_date_or_contract_month: str | None = None
    include_expired: bool | None = None
    strike: float | None = None
    right: str | None = None
    multiplier: str | None = None
    sec_id_type: str | None = None
    sec_id: str | None = None
    con_id: int | str | None = None
    metadata: Mapping[str, Any] | None = None


@dataclass(slots=True)
class ContractBuildResult:
    """Rendered contract alongside normalized attributes."""

    contract: Any
    exchange: str | None
    currency: str | None
    local_symbol: str | None
    sec_type: str


class IBContractFactory:
    """Factory responsible for creating IB contract objects."""

    def __init__(self, defaults: ContractDefaults | None = None) -> None:
        self._defaults = defaults or ContractDefaults()

    def build(self, params: ContractParams) -> ContractBuildResult:
        metadata_input = params.metadata or {}
        metadata = metadata_input if isinstance(metadata_input, Mapping) else {}

        symbol = (params.symbol or "").strip().upper()
        if not symbol:
            raise ValueError("symbol must not be empty")

        explicit_sec_type = _string_upper(params.sec_type)
        sec_type = _infer_security_type(symbol, explicit_sec_type, metadata)

        if sec_type == "STK":
            corrections = {"APPL": "AAPL"}
            mapped = corrections.get(symbol)
            if mapped:
                symbol = mapped

        con_id_value = _normalise_con_id(params.con_id, metadata)
        if sec_type == "FUT" and symbol in INDEX_FUTURE_SYMBOLS and params.con_id is None:
            con_id_value = None

        exchange_value = params.exchange or metadata.get("exchange")
        if exchange_value is None and "primaryExchange" in metadata:
            exchange_value = metadata["primaryExchange"]
        exchange = _string_upper(exchange_value) or ""

        currency_value = params.currency or metadata.get("currency")
        currency = _string_upper(currency_value) or ""

        local_symbol = params.local_symbol or metadata.get("local_symbol") or metadata.get("localSymbol")
        if sec_type == "FUT" and symbol in INDEX_FUTURE_SYMBOLS and params.local_symbol is None:
            local_symbol = None
        if isinstance(local_symbol, str):
            local_symbol = local_symbol.upper()
        else:
            local_symbol = None

        if sec_type == "FUT" and local_symbol:
            simplified_local = re.sub(r"\s+", "", local_symbol)
            simplified_symbol = re.sub(r"\s+", "", symbol)
            if simplified_local == simplified_symbol or not any(char.isdigit() for char in simplified_local):
                local_symbol = None

        primary_exchange_value = params.primary_exchange or metadata.get("primary_exchange") or metadata.get("primaryExchange")
        primary_exchange = _string_upper(primary_exchange_value)

        trading_class_value = params.trading_class or metadata.get("trading_class") or metadata.get("tradingClass")
        trading_class = _string_upper(trading_class_value)

        multiplier_value = params.multiplier or metadata.get("multiplier")
        multiplier = str(multiplier_value) if multiplier_value is not None else None

        expiry_value = (
            params.last_trade_date_or_contract_month
            or metadata.get("lastTradeDateOrContractMonth")
            or metadata.get("expiry")
            or metadata.get("last_trade_date")
            or metadata.get("lastTradeDate")
            or metadata.get("contract_month")
            or metadata.get("contractMonth")
        )
        expiry = str(expiry_value) if expiry_value is not None else ""

        if sec_type == "FUT" and not expiry:
            ref_date_val = metadata.get("reference_date") or metadata.get("today")
            ref_date = _coerce_reference_date(ref_date_val)
            expiry = _infer_future_expiry(symbol, metadata, local_symbol=local_symbol, today=ref_date)
        if sec_type == "FUT" and symbol in INDEX_FUTURE_SYMBOLS and expiry:
            expiry_month = _contract_month_token(expiry)
            if expiry_month:
                rebuilt_local_symbol = _build_index_future_local_symbol(symbol, expiry_month)
                if rebuilt_local_symbol:
                    local_symbol = rebuilt_local_symbol

        strike_value = params.strike
        if strike_value is None:
            strike_value = metadata.get("strike")
        try:
            strike = float(strike_value) if strike_value is not None else 0.0
        except (TypeError, ValueError):
            strike = 0.0

        right_value = params.right or metadata.get("right")
        right = _string_upper(right_value) or ""

        include_expired = params.include_expired
        if include_expired is None:
            include_expired_value = metadata.get("include_expired", metadata.get("includeExpired"))
            if isinstance(include_expired_value, str):
                lowered = include_expired_value.strip().lower()
                include_expired = lowered in {"1", "true", "yes", "y"}
            elif include_expired_value is not None:
                include_expired = bool(include_expired_value)

        sec_id_type = params.sec_id_type or metadata.get("sec_id_type") or metadata.get("secIdType")
        sec_id = params.sec_id or metadata.get("sec_id") or metadata.get("secId")

        # Normalize common crypto pair inputs before computing exchange defaults
        # Examples: BTCUSD, ETHUSDT, BTC/USDC, ETH-USD -> CRYPTO on PAXOS with USD
        if sec_type == "CRYPTO":
            # Try to extract base/quote parts from the symbol
            m = re.match(r"^([A-Z]{2,6})(?:USD|USDT|USDC)$", symbol)
            m_sep = re.match(r"^([A-Z]{2,6})[-/]?(USD|USDT|USDC)$", symbol)
            base_symbol = None
            quote = None
            if m:
                base_symbol = m.group(1)
                quote = "USD"
            elif m_sep:
                base_symbol = m_sep.group(1)
                quote = "USD"
            if base_symbol:
                symbol = base_symbol
            # IB crypto contracts quote in USD; coerce USDT/USDC to USD when unspecified
            if not currency:
                currency = quote or "USD"
            # IB does not support SMART routing for CRYPTO; default to PAXOS
            if not exchange or exchange == "SMART":
                exchange = "PAXOS"

        requested_exchange = exchange
        builder_exchange = exchange or self._default_exchange_for(sec_type)
        if isinstance(builder_exchange, str):
            builder_exchange = builder_exchange.upper()
        else:
            builder_exchange = ""

        if sec_type == "FUT":
            default_future_exchange = self._default_exchange_for("FUT")
            if default_future_exchange:
                default_future_exchange = default_future_exchange.upper()
            if builder_exchange == "SMART" and default_future_exchange:
                builder_exchange = default_future_exchange
            if primary_exchange == "SMART":
                primary_exchange = None
            builder_exchange = _normalise_index_future_exchange(symbol, builder_exchange)

            future_root = _resolve_future_root(symbol, metadata) or ""
            if future_root in VIX_FUTURE_ROOTS:
                # IB expects VIX futures to use symbol 'VIX' with tradingClass 'VX'.
                # Normalize inputs to IB conventions while preserving exchange logic.
                symbol = "VIX"
                if not trading_class:
                    trading_class = "VX"
                # For VIX futures, the correct exchange is CFE. Default to CFE
                # when the incoming exchange is blank/SMART or equals the default
                # future exchange, and keep primaryExchange as SMART only when
                # the user requested SMART routing.
                fallback_candidates = {"", "SMART"}
                if default_future_exchange:
                    fallback_candidates.add(default_future_exchange)
                if builder_exchange in fallback_candidates:
                    builder_exchange = "CFE"
                    if requested_exchange == "SMART" and not primary_exchange:
                        primary_exchange = "SMART"

            # MET 使用 CME 作为交易所；当用户未明确设置或使用 SMART 时进行默认修正
            if future_root in CRYPTO_FUTURE_ROOTS:
                if not trading_class:
                    trading_class = future_root
                fallback_candidates = {"", "SMART"}
                if default_future_exchange:
                    fallback_candidates.add(default_future_exchange)
                if builder_exchange in fallback_candidates:
                    builder_exchange = "CME"
                    if requested_exchange == "SMART" and not primary_exchange:
                        primary_exchange = "SMART"

        builder_currency = currency or self._default_currency_for(sec_type) or ""
        if builder_currency:
            builder_currency = builder_currency.upper()

        builder_kwargs: dict[str, Any] = {}
        if builder_currency:
            builder_kwargs["currency"] = builder_currency
        if primary_exchange:
            builder_kwargs["primaryExchange"] = primary_exchange
        if local_symbol:
            builder_kwargs["localSymbol"] = local_symbol
        if trading_class:
            builder_kwargs["tradingClass"] = trading_class
        if multiplier:
            builder_kwargs["multiplier"] = multiplier
        if include_expired is not None:
            builder_kwargs["includeExpired"] = include_expired

        if con_id_value is not None:
            contract = Contract()
            contract.conId = con_id_value
            contract.symbol = symbol
            contract.secType = sec_type
            if builder_exchange:
                contract.exchange = builder_exchange
            if builder_currency:
                contract.currency = builder_currency
            if local_symbol:
                contract.localSymbol = local_symbol
            if primary_exchange:
                contract.primaryExchange = primary_exchange
            if expiry:
                contract.lastTradeDateOrContractMonth = expiry
            if multiplier:
                contract.multiplier = multiplier
            if trading_class:
                contract.tradingClass = trading_class
            if include_expired is not None:
                contract.includeExpired = include_expired  # type: ignore[attr-defined]
            if sec_id_type:
                contract.secIdType = sec_id_type  # type: ignore[attr-defined]
            if sec_id:
                contract.secId = sec_id  # type: ignore[attr-defined]
            result_exchange = builder_exchange or None
            result_currency = builder_currency or None
            result_local_symbol = local_symbol or None
            return ContractBuildResult(
                contract=contract,
                exchange=result_exchange,
                currency=result_currency,
                local_symbol=result_local_symbol,
                sec_type=sec_type,
            )

        if sec_type == "STK":
            contract = Stock(symbol, exchange=builder_exchange or "SMART", **builder_kwargs)
        elif sec_type in {"CASH", "FX"}:
            contract = Forex(symbol, exchange=builder_exchange or "IDEALPRO", **builder_kwargs)
        elif sec_type == "FUT":
            contract = Future(
                symbol=symbol,
                lastTradeDateOrContractMonth=expiry,
                exchange=builder_exchange,
                **builder_kwargs,
            )
        elif sec_type in {"OPT", "IOPT"}:
            contract = Option(
                symbol=symbol,
                lastTradeDateOrContractMonth=expiry,
                strike=strike,
                right=right,
                exchange=builder_exchange,
                **builder_kwargs,
            )
        elif sec_type in {"FOP", "FUTOPT"}:
            contract = FuturesOption(
                symbol=symbol,
                lastTradeDateOrContractMonth=expiry,
                strike=strike,
                right=right,
                exchange=builder_exchange,
                **builder_kwargs,
            )
        elif sec_type == "CFD":
            contract = CFD(symbol, exchange=builder_exchange, **builder_kwargs)
        elif sec_type in {"IND", "INDEX"}:
            contract = Index(symbol, exchange=builder_exchange, **builder_kwargs)
        elif sec_type == "CMDTY":
            contract = Commodity(symbol, exchange=builder_exchange, **builder_kwargs)
        elif sec_type == "BOND" and sec_id_type and sec_id:
            bond_kwargs = dict(builder_kwargs)
            if builder_exchange:
                bond_kwargs.setdefault("exchange", builder_exchange)
            contract = Bond(secIdType=sec_id_type, secId=sec_id, **bond_kwargs)
        elif sec_type == "CRYPTO":
            contract = Crypto(symbol, exchange=builder_exchange, **builder_kwargs)
        else:
            contract = Contract()
            contract.symbol = symbol
            contract.secType = sec_type
            if builder_exchange:
                contract.exchange = builder_exchange
            if builder_currency:
                contract.currency = builder_currency
            if local_symbol:
                contract.localSymbol = local_symbol
            if primary_exchange:
                contract.primaryExchange = primary_exchange
            if expiry:
                contract.lastTradeDateOrContractMonth = expiry
            if multiplier:
                contract.multiplier = multiplier
            if trading_class:
                contract.tradingClass = trading_class
            if include_expired is not None:
                contract.includeExpired = include_expired  # type: ignore[attr-defined]
            if sec_id_type:
                contract.secIdType = sec_id_type  # type: ignore[attr-defined]
            if sec_id:
                contract.secId = sec_id  # type: ignore[attr-defined]

        result_exchange = builder_exchange or None
        result_currency = builder_currency or None
        result_local_symbol = local_symbol or None
        return ContractBuildResult(
            contract=contract,
            exchange=result_exchange,
            currency=result_currency,
            local_symbol=result_local_symbol,
            sec_type=sec_type,
        )

    def _default_exchange_for(self, sec_type: str) -> str | None:
        sec_type_upper = (sec_type or "").upper()
        if sec_type_upper == "STK":
            return self._defaults.stock_exchange
        if sec_type_upper == "FUT":
            return self._defaults.future_exchange or self._defaults.stock_exchange
        if sec_type_upper in {"IND", "INDEX"}:
            return self._defaults.index_exchange or self._defaults.stock_exchange
        if sec_type_upper in {"CASH", "FX"}:
            return self._defaults.forex_exchange
        if sec_type_upper == "CRYPTO":
            return "PAXOS"
        return self._defaults.stock_exchange

    def _default_currency_for(self, sec_type: str) -> str | None:
        sec_type_upper = (sec_type or "").upper()
        if sec_type_upper == "STK":
            return self._defaults.stock_currency
        if sec_type_upper == "FUT":
            return self._defaults.future_currency or self._defaults.stock_currency
        if sec_type_upper in {"IND", "INDEX"}:
            return self._defaults.index_currency or self._defaults.stock_currency
        if sec_type_upper in {"CASH", "FX"}:
            return self._defaults.forex_currency
        if sec_type_upper == "CRYPTO":
            return "USD"
        return self._defaults.stock_currency


def _normalise_con_id(con_id: int | str | None, metadata: Mapping[str, Any]) -> int | str | None:
    candidate = con_id
    if candidate is None:
        raw_metadata_con_id = metadata.get("con_id", metadata.get("conId"))
        candidate = raw_metadata_con_id
    if isinstance(candidate, str):
        token = candidate.strip()
        if not token:
            return None
        try:
            return int(token)
        except ValueError:
            return token
    if candidate is None:
        return None
    try:
        return int(candidate)
    except (TypeError, ValueError):
        return str(candidate)


def _normalise_string(value: Any) -> str | None:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            return stripped
    return None


def _string_upper(value: Any) -> str | None:
    normalised = _normalise_string(value)
    return normalised.upper() if normalised else None


def _extract_metadata_sec_type(metadata: Mapping[str, Any]) -> str | None:
    for key in (
        "sec_type",
        "secType",
        "instrument_type",
        "instrumentType",
        "type",
    ):
        candidate = _string_upper(metadata.get(key))
        if candidate:
            return candidate
    instrument = metadata.get("instrument")
    if isinstance(instrument, str):
        parts = [part.strip() for part in instrument.split("|")]
        if len(parts) > 1 and parts[1]:
            return parts[1].upper()
    return None


def _extract_local_symbol(metadata: Mapping[str, Any]) -> str | None:
    for key in ("local_symbol", "localSymbol", "ib_local_symbol", "ibLocalSymbol"):
        value = metadata.get(key)
        normalised = _normalise_string(value)
        if normalised:
            simplified = re.sub(r"\s+", "", normalised).upper()
            if simplified:
                return simplified
    return None


def normalize_contract_local_symbol(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    simplified = re.sub(r"\s+", "", value).upper()
    return simplified or None


def extract_local_symbol_from_metadata(metadata: Mapping[str, Any]) -> str | None:
    return _extract_local_symbol(metadata)


def _coerce_reference_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(NEW_YORK_TZ).date()
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is not None:
            return parsed.astimezone(NEW_YORK_TZ).date()
        return parsed.date()
    return None


def _infer_security_type(symbol: str, sec_type: str | None, metadata: Mapping[str, Any]) -> str:
    metadata_type = _extract_metadata_sec_type(metadata)
    initial = _string_upper(sec_type) or metadata_type or "STK"

    if metadata_type and metadata_type != "STK":
        return metadata_type

    if initial and initial != "STK":
        return initial

    symbol_key = _string_upper(symbol) or ""
    if FUTURE_SYMBOL_CODE_PATTERN.match(symbol_key):
        return "FUT"

    # Index futures like ES/NQ/YM should be treated as futures even when
    # provided without an explicit contract month (e.g., "NQ"). The previous
    # length guard incorrectly excluded two-letter roots and caused STK
    # inference, leading to conId qualification failures for DOM/ticker/bar
    # subscriptions. Map all known index future roots to FUT here.
    if symbol_key in INDEX_FUTURE_SYMBOLS:
        return "FUT"

    root = _resolve_future_root(symbol, metadata)
    if root and root in VIX_FUTURE_ROOTS:
        return "FUT"

    # Heuristics: common crypto USD pairs like BTCUSD/ETHUSDT should map to CRYPTO.
    symbol_key = _string_upper(symbol) or ""
    if symbol_key and (
        re.match(r"^[A-Z]{2,6}(?:USD|USDT|USDC)$", symbol_key)
        or re.match(r"^[A-Z]{2,6}[-/](?:USD|USDT|USDC)$", symbol_key)
    ):
        return "CRYPTO"

    local_symbol = _extract_local_symbol(metadata)
    if local_symbol and FUTURE_SYMBOL_CODE_PATTERN.match(local_symbol):
        return "FUT"

    return "STK"


def infer_security_type(symbol: str, sec_type: str | None, metadata: Mapping[str, Any]) -> str:
    return _infer_security_type(symbol, sec_type, metadata)


def _index_future_root(symbol: str) -> str | None:
    token = (symbol or "").strip().upper()
    if not token:
        return None
    for candidate in INDEX_FUTURE_ROOTS:
        if token.startswith(candidate):
            return candidate
    return None


def resolve_index_future_root(symbol: str) -> str | None:
    return _index_future_root(symbol)


def _normalise_index_future_exchange(symbol: str, exchange: str | None) -> str:
    normalised_exchange = (exchange or "").upper()
    root = _index_future_root(symbol)
    if root is None:
        return normalised_exchange

    default_exchange = INDEX_FUTURE_DEFAULT_EXCHANGES.get(root, "CME")
    if not normalised_exchange:
        return default_exchange

    override = INDEX_FUTURE_EXCHANGE_OVERRIDES.get(normalised_exchange)
    if override is not None:
        return override or default_exchange

    if normalised_exchange == default_exchange:
        return normalised_exchange

    return default_exchange


def normalize_index_future_exchange(symbol: str, exchange: str | None) -> str:
    return _normalise_index_future_exchange(symbol, exchange)


def _third_friday(year: int, month: int) -> date:
    first_day = date(year, month, 1)
    days_until_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=days_until_friday)
    return first_friday + timedelta(days=14)


def _current_new_york_date() -> date:
    return datetime.now(NEW_YORK_TZ).date()


def _quarterly_rollover_date(expiry: date) -> date:
    return expiry - timedelta(days=4)


def _increment_month(year: int, month: int) -> tuple[int, int]:
    if month == 12:
        return year + 1, 1
    return year, month + 1


def _resolve_year_from_suffix(year_code: str, *, today: date | None = None) -> int | None:
    if not year_code or not year_code.isdigit():
        return None

    digits = len(year_code)
    suffix = int(year_code)
    if today is None:
        today = _current_new_york_date()

    if digits >= 4:
        return suffix

    current_year = today.year
    base = current_year - (current_year % (10**digits))
    year = base + suffix
    while year < current_year - 1:
        year += 10**digits
    return year


def _expiry_from_local_symbol(local_symbol: str, *, today: date | None = None) -> str | None:
    simplified = re.sub(r"\s+", "", local_symbol or "").upper()
    if not simplified:
        return None

    match = re.match(r"^[A-Z]+([FGHJKMNQUVXZ])(\d{1,4})$", simplified)
    if not match:
        return None

    month_code, year_code = match.groups()
    month = CONTRACT_MONTH_CODES.get(month_code)
    if month is None:
        return None

    year = _resolve_year_from_suffix(year_code, today=today)
    if year is None:
        return None

    return f"{year}{month:02d}"


def _nearest_quarterly_expiry(*, today: date | None = None, offset: int = 0) -> str:
    if today is None:
        today = _current_new_york_date()

    quarter_offset = max(0, int(offset))
    year = today.year
    while True:
        for month in QUARTERLY_CONTRACT_MONTHS:
            expiry = _third_friday(year, month)
            if today >= _quarterly_rollover_date(expiry):
                continue
            if quarter_offset:
                quarter_offset -= 1
                continue
            return expiry.strftime("%Y%m%d")
        year += 1


def _resolve_future_root(symbol: str, metadata: Mapping[str, Any]) -> str | None:
    candidates: tuple[Any, ...] = (
        symbol,
        metadata.get("resolved_symbol"),
        metadata.get("symbol"),
        metadata.get("root"),
        metadata.get("root_symbol"),
        metadata.get("rootSymbol"),
        metadata.get("trading_class"),
        metadata.get("tradingClass"),
    )

    for candidate in candidates:
        token = _string_upper(candidate)
        if not token:
            continue
        match = re.match(r"^([A-Z]{1,3})", token)
        if match:
            return match.group(1)
    return None


def _vix_monthly_settlement(year: int, month: int) -> date:
    next_year, next_month = _increment_month(year, month)
    third_friday_next_month = _third_friday(next_year, next_month)
    return third_friday_next_month - timedelta(days=30)


def _nearest_vix_expiry(*, today: date | None = None, offset: int = 0) -> str:
    if today is None:
        today = _current_new_york_date()

    month_offset = max(0, int(offset))
    year = today.year
    month = today.month

    while True:
        settlement = _vix_monthly_settlement(year, month)
        if settlement >= today:
            if month_offset == 0:
                return settlement.strftime("%Y%m%d")
            month_offset -= 1
        year, month = _increment_month(year, month)


def _infer_future_expiry(
    symbol: str,
    metadata: Mapping[str, Any],
    *,
    local_symbol: str | None = None,
    today: date | None = None,
) -> str:
    offset_value = metadata.get("expiry_offset") or metadata.get("front_month_offset")
    try:
        month_offset = int(offset_value) if offset_value is not None else 0
    except (TypeError, ValueError):
        month_offset = 0

    root = _resolve_future_root(symbol, metadata) or ""
    if root in VIX_FUTURE_ROOTS:
        return _nearest_vix_expiry(today=today, offset=month_offset)

    # Crypto futures typically list monthly contracts; infer the nearest month.
    if root in CRYPTO_FUTURE_ROOTS:
        return _nearest_monthly_contract_month(today=today, offset=month_offset)

    if symbol.upper() in INDEX_FUTURE_SYMBOLS:
        return _nearest_quarterly_expiry(today=today, offset=month_offset)

    if local_symbol:
        inferred = _expiry_from_local_symbol(local_symbol, today=today)
        if inferred:
            return inferred

    return ""


def infer_future_expiry(
    symbol: str,
    metadata: Mapping[str, Any],
    *,
    local_symbol: str | None = None,
    today: date | None = None,
) -> str:
    return _infer_future_expiry(
        symbol,
        metadata,
        local_symbol=local_symbol,
        today=today,
    )


def _nearest_monthly_contract_month(*, today: date | None = None, offset: int = 0) -> str:
    """Return the current or next monthly contract month in YYYYMM format.

    Used for non-index futures like CME crypto contracts where monthly listings
    are the norm. This avoids empty expiry fields that IB treats as unknown.
    """
    if today is None:
        today = _current_new_york_date()
    month_offset = max(0, int(offset))
    year = today.year
    month = today.month
    for _ in range(month_offset):
        year, month = _increment_month(year, month)
    return f"{year}{month:02d}"


def _contract_month_token(value: str | None) -> str | None:
    token = str(value or "").strip()
    if re.fullmatch(r"\d{8}", token):
        return token[:6]
    if re.fullmatch(r"\d{6}", token):
        return token
    return None


def _build_index_future_local_symbol(symbol: str, contract_month: str) -> str | None:
    match = re.fullmatch(r"(\d{4})(\d{2})", contract_month or "")
    if not match:
        return None
    year = int(match.group(1))
    month = int(match.group(2))
    month_codes = {
        1: "F",
        2: "G",
        3: "H",
        4: "J",
        5: "K",
        6: "M",
        7: "N",
        8: "Q",
        9: "U",
        10: "V",
        11: "X",
        12: "Z",
    }
    month_code = month_codes.get(month)
    if month_code is None:
        return None
    return f"{symbol}{month_code}{year % 10}"


__all__ = [
    "ContractDefaults",
    "ContractParams",
    "ContractBuildResult",
    "IBContractFactory",
    "INDEX_FUTURE_SYMBOLS",
    "INDEX_FUTURE_DEFAULT_EXCHANGES",
    "INDEX_FUTURE_EXCHANGE_OVERRIDES",
    "INDEX_FUTURE_ROOTS",
    "FUTURE_EXCHANGE_FALLBACKS",
    "FUTURE_SYMBOL_CODE_PATTERN",
    "extract_local_symbol_from_metadata",
    "infer_future_expiry",
    "infer_security_type",
    "normalize_contract_local_symbol",
    "normalize_index_future_exchange",
    "resolve_index_future_root",
]
