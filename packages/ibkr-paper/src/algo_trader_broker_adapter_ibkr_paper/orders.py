"""Order helpers for building contracts and results."""


import re
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Literal, Tuple

from ib_async import (
    Future,
    LimitOrder,
    MarketOrder,
    Option,
    Order,
    Stock,
    StopLimitOrder,
    StopOrder,
)

from .contract_builder import (
    ContractParams,
    FUTURE_SYMBOL_CODE_PATTERN,
    IBContractFactory,
    INDEX_FUTURE_SYMBOLS,
    _resolve_year_from_suffix,
    infer_future_expiry,
    normalize_contract_local_symbol,
)

_INDEX_FUTURE_SYMBOLS: frozenset[str] = frozenset(INDEX_FUTURE_SYMBOLS)
_INDEX_FUTURE_PRICE_TICKS: dict[str, Decimal] = {
    "ES": Decimal("0.25"),
    "MES": Decimal("0.25"),
    "NQ": Decimal("0.25"),
    "MNQ": Decimal("0.25"),
    "YM": Decimal("1"),
    "MYM": Decimal("1"),
    "RTY": Decimal("0.1"),
    "M2K": Decimal("0.1"),
}


OrderSide = Literal["BUY", "SELL"]
OrderType = Literal["MKT", "LMT", "STP", "STP LMT"]


_CONTRACT_FACTORY = IBContractFactory()


@dataclass(slots=True)
class StockOrderRequest:
    """Parameters required to submit a stock order."""

    symbol: str
    side: OrderSide
    quantity: float
    order_type: OrderType
    limit_price: float | None = None
    stop_price: float | None = None
    exchange: str = "SMART"
    currency: str = "USD"
    primary_exchange: str | None = None
    local_symbol: str | None = None
    trading_class: str | None = None
    contract_id: int | None = None
    tif: str = "DAY"
    account: str | None = None
    client_order_id: str | None = None
    outside_rth: bool | None = None
    open_close: str | None = None
    order_id: int | None = None

    def build(self) -> Tuple[Stock, Order]:
        """Create the :class:`Stock` contract and :class:`Order`."""

        exchange = (self.exchange or "SMART").strip() or "SMART"
        currency = (self.currency or "USD").strip() or "USD"
        params = ContractParams(
            symbol=self.symbol,
            sec_type="STK",
            exchange=exchange,
            currency=currency,
            primary_exchange=self.primary_exchange,
            local_symbol=self.local_symbol,
            trading_class=self.trading_class,
        )
        result = _CONTRACT_FACTORY.build(params)
        contract = result.contract
        order = _build_order(
            side=self.side,
            quantity=self.quantity,
            order_type=self.order_type,
            limit_price=self.limit_price,
            stop_price=self.stop_price,
            tif=self.tif,
            account=self.account,
            client_order_id=self.client_order_id,
            outside_rth=self.outside_rth,
            open_close=self.open_close,
            order_id=self.order_id,
        )
        return contract, order


@dataclass(slots=True)
class FutureOrderRequest:
    """Parameters required to submit a futures order."""

    symbol: str
    side: OrderSide
    quantity: float
    order_type: OrderType
    limit_price: float | None = None
    stop_price: float | None = None
    last_trade_date: str = ""
    exchange: str = "CME"
    currency: str = "USD"
    multiplier: str | None = None
    primary_exchange: str | None = None
    local_symbol: str | None = None
    trading_class: str | None = None
    contract_id: int | None = None
    tif: str = "DAY"
    account: str | None = None
    client_order_id: str | None = None
    open_close: str | None = None
    order_id: int | None = None

    def build(self) -> Tuple[Future, Order]:
        """Create the :class:`Future` contract and :class:`Order`."""

        symbol = self.symbol
        exchange = (self.exchange or "CME").strip() or "CME"
        currency = (self.currency or "USD").strip() or "USD"
        explicit_last_trade_date = str(self.last_trade_date or "").strip()
        symbol, explicit_local_symbol = _normalize_future_symbol_inputs(
            symbol=symbol,
            local_symbol=self.local_symbol,
        )
        contract_month = _resolve_contract_month(
            symbol=symbol,
            local_symbol=explicit_local_symbol,
            last_trade_date=explicit_last_trade_date,
        )
        explicit_local_symbol = _align_local_symbol_with_contract_month(
            symbol=symbol,
            local_symbol=explicit_local_symbol,
            contract_month=contract_month,
        )

        params = ContractParams(
            symbol=symbol,
            sec_type="FUT",
            exchange=exchange,
            currency=currency,
            primary_exchange=self.primary_exchange,
            local_symbol=explicit_local_symbol,
            trading_class=self.trading_class,
            multiplier=self.multiplier,
            con_id=self.contract_id,
            last_trade_date_or_contract_month=contract_month,
        )
        result = _CONTRACT_FACTORY.build(params)
        contract = result.contract
        limit_price = _normalize_future_order_price(symbol, self.limit_price)
        stop_price = _normalize_future_order_price(symbol, self.stop_price)
        order = _build_order(
            side=self.side,
            quantity=self.quantity,
            order_type=self.order_type,
            limit_price=limit_price,
            stop_price=stop_price,
            tif=self.tif,
            account=self.account,
            client_order_id=self.client_order_id,
            open_close=self.open_close,
            order_id=self.order_id,
        )
        return contract, order


@dataclass(slots=True)
class OptionOrderRequest:
    """Parameters required to submit an options order."""

    symbol: str
    side: OrderSide
    quantity: float
    order_type: OrderType
    limit_price: float | None = None
    stop_price: float | None = None
    last_trade_date: str = ""
    strike: float | None = None
    right: str | None = None
    exchange: str = "SMART"
    currency: str = "USD"
    multiplier: str | None = None
    primary_exchange: str | None = None
    local_symbol: str | None = None
    trading_class: str | None = None
    tif: str = "DAY"
    account: str | None = None
    client_order_id: str | None = None
    outside_rth: bool | None = None
    open_close: str | None = None
    order_id: int | None = None

    def build(self) -> Tuple[Option, Order]:
        """Create the :class:`Option` contract and :class:`Order`."""

        exchange = (self.exchange or "SMART").strip() or "SMART"
        currency = (self.currency or "USD").strip() or "USD"
        params = ContractParams(
            symbol=self.symbol,
            sec_type="OPT",
            exchange=exchange,
            currency=currency,
            primary_exchange=self.primary_exchange,
            local_symbol=self.local_symbol,
            trading_class=self.trading_class,
            multiplier=self.multiplier,
            last_trade_date_or_contract_month=self.last_trade_date,
            strike=self.strike,
            right=self.right,
        )
        result = _CONTRACT_FACTORY.build(params)
        contract = result.contract
        order = _build_order(
            side=self.side,
            quantity=self.quantity,
            order_type=self.order_type,
            limit_price=self.limit_price,
            stop_price=self.stop_price,
            tif=self.tif,
            account=self.account,
            client_order_id=self.client_order_id,
            outside_rth=self.outside_rth,
            open_close=self.open_close,
            order_id=self.order_id,
        )
        return contract, order


@dataclass(slots=True)
class OrderResult:
    """Lightweight representation of an order response."""

    order_id: int
    status: str
    filled: float
    remaining: float
    avg_fill_price: float | None
    contract: dict[str, object]
    perm_id: str | None = None


def _build_order(
    *,
    side: OrderSide,
    quantity: float,
    order_type: OrderType,
    limit_price: float | None,
    stop_price: float | None,
    tif: str,
    account: str | None,
    client_order_id: str | None = None,
    outside_rth: bool | None = None,
    open_close: str | None = None,
    order_id: int | None = None,
) -> Order:
    action = side.upper()
    if order_type == "MKT":
        order = MarketOrder(action, quantity)
    elif order_type == "LMT":
        if limit_price is None:
            raise ValueError("Limit price must be provided for limit orders")
        order = LimitOrder(action, quantity, limit_price)
    elif order_type == "STP":
        if stop_price is None:
            raise ValueError("Stop price must be provided for stop orders")
        order = StopOrder(action, quantity, stop_price)
    elif order_type == "STP LMT":
        if stop_price is None:
            raise ValueError("Stop price must be provided for stop-limit orders")
        if limit_price is None:
            raise ValueError("Limit price must be provided for stop-limit orders")
        order = StopLimitOrder(action, quantity, limit_price, stop_price)
    else:  # pragma: no cover - defensive guard
        raise ValueError(f"Unsupported order type: {order_type}")

    if order_id is not None:
        order.orderId = int(order_id)
    order.tif = tif
    if account:
        order.account = account
    if client_order_id:
        order.orderRef = client_order_id
    if outside_rth is not None:
        order.outsideRth = bool(outside_rth)
    if open_close:
        order.openClose = str(open_close).strip().upper()
    return order


def _normalize_future_order_price(symbol: str, price: float | None) -> float | None:
    if price is None:
        return None
    root = (symbol or "").strip().upper()
    tick = _INDEX_FUTURE_PRICE_TICKS.get(root)
    if tick is None:
        return price
    value = Decimal(str(price))
    ticks = (value / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return float(ticks * tick)


def _normalize_future_symbol_inputs(
    *,
    symbol: str,
    local_symbol: str | None,
) -> tuple[str, str | None]:
    symbol_token = (symbol or "").strip().upper()
    local_symbol_token = normalize_contract_local_symbol(local_symbol)
    if FUTURE_SYMBOL_CODE_PATTERN.match(symbol_token):
        if local_symbol_token is None:
            local_symbol_token = symbol_token
        match = re.match(r"^([A-Z]{1,3})[FGHJKMNQUVXZ]\d{1,4}$", symbol_token)
        if match:
            symbol_token = match.group(1)
    return symbol_token or symbol, local_symbol_token


def _resolve_contract_month(
    *,
    symbol: str,
    local_symbol: str | None,
    last_trade_date: str,
) -> str:
    if last_trade_date:
        token = last_trade_date.strip()
        if re.fullmatch(r"\d{8}", token):
            return token
        if re.fullmatch(r"\d{6}", token):
            return token
        raise ValueError("last_trade_date must be YYYYMM or YYYYMMDD")

    if local_symbol:
        match = re.match(r"^[A-Z]{1,3}([FGHJKMNQUVXZ])(\d{1,4})$", local_symbol)
        if match:
            month_code, year_code = match.groups()
            month_map = {
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
            month = month_map.get(month_code)
            if month is None:
                raise ValueError("invalid month code in local_symbol")
            if len(year_code) >= 4:
                year = int(year_code)
            else:
                year = _resolve_year_from_suffix(year_code)
                if year is None:
                    raise ValueError("invalid year code in local_symbol")
            return f"{year:04d}{month:02d}"
    if symbol.upper() in _INDEX_FUTURE_SYMBOLS:
        inferred = infer_future_expiry(symbol, {}, local_symbol=None)
        if inferred:
            return inferred
    inferred = infer_future_expiry(symbol, {}, local_symbol=local_symbol)
    if inferred:
        return inferred
    raise ValueError(f"Unable to resolve contract month for FUT symbol: {symbol}")


def _is_index_future_symbol(symbol: str) -> bool:
    token = (symbol or "").strip().upper()
    if not token:
        return False
    return token in _INDEX_FUTURE_SYMBOLS


def _align_local_symbol_with_contract_month(
    *,
    symbol: str,
    local_symbol: str | None,
    contract_month: str,
) -> str | None:
    if symbol.upper() not in _INDEX_FUTURE_SYMBOLS:
        return local_symbol
    month_token = contract_month[:6] if len(contract_month) >= 6 else contract_month
    local_month = _parse_local_symbol_month(local_symbol)
    if local_month == month_token:
        return local_symbol
    return _build_index_future_local_symbol(symbol, month_token)


def _parse_local_symbol_month(local_symbol: str | None) -> str | None:
    token = normalize_contract_local_symbol(local_symbol)
    if token is None:
        return None
    match = re.match(r"^[A-Z]{1,3}([FGHJKMNQUVXZ])(\d{1,4})$", token)
    if not match:
        return None
    month_code, year_code = match.groups()
    month_map = {
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
    month = month_map.get(month_code)
    if month is None:
        return None
    if len(year_code) >= 4:
        year = int(year_code)
    else:
        resolved_year = _resolve_year_from_suffix(year_code)
        if resolved_year is None:
            return None
        year = resolved_year
    return f"{year:04d}{month:02d}"


def _build_index_future_local_symbol(symbol: str, contract_month: str) -> str | None:
    match = re.fullmatch(r"(\d{4})(\d{2})", contract_month or "")
    if not match:
        return None
    year = int(match.group(1))
    month = int(match.group(2))
    code_map = {
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
    month_code = code_map.get(month)
    if month_code is None:
        return None
    return f"{symbol.upper()}{month_code}{year % 10}"
