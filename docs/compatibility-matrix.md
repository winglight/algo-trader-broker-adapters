# Adapter compatibility matrix

| Package | Adapter version | Broker SDK | Vendor client | Environment | Asset classes |
| --- | --- | --- | --- | --- | --- |
| `algo-trader-broker-adapter-ibkr-paper` | 0.1.0 | `>=1,<2` | `ib_async>=2.0.1,<3` | Paper | STK, FUT |
| `algo-trader-broker-adapter-alpaca-paper` | 0.1.0 | `>=1,<2` | `alpaca-py==0.43.5` | Paper | STK, ETF |
| `algo-trader-broker-adapter-ccxt-crypto` | 0.1.0 | `>=1,<2` | `ccxt==4.5.56` | OKX Demo/Paper | CRYPTO_SPOT, CRYPTO_PERPETUAL |

## OKX Demo Phase 4/5 constraints

- One public `ccxt_crypto` adapter/Runner profile owns both Spot and Perpetual;
  its internal contexts keep targets, reconciliation generations, caches and
  readiness isolated.
- Spot supports BTC/USDT and ETH/USDT with `MKT`, `LMT` GTC, and cancel.
- Perpetual supports only BTC/USDT:USDT and ETH/USDT:USDT linear USDT swaps,
  one-way/net, isolated margin and fixed 2x leverage; `MKT`, `LMT`, GTC/IOC,
  cancel and bounded reduce-only are supported.
- Perpetual mark, index and funding use dedicated target-scoped streams; loss
  of any required stream blocks risk increase until full reconciliation.
- `set_sandbox_mode(True)`, `x-simulated-trading: 1`, and Demo WebSocket hosts
  are mandatory and cannot be disabled by configuration.
- Market quantity is base currency (`tgtCcy=base_ccy`); margin, borrowing,
  Spot margin/borrowing, transfers, withdrawals and Production endpoints are
  rejected. Runtime mode/leverage mutations are administrator-only and are not
  exposed through the adapter order API.
- Public, private, trading, and Market-order gates are independently disabled
  by default. Unknown submissions are reconciled and never blindly retried.

## Alpaca Phase 4 constraints

- Whole shares only; `MKT`, `LMT`, `STP`, and `STP LMT`; `DAY` and `GTC`.
- Paper trading is fixed in code. Live trading is not configurable.
- Market-data feed is exactly `iex` or `sip`; no entitlement fallback.
- Futures, options, crypto, extended-hours orders, replacement, scanner, and DOM
  are unsupported.
- A non-empty persisted `client_order_id` is required before submission.
- Vendor SDK upgrades require a new adapter patch version and contract tests.
