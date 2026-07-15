# Adapter compatibility matrix

| Package | Adapter version | Broker SDK | Vendor client | Environment | Asset classes |
| --- | --- | --- | --- | --- | --- |
| `algo-trader-broker-adapter-ibkr-paper` | 0.1.0 | `>=1,<2` | `ib_async>=2.0.1,<3` | Paper | STK, FUT |
| `algo-trader-broker-adapter-alpaca-paper` | 0.1.0 | `>=1,<2` | `alpaca-py==0.43.5` | Paper | STK, ETF |

## Alpaca Phase 4 constraints

- Whole shares only; `MKT`, `LMT`, `STP`, and `STP LMT`; `DAY` and `GTC`.
- Paper trading is fixed in code. Live trading is not configurable.
- Market-data feed is exactly `iex` or `sip`; no entitlement fallback.
- Futures, options, crypto, extended-hours orders, replacement, scanner, and DOM
  are unsupported.
- A non-empty persisted `client_order_id` is required before submission.
- Vendor SDK upgrades require a new adapter patch version and contract tests.
