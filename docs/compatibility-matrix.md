# Adapter compatibility matrix

| Package | Adapter version | Broker SDK | Vendor client | Environment | Asset classes |
| --- | --- | --- | --- | --- | --- |
| Built-in `projectx_topstep` profile ([main repository](https://github.com/winglight/algo-trader/tree/main/src/broker_adapters/projectx_topstep)) | 0.1.0 dry-run; 0.2.0 read-only | Built into the matching Local Runtime release | ProjectX REST + SignalR protocol | Local dry-run or provider read-only in public installations | FUT (MNQ only) |
| `algo-trader-broker-adapter-ibkr-paper` | 0.2.0 | `>=1,<2` | `ib_async>=2.0.1,<3` | Paper | STK, FUT |
| `algo-trader-broker-adapter-alpaca-paper` | 0.1.0 | `>=1,<2` | `alpaca-py==0.43.5` | Paper | STK, ETF |
| `algo-trader-broker-adapter-ccxt-crypto` | 0.1.0 | `>=1,<2` | `ccxt==4.5.56` | OKX Demo/Paper | CRYPTO_SPOT, CRYPTO_PERPETUAL |

## ProjectX / Topstep public constraints

- `projectx_topstep` is a built-in controlled profile in the main
  `winglight/algo-trader` repository, not an independently installable package
  from this adapter repository.
- The public installer in
  [`winglight/algo-trader-ib`](https://github.com/winglight/algo-trader-ib)
  exposes only `dry_run` and provider `read_only`.
- `dry_run` keeps account, order, fill, fee, and Topstep-style risk state local
  and does not call provider Order, Position, or Account APIs.
- `read_only` authenticates to one exact ProjectX account, reconciles provider
  state, and observes the active MNQ contract. All place, cancel, modify, close,
  and other mutation requests fail with a stable read-only error; it creates no
  local simulated fills.
- Public configuration forces live execution, provider mutation activation,
  remote execution, and local-device-attestation bypass off. Mode or connection
  failure is terminal and never falls back to dry-run or another adapter.
- Username, API key, account identity, tokens, and full provider responses must
  not appear in Git, manifests, reports, logs, screenshots, or support tickets.

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

## IB V9.2 option foundation

The existing IB package includes verified-account, bounded SPY/QQQ contract
reads and `IBKR_LIVE` snapshots. The direct read flow is fixture-verified with
`ib_async 2.0.1`; the declared trading asset classes above are unchanged. Full
`options/1.0` activation and account/Greeks/calendar/source certification remain
pending; implemented native order/read ports are detailed below. Local's legacy entrypoint delegates
to the package and retains its manifest identity.

The IB read foundation also maps fresh account funds, net option inventory and
API open-order references into the public Account DTOs. AvailableFunds and stock
BuyingPower stay distinct. Option approval, native cost units, manual-order
visibility and execution/lifecycle/commission completeness are not inferred.

IB native OPT/BAG preparation and guarded send are implemented with synthetic
wire/database verification. Complete options activation remains disabled pending
remaining original protocol ports and account/route/source certification.
Initial ACKs produce permanent-parent and compound-leg links/status only.

IB's same native walkthrough now continues through parent statuses, actual OPT
executions and associated commissions. Original execution IDs remain whole and
BAG aggregate executions stay nonfinancial. Native revisions and full
source certification are still pending; no account activation is implied.

IB also supports scoped guarded cancellation and finite native recovery reads.
Its native walkthrough continues through partial fill, cancel and cache-free
order/execution downloads; it does not claim complete historical visibility or
account certification.

IB market data also supplies owner-isolated quote iterators, native intraday
bar/trade/bid-ask history with query-bound pagination, and refreshed contract RTH
calendars. Historical coverage stays partial and unknown broker cutoffs remain
explicit; these implemented ports do not certify an account or trading shape.
