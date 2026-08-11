# CCXT Crypto Paper adapter

`ccxt_crypto` is an independently installable Broker SDK 1.x package for the
Phase 4 OKX Demo Trading Spot integration. Version 0.1.0 is deliberately fixed
to OKX Demo and cannot be configured for Production.

## Supported scope

- Environment: `PAPER`; OKX Demo Trading only.
- Instruments: `BTC/USDT`, `ETH/USDT` Spot.
- Orders: Market, Limit GTC, and Cancel through the Broker V2 endpoints.
- Data: ticker, trades, OHLCV, orders, fills, and balances.
- Reconciliation: open/closed orders, trades, balances, and Spot positions.

Margin, borrowing, derivatives, transfer, withdrawal, deposits, order replace,
stop orders, scanners, market depth, provider fallback, and Production endpoints
are rejected before a vendor mutation.

## Required safety properties

- The exchange ID is fixed to `okx`.
- `set_sandbox_mode(True)` is executed immediately after exchange creation.
- Every Demo REST request requires `x-simulated-trading: 1`.
- Demo WebSockets must use `wspap.okx.com`; `ws.okx.com` is rejected.
- Spot orders always use `tdMode=cash`; Market amount is base currency through
  `tgtCcy=base_ccy`.
- Unknown create outcomes are reconciled by `clOrdId` and never blindly retried.
- Installation does not enable public data, private reads, trading, or Market
  orders.

## Configuration

The host application translates its `BROKER_RUNNER_CCXT_CRYPTO_*` environment
variables into the package settings below. Secrets must be injected by a secret
store and must never appear in a tracked file.

| Setting | Default | Notes |
| --- | --- | --- |
| `exchange_id` | `okx` | Other values rejected |
| `sandbox` / `live` | `true` / `false` | Immutable safety boundary |
| `api_key`, `secret`, `passphrase` | empty | All required for an installed OKX Demo profile |
| `allowed_symbols` | BTC/USDT, ETH/USDT | Non-empty subset only |
| `execution_target_id` | `okx-spot-demo-paper-1` | Immutable Phase 4 identity |
| `market_data_target_id` | `okx-spot-demo-market-1` | Immutable Phase 4 identity |

## Offline verification

Tests use a fake CCXT exchange and make no network calls:

```bash
PYTHONPATH=../algo-trader/packages/broker-sdk/src \
  pytest packages/ccxt-crypto/tests -q
ruff check packages/ccxt-crypto
python -m build packages/ccxt-crypto
```

The installed profile uses its configured OKX Demo credentials and exposes the
real market, account, and order contracts without test-only feature gates.

## Rollback

Cancel and reconcile open orders, detach deployment bindings, revoke the Demo
key, then stop the original Broker Runner service. Never reuse or redirect this
package to Production.
