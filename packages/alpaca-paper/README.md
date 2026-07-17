# Alpaca Paper adapter

Independent Alpaca Paper adapter for the algo-trader Broker SDK 1.x.

## Scope

- US equities and ETFs through Alpaca Paper only.
- Whole-share `MKT`, `LMT`, `STP`, and `STP LMT` orders with `DAY` or `GTC`.
- Account, positions, open/completed order reconciliation, fill activities,
  historical bars, snapshots, and live stock bars/trades/quotes.
- Futures, options, crypto, fractional shares, extended hours, order replacement,
  scanners, and market depth are rejected explicitly.

Alpaca does not currently provide futures trading through this API. This package
never converts a futures request into an equity request and never falls back to
another adapter or market-data feed.

## Configuration

The broker runner passes the following settings to the package:

```text
alpaca_api_key_id
alpaca_secret_key
alpaca_data_feed=iex
alpaca_request_timeout_seconds=15
alpaca_reconcile_lookback_hours=72
alpaca_max_concurrency=8
```

Production clients always use `paper=True`; no live trading base URL is
configurable. Credentials are required only when this adapter is selected.

## Development

Tests inject a fake backend and never access Alpaca:

```bash
PYTHONPATH=../broker-sdk/src:src pytest tests -q
```

Live credential probes, market-data shadowing, Paper orders, package publishing,
and remote pushes require separate approval in the main project.

The approved Phase 8 main-project acceptance keeps Alpaca Paper operations to
whole-share AAPL/SPY tests and an explicitly confirmed one-share TSLA long
cleanup. The acceptance driver must reject any symbol, side, quantity, live
endpoint, extended-hours request, or unapproved cancellation outside that
allowlist; an unknown submit outcome is reconciled by client order ID and is
never blindly retried.
