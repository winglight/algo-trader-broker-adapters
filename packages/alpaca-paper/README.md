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

## V9.2 raw option evidence (development)

The existing trading connection now preserves native WebSocket bytes before
alpaca-py converts JSON numbers or constructs models. The receive loop is tested
against the pinned `alpaca-py==0.43.5`; authentication, subscriptions and sockets
remain the official SDK's. Stream failures propagate to this adapter's existing
recovery handler rather than reconnecting invisibly inside the SDK.

`set_option_event_handler(OptionVerifiedAccount, handler)` installs a Runner
verified, account-scoped durable sink. Each queued frame captures that sink at
native receipt time. Disconnect/rebind detaches future observations; delayed
frames keep their original scope. Persistence failures propagate as stream
failures. Raw bytes/full native IDs are preserved, and malformed bounded frames
remain unresolved evidence. Authentication frames are not journaled.

Only explicit US-equity frames use the old stock callback. Option parents,
children, and unclassified frames require the durable sink; they never become
stock fills. Legacy order/activity reconciliation likewise excludes options and
unclassified orders. This is **not** option backfill: REST pagination, source
watermarks, gap recovery and financial normalization are still pending.

The production adapter does not yet declare `options/1.0`. Runner still requires
the complete extension handshake, so this producer is currently verified using
synthetic extension fixtures. No read/order capability or Paper/Live
certification is implied. Enabling option contexts requires the remaining
extension methods and their scope checks; do not bypass that handshake.

Vendor references: [TradingStream](https://alpaca.markets/sdks/python/api_reference/trading/stream.html)
and [native trade update/option leg fields](https://docs.alpaca.markets/docs/websocket-streaming).

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
PYTHONPATH=src:../../../packages/broker-sdk/src pytest tests -q
```

Live credential probes, market-data shadowing, Paper orders, package publishing,
and remote pushes require separate approval in the main project.

The approved Phase 8 main-project acceptance keeps Alpaca Paper operations to
whole-share AAPL/SPY tests and an explicitly confirmed one-share TSLA long
cleanup. The acceptance driver must reject any symbol, side, quantity, live
endpoint, extended-hours request, or unapproved cancellation outside that
allowlist; an unknown submit outcome is reconciled by client order ID and is
never blindly retried.
