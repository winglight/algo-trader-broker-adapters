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
unclassified orders. This legacy filtering does not provide option backfill;
the independent raw activity acquisition path is described below.

`decode_option_event` now interprets supported trade update statuses and exact
single/multileg executions using Runner-retained qualifications. It cross-checks
native asset UUID, symbol, economic identity, account and child order UUID;
missing or ambiguous evidence stays unresolved. Parent net/cumulative prices
and quantities never create fills. Child client labels do not replace the ATI
parent reference; child status is separate from parent status. Full execution
IDs and decimal premiums survive unchanged. Native nanoseconds are floored to
the domain's microsecond timestamp; original timestamps remain in raw bytes.
No fee, correction revision, trade bust or lifecycle activity is inferred by
this codec. Those sources remain pending; original REST/WS execution evidence
and the Orders cross-reference path are described below.

The production adapter does not yet declare `options/1.0`. Runner still requires
the complete extension handshake, so this producer is currently verified using
synthetic extension fixtures. No read/order capability or Paper/Live
certification is implied. Enabling option contexts requires the remaining
extension methods and their scope checks; do not bypass that handshake.

Vendor references: [TradingStream](https://alpaca.markets/sdks/python/api_reference/trading/stream.html)
and [native trade update/option leg fields](https://docs.alpaca.markets/docs/websocket-streaming).

## V9.2 original activity acquisition (development)

`read_option_activity_page` requires the currently verified account scope and
returns one original response from `/v2/account/activities`, without filtering
activity types or converting numeric fields to floats. It reuses the adapter's
Paper credentials and fixed host, with `after`, `until`, `direction=asc`,
`page_size=100` and the complete `page_token`. Windows span at most 31 days.
The HTTP client enforces the request deadline and an 8 MiB response ceiling,
does not follow redirects, and makes no automatic retry.

After Runner persistence, `index_option_activity_page` indexes all records by
original UTF-8 byte spans and full activity type/ID. Unknown types are retained;
the same ID under different types is not collapsed. Invalid JSON, duplicate
keys, missing IDs, oversized pages or an echoed exclusive cursor are rejected.
A short page still has a continuation; only `[]` exhausts acquisition. Runner
owns the durable cursor, cross-page cycle detection and incomplete source marks.

The vendor's current [activities API](https://docs.alpaca.markets/us/reference/getaccountactivities-2)
defines the date bounds using creation time, which can differ from settlement
or trade time. New overlapping scans are required for late postings; finished
pagination is not financial completeness. The [activity object/pagination
reference](https://docs.alpaca.markets/us/docs/account-activities) does not by
itself prove that a FILL ID suffix is the WS execution UUID. This acquisition
path preserves the entire ID and creates no financial aliases or fills.
Original REST FILL interpretation is implemented below. Revisions, fees,
lifecycle and runtime scheduling remain in development; these stages do not
enable the full production option extension.

`read_option_order_evidence` retrieves bounded original bytes from the official
[order-by-ID endpoint](https://docs.alpaca.markets/us/reference/getorderbyorderid-1)
with `nested=true`, using the same verified Paper scope. The FILL codec receives
that retained order through a Runner resolver, checks order/child UUID, native
asset UUID, symbol and side, then resolves independent EXACT qualification.
The native activity supplies actual quantity/premium/time; parent net price,
cumulative fields and current order status do not produce activity fills or
statuses. Explicit stock order evidence yields non-option classification.

The native execution keeps its full REST ID. Canonical WS UUID executions and
REST FILLs additionally carry source and exact native leg evidence. Orders uses
that evidence with its frozen plan to verify cross-source aliases inside the
existing financial transaction; the adapter does not unconditionally strip ID
suffixes. Missing/ambiguous proof remains unresolved. REST-first, WS-first,
legacy WS and overlapping scans are covered by synthetic MariaDB integration;
they are not real account certification.

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
