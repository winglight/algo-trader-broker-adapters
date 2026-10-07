# Changelog

## 0.2.0

- Declare the unified `supports_screener` capability and IB native Screener metadata.
- Add Scanner-backed Screener discovery support.

## 0.1.0

- Initial isolated IBKR Paper package implementing Broker SDK protocol 1.0.

## Unreleased — V9.2 option reads

- Add explicit-account SPY/QQQ SMART discovery, conId qualification and live
  bid/ask snapshots on the current IB connection, without reconnect/retry.
- Isolate finite quote subscriptions from existing contract subscriptions.
- Keep full options protocol activation and trading certification disabled.

- Add request-scoped native account/position callbacks, exact USD funding fields,
  qualified option inventory and stable open-order references. Preserve unknown
  permission/cost-unit/source-completeness states and the evidence archive hook.

- Add guarded OPT/BAG limit preparation and one native send after durable Runner
  client/order-ID capture. Preserve signed BAG prices and exact leg actions.
- Retain/interpret initial native acknowledgements without fabricating fills;
  missing acknowledgements stay UNKNOWN and are never automatically resent.

- Retain ongoing scoped IB order/execution/commission callbacks before native
  wrapper deduplication; normalize original OPT fills and provisional USD fees.
  BAG summaries never generate financial legs; corrections remain unresolved.

- Add one guarded native cancellation after exact client/order-ID persistence.
- Add finite open/completed-order and account execution recovery downloads,
  including completedOrder evidence; retain source-window limitations and
  deduplicate only in the existing financial authority.

- Add owner-isolated live option quote iterators for Runner's existing leases.
- Add paginated native intraday bars/trade/bid-ask history, retaining same-second
  tick overflow and explicit partial archive coverage.
- Add fresh native contract calendars with explicit unknown broker cutoffs.

- Map native execId correction versions and their associated commissions into
  existing execution/fee revisions without losing the delivered IDs. Recovery
  counts only the latest execution version; commission finality remains unknown.
