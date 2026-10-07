# IBKR Paper adapter

Package entry point: `algo_trader.broker_adapters:ibkr_paper`.

This package implements the same Broker SDK 1.x contract as the built-in
`ibkr_paper` adapter. Installing the package does not activate it. The main
application must explicitly set `BROKER_RUNNER_IBKR_PAPER_PROVIDER=package`.

Only IBKR Paper accounts are in scope. Live trading is not enabled by this package.

## V9.2 option read foundation

The existing adapter now has scoped `list_option_contracts`,
`qualify_option_contracts`, `option_snapshot`, `option_capabilities`,
`option_account_permissions`, `option_account_state`, `stream_option_quotes`,
`option_history` and `option_calendar` ports.
They require Runner's verified Paper account and the current connection; reads
never reconnect or replay themselves. Discovery uses bounded SPY/QQQ SMART
parameters and exact ContractDetails qualification. Bindings retain conId,
localSymbol and a metadata hash. The P0 standard-product rule is explicit and
versioned; unknown/adjusted roots and nonstandard multipliers are excluded.
IB ContractDetails is not a full deliverable or account-permission record.

`IBKR_LIVE` snapshots own their native subscription IDs. They use received bid
and ask tick times separately, require a native market-data-type callback of 1
for executable quote quality, and leave unverified Greeks empty. Tick times are
socket receipt times, not exchange timestamps. Reading does not certify trading.

The full `options/1.0` handshake remains disabled pending the remaining original
protocol ports and account/source certification. Legacy option endpoints still reject
requests. Local's compatibility entrypoint now delegates to this package while
preserving its existing manifest entrypoint. The synthetic read walkthrough uses
`ib_async 2.0.1`; no Gateway connection or real order is part of that check.

Account observations use explicit-account `reqAccountUpdatesMulti` and
`reqPositionsMulti` request IDs/end markers, then a bounded all-open-API-order
download. Callback observations are retained through the existing evidence
archive when supplied. USD `AvailableFunds` is kept separate from stock
`BuyingPower`; position conIds are qualified again. Original `avgCost` stays in
its unverified unit until source certification. Native permIds are stable order
references; missing permIds preserve client/order/generation identity. Manual
order visibility, executions, lifecycle and commission coverage remain explicitly
incomplete. These reads do not grant option approval or enable trading.

## Guarded option submission

The existing adapter builds exact OPT and BAG limit orders behind Runner's
single-use gate. Runner supplies the already verified execution shape/tick;
there is no unguarded option writer. BAG parents always BUY with the original
signed limit, actual leg actions/ratios and retail `openClose=0`; the parent
retains O/C. No NonGuaranteed fallback or native replacement is introduced.

Before socket I/O, Runner durably stores the native client/order IDs and complete
prepared request. The gate consumes only after persistence and current authority
checks, with a free native transport slot. Lost responses remain UNKNOWN and
are not replayed. Initial openOrder/error observations are retained; acknowledged
parents use permId and BAG legs use the native permId:conId pair. Initial ACK
normalization creates links/status only, never per-leg executions from BAG totals.
Original execution/commission callbacks and guarded cancellation/native recovery
reads are implemented below; corrections, source coverage, account certification
and complete protocol activation remain pending. The same synthetic read/send
walkthrough verifies actual ib_async wire serialization and MariaDB preparation.

Native `openOrder`, `orderStatus`, `execDetails` and `commissionReport` callbacks
are copied before ib_async mutates/deduplicates them and drained into Runner's
durable handler with their captured account scope. Storage failure fences further
submissions. Original OPT executions preserve full `.01` execIds and actual
contract quantity/premium; BAG execution summaries supply only parent status.
USD commissions use the associated full execId and are provisional, with the
execution's native time as association time. Corrections, pending-price events,
unassociated commissions and unknown source coverage remain reconciliation work.

Guarded cancellation reads the current native parent, checks permanent ID,
account, original command economics and current API client ownership, then
retains the exact cancel client/order IDs before one native cancelOrder call.
REQUESTED is not a terminal state. No global cancellation or reconnect/retry
helper is used. Order recovery downloads all-open API orders, completed API
orders and account-filtered executions, retaining native callbacks even when
ib_async would suppress duplicate execution events. Completed-order responses
lack API client/order IDs; those missing IDs are not fabricated. Exact actual
leg executions supply quantities; a filled BAG summary alone stays unresolved.
History/manual visibility/fee/lifecycle coverage remains explicitly incomplete.

## Option market data

Live streams use the same bid/ask normalization as snapshots, with separate
native reqIds per owner. Runner owns lease expiry/heartbeat; closing one iterator
cancels only its tickers. Each stream stays on its verified connection and keeps
only the latest sides per contract. A model/last tick cannot refresh their age.

History requires `provider_native`, raw adjustment and exact qualified contracts.
Native intraday bars (1/2/3/5/10/15/20/30 minutes; 1/2/3/4/8 hours), trade ticks
and bid/ask ticks are supported. Requests are serialized and paced on the current
adapter connection; one native chunk is read per page, with bounded five-minute
cursors tied to the query and binding. Pages retain every tick in the native final
second. `complete` ends pagination, while coverage remains PARTIAL/UNAVAILABLE.
Expired options and option daily bars are unsupported by IB; this port is not an
options archive. Historical quotes always have RESEARCH_ONLY quality.

Calendars refresh the exact ContractDetails and use `liquidHours` in explicit
Eastern time for RTH bounds and CLOSED dates. Both native hours fields and zone
contribute to the revision hash. Missing dates/split sessions are not inferred;
entry, close and exercise broker cutoffs remain unknown with reason codes.

Sources: [IB historical limits](https://interactivebrokers.github.io/tws-api/historical_limitations.html),
[historical ticks](https://interactivebrokers.github.io/tws-api/historical_time_and_sales.html),
[ContractDetails](https://interactivebrokers.github.io/tws-api/classIBApi_1_1ContractDetails.html).
The existing synthetic native walkthrough covers these reads alongside the same
submission/cancel/recovery flow; it does not access a Gateway or place real orders.
