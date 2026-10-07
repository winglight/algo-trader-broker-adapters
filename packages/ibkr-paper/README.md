# IBKR Paper adapter

Package entry point: `algo_trader.broker_adapters:ibkr_paper`.

This package implements the same Broker SDK 1.x contract as the built-in
`ibkr_paper` adapter. Installing the package does not activate it. The main
application must explicitly set `BROKER_RUNNER_IBKR_PAPER_PROVIDER=package`.

Only IBKR Paper accounts are in scope. Live trading is not enabled by this package.

## V9.2 option read foundation

The existing adapter now has scoped `list_option_contracts`,
`qualify_option_contracts`, `option_snapshot`, `option_capabilities`,
`option_account_permissions`, and `option_account_state` ports.
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

The full `options/1.0` handshake remains disabled pending account certification, calendar,
order/event and reconciliation ports. Legacy option endpoints still reject
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
Original execution/commission callbacks are implemented below; corrections,
cancel/recovery, account certification and complete protocol activation remain pending. The same synthetic read/send
walkthrough verifies actual ib_async wire serialization and MariaDB preparation.

Native `openOrder`, `orderStatus`, `execDetails` and `commissionReport` callbacks
are copied before ib_async mutates/deduplicates them and drained into Runner's
durable handler with their captured account scope. Storage failure fences further
submissions. Original OPT executions preserve full `.01` execIds and actual
contract quantity/premium; BAG execution summaries supply only parent status.
USD commissions use the associated full execId and are provisional, with the
execution's native time as association time. Corrections, pending-price events,
unassociated commissions and unknown source coverage remain reconciliation work.
