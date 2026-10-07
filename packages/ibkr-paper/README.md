# IBKR Paper adapter

Package entry point: `algo_trader.broker_adapters:ibkr_paper`.

This package implements the same Broker SDK 1.x contract as the built-in
`ibkr_paper` adapter. Installing the package does not activate it. The main
application must explicitly set `BROKER_RUNNER_IBKR_PAPER_PROVIDER=package`.

Only IBKR Paper accounts are in scope. Live trading is not enabled by this package.

## V9.2 option read foundation

The existing adapter now has scoped `list_option_contracts`,
`qualify_option_contracts`, `option_snapshot`, and `option_capabilities` ports.
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

The full `options/1.0` handshake remains disabled pending the account, calendar,
order/event and reconciliation ports. Legacy option endpoints still reject
requests. Local's compatibility entrypoint now delegates to this package while
preserving its existing manifest entrypoint. The synthetic read walkthrough uses
`ib_async 2.0.1`; no Gateway connection or real order is part of that check.
