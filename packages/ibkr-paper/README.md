# IBKR Paper adapter

Package entry point: `algo_trader.broker_adapters:ibkr_paper`.

This package implements the same Broker SDK 1.x contract as the built-in
`ibkr_paper` adapter. Installing the package does not activate it. The main
application must explicitly set `BROKER_RUNNER_IBKR_PAPER_PROVIDER=package`.

Only IBKR Paper accounts are in scope. Live trading is not enabled by this package.
