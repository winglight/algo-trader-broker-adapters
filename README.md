# algo-trader broker adapters

Independent, open-source broker adapter packages for the algo-trader Broker SDK.
Each adapter is installed and versioned separately. The repository does not contain
credentials and must not depend on the main application's private `src` package.

## Packages

- `packages/ibkr-paper`: IBKR Paper implementation (`ibkr_paper` entry point).
- `packages/alpaca-paper`: Alpaca Paper implementation (`alpaca_paper` entry point),
  limited to whole-share US equities and ETFs.

See `docs/adding-an-adapter.md` before creating another package.
