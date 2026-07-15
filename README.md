# algo-trader broker adapters

Independent, open-source broker adapter packages for the algo-trader Broker SDK.
Each adapter is installed and versioned separately. The repository does not contain
credentials and must not depend on the main application's private `src` package.

## Packages

- `packages/ibkr-paper`: IBKR Paper implementation (`ibkr_paper` entry point).
- `packages/alpaca-paper`: reserved for the approved Alpaca Paper implementation;
  Phase 3 intentionally contains documentation only.

See `docs/adding-an-adapter.md` before creating another package.
