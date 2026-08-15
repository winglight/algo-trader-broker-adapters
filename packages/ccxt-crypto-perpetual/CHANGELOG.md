# Changelog

## 0.1.0

- Add the Phase 5 internal OKX Demo Trading context for BTC and ETH USDT-linear perpetuals.
- Expose it only through the single public `ccxt_crypto` adapter/profile.
- Enforce Paper-only, one-way/net positions, isolated margin, fixed 2x leverage,
  target-scoped V2 orders, conservative reduce-only projection, private streams,
  REST reconciliation, perpetual risk snapshots, and funding ledger mapping.
