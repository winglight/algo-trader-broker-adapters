# OKX Demo Perpetual Adapter

Phase 5 Paper-only adapter for BTC/USDT:USDT and ETH/USDT:USDT linear swaps.

The adapter is fail-closed and fixed to:

- OKX Demo Trading (`sandbox=true`, `live=false`)
- one-way/net position mode
- isolated margin
- 2x leverage
- a dedicated execution target, market-data target, and credential boundary

It never sets account mode or leverage automatically. Startup reads the broker state back and
blocks readiness when it differs from the approved policy.
