# OKX Demo Perpetual Internal Context

Internal Phase 5 Paper-only execution context used exclusively by the public
`ccxt_crypto` adapter. This package has no broker-adapter entry point and cannot
be selected as a Runner profile.

The adapter is fail-closed and fixed to:

- OKX Demo Trading (`sandbox=true`, `live=false`)
- one-way/net position mode
- isolated margin
- 2x leverage
- dedicated execution and market-data targets under the unified credential boundary

It never sets account mode or leverage automatically. Startup reads the broker state back and
blocks readiness when it differs from the approved policy.
