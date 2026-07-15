# Adding an adapter

1. Obtain approval for the phase design before implementation.
2. Assign a stable snake-case adapter/profile ID.
3. Implement Broker SDK 1.x using only public SDK imports.
4. Declare capabilities honestly and map vendor exceptions to SDK errors.
5. Add isolated contract, unit, packaging and no-main-repository-import tests.
6. Keep activation explicit and fail closed when the selected package is absent.
7. Document configuration, paper-only verification, rollback and release checks.

Do not add automatic provider fallback or live-order verification.
