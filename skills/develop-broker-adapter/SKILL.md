---
name: develop-broker-adapter
description: Create a new isolated broker adapter package for the algo-trader Broker SDK 1.x. Use when adding a broker or data-provider integration, scaffolding a package under packages/, defining a new adapter entry point and profile ID, implementing SDK contracts, declaring capabilities and restrictions, or preparing contract, packaging, and import-isolation tests. Use convert-adapter-to-live instead when turning an existing Paper adapter into a Live adapter.
---

# Develop Broker Adapter

Create a separately versioned package that uses only the public Broker SDK,
fails closed for unsupported operations, and can be installed without activating
it.

## 1. Inspect the contracts

Read these repository files before editing:

- `README.md`
- `docs/adding-an-adapter.md`
- `docs/compatibility-matrix.md`
- `CONTRIBUTING.md`
- `SECURITY.md`
- `templates/adapter-package/`
- the closest existing package under `packages/`
- the public Broker SDK types and protocol for the target SDK version

Do not copy imports from the main application's private `src` package.

## 2. Define the adapter

Resolve these items from the request and vendor documentation:

- stable snake-case adapter/profile ID;
- distribution and Python package names;
- environment: `SIMULATED`, `PAPER`, or another non-Live environment;
- supported trading products, order types, time-in-force values, account
  features, and market-data streams;
- explicit non-goals and vendor entitlement requirements;
- credential names and whether each is required;
- vendor SDK version range and Python version;
- order idempotency, reconciliation, reconnect, and unknown-outcome behavior.

Use a separate profile ID for each materially different environment. Do not
introduce a Live endpoint in a Paper adapter. For Live work, stop and use
`convert-adapter-to-live`.

## 3. Scaffold the package

Start from `templates/adapter-package/` and create:

```text
packages/<adapter-id>/
  pyproject.toml
  README.md
  CHANGELOG.md
  src/<python_package>/
  tests/
```

Register exactly one reviewed entry point:

```toml
[project.entry-points."algo_trader.broker_adapters"]
<adapter_id> = "<python_package>:create_adapter"
```

Pin or bound vendor dependencies. Declare Broker SDK compatibility explicitly,
for example `algo-trader-broker-sdk>=1,<2`.

## 4. Implement fail-closed behavior

Implement the Broker SDK lifecycle, manifest, capabilities, account, order, and
market-data methods required by the intended scope.

- Report only implemented capabilities.
- Validate settings before creating network clients.
- Map vendor failures to public SDK errors without exposing secrets.
- Require a persisted client order ID before submission when the vendor
  supports idempotency.
- Reconcile unknown submit outcomes by client order ID; never blindly retry.
- Reject unsupported products and operations before invoking the vendor SDK.
- Do not add automatic broker, endpoint, entitlement, or data-feed fallback.
- Keep credentials out of URLs, command lines, logs, exceptions, fixtures, and
  repository files.

Installing the distribution must not select or start the adapter.

## 5. Test in isolation

Add tests for:

- manifest identity, protocol, version, environment, and capabilities;
- lifecycle and reconnect behavior;
- account, position, order, fill, cancellation, and reconciliation mapping;
- supported order/product combinations;
- every declared unsupported operation;
- vendor error mapping and unknown submit outcomes;
- entry-point discovery and wheel installation;
- import isolation with the main repository unavailable;
- credential and log redaction.

Unit and contract tests must use fakes and make no broker network calls. Run the
repository checks from its root:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip pytest ruff build
python -m pip install -e ../packages/broker-sdk
python -m pip install -e packages/<adapter-id>
pytest packages/<adapter-id>/tests -q
ruff check packages/<adapter-id>
python -m build packages/<adapter-id>
```

Adapt the SDK path for a standalone checkout. Also test the built wheel in a
clean environment.

## 6. Document and integrate

Update:

- the package README with purpose, configuration, supported products,
  capabilities, limits, verification, and rollback;
- `docs/compatibility-matrix.md`;
- the repository adapter list;
- release notes or package changelog.

If the local trading system must discover the adapter, separately update its
reviewed profile allowlist, settings parser, installer lock/checksums, UI help
metadata, and tests. Do not activate the profile or submit a broker order as
part of normal development or installation.

## Completion criteria

Finish only when the package builds, isolated tests pass, capability claims
match implemented methods, documentation states the hard limits, credentials
remain absent from artifacts, and installation remains inactive by default.
