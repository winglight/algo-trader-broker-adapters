---
name: convert-adapter-to-live
description: Convert an existing algo-trader Paper broker adapter into a separate Live adapter package and integrate it into ATI Local Runtime without activating it. Use when creating a Live profile, changing vendor clients from Paper to production endpoints, adding Live credentials and safeguards, packaging and installing the plugin under data/broker-plugins, extending the runtime profile allowlist or installer, or validating a Live installation. This skill never authorizes real orders, adapter activation, account liquidation, or credential transmission; those require separate explicit user approval.
---

# Convert Adapter To Live

Create a separate Live adapter and install it inactive. Treat source conversion,
plugin installation, Live activation, and real-order verification as four
different authorization boundaries.

## Non-negotiable safety boundary

- Never mutate a Paper package into a dual-mode package.
- Never reuse the Paper profile ID, credentials, state, or client order ID
  namespace.
- Never default, fall back, or auto-switch to a Live endpoint.
- Never install by modifying the official Broker Runner image.
- Never write the active adapter selection during installation.
- Never expose credentials in CLI arguments, URLs, logs, diffs, tests, or
  generated artifacts.
- Never activate the Live profile, cancel or replace an order, flatten an
  account, or submit a real order without a separate, action-specific user
  instruction and the runtime safety gates succeeding.

If the repository terms or product agreement prohibit Live use, stop and require
those terms and release policy to be updated and approved before publishing or
installing the Live adapter.

## 1. Audit the Paper adapter

Read:

- the source package, tests, README, changelog, and `pyproject.toml`;
- `README.md`, `docs/compatibility-matrix.md`, `CONTRIBUTING.md`, and
  `SECURITY.md`;
- Broker SDK contracts;
- the local runtime profile registry, settings parser, installer, plugin lock,
  watchdog route, and adapter selector/help metadata.

Produce an inventory of Paper-only assumptions:

- fixed Paper client flags or base URLs;
- Paper-specific account identifiers and permissions;
- credential names;
- order types, products, time-in-force, extended-hours behavior, and data feeds;
- idempotency and reconciliation behavior;
- rate limits, entitlements, market sessions, and vendor error semantics;
- tests that assert Paper endpoints or reject Live configuration.

Do not edit until the intended Live scope and forbidden operations are explicit.

## 2. Create a separate Live package

Copy structure, not identity. Use names such as:

```text
packages/<broker>-live/
distribution: algo-trader-broker-adapter-<broker>-live
entry point: <broker>_live
environment: LIVE
```

Give the Live adapter its own:

- Python package and entry point;
- settings namespace and credential files;
- order/client-order ID namespace;
- state and reconciliation cursor;
- manifest, capabilities, version, README, changelog, tests, and release
  artifact.

Remove Paper-only client construction. Production endpoints must be fixed by
reviewed code or an exact allowlist; reject user-supplied arbitrary URLs. Do not
allow automatic Paper fallback.

## 3. Add Live safeguards

Fail closed unless all required Live settings are valid. At minimum:

- distinguish Live credentials from Paper credentials;
- require explicit Live environment configuration;
- validate account identity and expected environment using read-only calls;
- preserve idempotent order submission and unknown-outcome reconciliation;
- expose `environment=LIVE` in the manifest and UI;
- declare only actually implemented products and operations;
- reject unsupported options, fractional quantities, extended hours, replace,
  shorting, or other features before vendor delegation;
- redact account identifiers and secrets from diagnostics;
- retain the runtime's position, open-order, strategy, risk, and switch gates.

Do not add a code path that silently bypasses confirmation or safety gates.

## 4. Validate without real orders

Use fake clients for all automated tests. Cover:

- Live identity and exact production endpoint selection;
- rejection of Paper credentials/endpoints and arbitrary URLs;
- separate state and idempotency namespaces;
- read-only account/environment verification;
- capability accuracy and unsupported-operation rejection;
- order mapping with fakes only;
- unknown submit outcome reconciliation without blind retry;
- credential/log redaction;
- wheel build, entry-point discovery, and import isolation.

Run package tests and build a wheel in a clean environment. A Live credential
probe, if explicitly authorized, must remain read-only: identity, permissions,
positions, open orders, entitlements, and market-data access only. Do not submit
an order as an installation test.

## 5. Integrate ATI Local Runtime

Update the local runtime as one reviewed change:

1. Add the new profile ID to the explicit profile allowlist.
2. Add a dedicated settings parser and Live credential file variables.
3. Add the adapter and capability/help metadata to API and UI tests.
4. Extend the installer to accept the profile only through an explicit Live
   opt-in; never infer it from an existing Paper installation.
5. Pin the adapters commit/archive, wheel versions, architecture, and SHA-256
   values in the plugin lock.
6. Build in a temporary directory with `--no-index`, verify checksums,
   distribution versions, entry points, imports, `pip check`, registry
   discovery, and SBOM, then atomically replace `data/broker-plugins/`.
7. Pass secrets only through `0600` or `0400` files and persist only file-backed
   environment references.
8. Preserve `.env`, `middle/.env`, data volumes, logs, license state, current
   active selection, and existing Paper plugins.
9. Delegate any required Broker Runner restart to the watchdog.

The installed Live profile must appear as enabled/installed/configured but
inactive. Installation success is not activation approval.

## 6. Verify installation and rollback

Verify without changing the active adapter:

- installed distribution version and checksum;
- entry-point discovery from the persistent plugin directory;
- profile order, identity, environment, configuration status, and capabilities;
- health endpoints and UI selector/help content;
- absence of credentials in API responses, logs, Compose output, and SBOM;
- unchanged active profile and unchanged broker account/order state.

Prepare rollback before installation:

- back up candidate environment files and the previous plugin directory;
- restore them atomically on failure;
- restart through watchdog when required;
- confirm the previous active profile remains selected;
- retain a permission-restricted diagnostic directory without secrets.

## 7. Stop before activation

Report the installed profile, evidence, remaining risks, and rollback path. Stop
before clicking or calling activation.

If the user later explicitly requests activation, re-check:

- exact Live profile and account;
- zero or acknowledged positions and open orders under the runtime policy;
- no running strategies or deployments that block switching;
- risk limits and kill switch;
- target validation and switch gates;
- a separate confirmation immediately before the activation action.

Treat any real-order probe as another later action requiring exact symbol,
side, type, quantity, maximum notional, time-in-force, allowed cancellation,
cleanup plan, and immediate confirmation. Never infer that permission from this
skill or from installation approval.

## Completion criteria

Complete conversion and installation only when automated tests and packaging
pass, the Live profile is installed but inactive, the current adapter and
account state are unchanged, credentials are absent from artifacts, the UI
accurately labels `LIVE`, and rollback is documented and tested without real
orders.
