# Live Adapter test installation

> Experimental test workflow only. This repository does not publish or support
> a ready-to-use Live Adapter, and the official ATI Local Runtime does not
> allowlist unknown adapter profiles.

## Risk and liability notice

This procedure is solely for isolated software testing by developers who build
their own compatible Broker Runner test image and adapter package. It is not a
production trading guide, investment advice, brokerage service, or promise of
execution quality or profitability.

Connecting software to a live brokerage account can submit real orders and can
cause partial or total loss of capital. Broker behavior, permissions, market
data, latency, outages, strategy defects, configuration mistakes, duplicate or
missed orders, and third-party failures can all cause losses. You are solely
responsible for every account, credential, strategy, order, risk control,
deployment, regulatory obligation, fee, profit, and loss. To the fullest extent
permitted by law, the maintainers and contributors disclaim liability for real
trading activity, trading losses, lost profits, and direct, indirect,
incidental, or consequential damages.

Do not continue unless you understand and accept those risks. Use a broker
sandbox or Paper account whenever one is available.

## What this test path requires

A Live Adapter cannot be enabled by renaming a Paper Adapter or changing a
Paper credential. A compatible test setup requires all of the following:

1. A separately reviewed adapter package implementing Broker SDK 1.x.
2. A unique profile ID and matching
   `algo_trader.broker_adapters` Python entry point.
3. An adapter manifest whose `adapter_id` matches that profile ID and whose
   environment is explicitly `LIVE`.
4. A custom Broker Runner test build that explicitly adds the profile to its
   reviewed external-profile allowlist and parses only that profile's dedicated
   configuration namespace.
5. Fail-closed capability, account, order, reconciliation, idempotency, and risk
   tests completed without live credentials.

The official Local Runtime ignores unknown installed entry points. This is an
intentional safety boundary; do not patch around it at runtime.

## Build the test adapter package

Start from `templates/adapter-package/` and keep the new package isolated:

```text
packages/<broker>-live-test/
  pyproject.toml
  README.md
  src/algo_trader_broker_adapter_<broker>_live_test/
  tests/
```

Register exactly one entry point in the package's `pyproject.toml`:

```toml
[project.entry-points."algo_trader.broker_adapters"]
<broker>_live_test = "algo_trader_broker_adapter_<broker>_live_test:create_adapter"
```

Build a wheel only after its unit, contract, packaging, and import-isolation
tests pass:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip build pytest ruff
python -m pip install "algo-trader-broker-sdk>=1,<2"
python -m pip install -e packages/<broker>-live-test
pytest packages/<broker>-live-test/tests
ruff check packages/<broker>-live-test
python -m build --wheel packages/<broker>-live-test
```

Never put broker credentials in the package, wheel, image, repository, shell
history, test fixtures, or captured logs.

## Prepare a compatible Broker Runner test build

In a private test branch of the Broker Runner host:

1. Add `<broker>_live_test` to the explicit external-profile allowlist.
2. Set its entry point to the package module registered above.
3. Give it a dedicated settings namespace such as
   `BROKER_RUNNER_<BROKER>_LIVE_TEST_`.
4. Mark its environment as `LIVE`; never reuse `PAPER`.
5. Add strict configuration parsing with no defaults for account identity,
   endpoint, or credentials.
6. Require explicit capability and risk gates and reject incomplete
   configuration at startup.
7. Build and tag a separate test-only Broker Runner image. Do not replace the
   official image tag.

The exact host-code change is intentionally not provided as a bypass patch:
allowlisting a Live Adapter is a security review decision, not an installation
toggle.

## Install the verified wheel into an isolated Runtime checkout

Use a disposable Local Runtime checkout and replace the placeholders with the
reviewed wheel and profile ID:

```bash
mkdir -p data/broker-plugins
python3.11 -m pip install \
  --no-deps \
  --target data/broker-plugins \
  /absolute/path/to/algo_trader_broker_adapter_<broker>_live_test-<version>-py3-none-any.whl
```

Verify the package and entry point without credentials:

```bash
PYTHONPATH="$PWD/data/broker-plugins" python3.11 -c \
  'from importlib import metadata; print([(e.name, e.value) for e in metadata.entry_points().select(group="algo_trader.broker_adapters")])'
```

Configure the isolated checkout to use the separately tagged test Runner image:

```env
BROKER_RUNNER_IMAGE=<private-registry>/<broker-runner-test-image>:<immutable-tag>
BROKER_RUNNER_PROFILE_REGISTRY_ENABLED=true
BROKER_RUNNER_ENABLED_ADAPTERS=sim,<broker>_live_test
BROKER_RUNNER_DEFAULT_ADAPTER_ID=sim
BROKER_RUNNER_PLUGIN_PATH=/app/data/broker-plugins
```

Keep `sim` as the initial adapter. Supply credentials through permission-limited
secret files supported by your reviewed test build, not directly in `.env` or
command-line arguments.

## Required verification before any broker connection

- Confirm the image and wheel by immutable digest.
- Confirm the active adapter remains `sim`.
- Run contract, capability, idempotency, reconnect, duplicate-order,
  reconciliation, stale-data, kill-switch, and maximum-loss tests.
- Verify every unsupported product and order type fails closed.
- Verify logs and diagnostics redact credentials and account identifiers.
- Verify stopping the test Runner cannot leave an unmanaged working order.
- Obtain independent review of the adapter, host allowlist, risk controls, and
  broker permissions.

If any check fails or is uncertain, stop and remove the test package. This page
does not authorize or recommend connecting to a live account.
