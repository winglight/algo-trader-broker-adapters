# algo-trader broker adapters

Independent, open-source broker adapter packages for the algo-trader Broker SDK
1.x. Each adapter is installed, activated, and versioned separately. This
repository contains no broker credentials and must not depend on the main
application's private `src` package.

- Public repository:
  [winglight/algo-trader-broker-adapters](https://github.com/winglight/algo-trader-broker-adapters)
- ATI product website: [ati.broyustudio.com](https://ati.broyustudio.com)
- ATI Local Runtime:
  [winglight/algo-trader-ib](https://github.com/winglight/algo-trader-ib)

## Adapter list

| Adapter | Package and entry point | Intended use | Supported trading products | Main capabilities | Limits |
| --- | --- | --- | --- | --- | --- |
| Sim | Built into the official Broker Runner image; `sim` | Run deterministic local development, demonstrations, strategy validation, and end-to-end tests without a broker account or external market-data connection | Stocks: `AAPL`, `MSFT`, `NVDA`, `AMZN`, `META`, `GOOGL`, `TSLA`, `AMD`, `JPM`, `SPY`; index futures: `ES`, `MES`, `NQ`, `MNQ`, `YM`, `MYM`, `RTY`, `M2K` | Simulated account, positions, PnL, orders, historical bars, real-time prices, and tick-by-tick data; `MKT`, `LMT`, `STP`, and `STP LMT`; `DAY` and `GTC`; configurable seed, initial cash, commission, and slippage | Simulation only; fixed instrument set and generated market data; no broker connectivity, options, DOM, fractional quantities, partial fills, scanners, or order replacement; results do not represent broker execution or live-market performance |
| IBKR Paper | `packages/ibkr-paper`; `ibkr_paper` | Develop and validate workflows against an Interactive Brokers Paper account through IB Gateway | Stocks and futures | Account, positions and PnL; order submission/cancellation and reconciliation; historical and real-time market data; tick-by-tick data; scanner support; `MKT`, `LMT`, `STP`, and `STP LMT`; `DAY` and `GTC` | Paper only; no live trading, options, DOM, fractional quantities, or order replacement; requires an IBKR Paper account, IB Gateway, and any applicable market-data subscriptions |
| Alpaca Paper | `packages/alpaca-paper`; `alpaca_paper` | Develop and validate workflows against Alpaca Paper | Whole-share US stocks and ETFs | Account, positions and orders; fill reconciliation; historical bars, snapshots, and live stock bars/trades/quotes; `MKT`, `LMT`, `STP`, and `STP LMT`; `DAY` and `GTC` | Paper only; no futures, options, crypto, fractional shares, extended-hours orders, order replacement, scanners, or market depth; data feed must be explicitly set to `iex` or `sip` and requires the corresponding entitlement |

Capabilities are a compatibility boundary, not a promise that a broker will
accept a particular symbol or order. Broker permissions, subscriptions, market
hours, exchange rules, and vendor outages still apply. See the
[compatibility matrix](docs/compatibility-matrix.md) and each package README for
the current version-specific details.

`sim` is part of the official Broker Runner image and is always installed; it is
listed here because it is a selectable adapter, but its source package is not
published from this repository. Installing an IBKR Paper or Alpaca Paper package
does not activate it. The host application must explicitly select the matching
entry point and configuration. There is no automatic provider or market-data
fallback.

## Development

### Requirements

- Python 3.11 or later.
- A compatible `algo-trader-broker-sdk` 1.x installation.
- Broker credentials only for separately approved Paper-environment probes;
  unit and contract tests must not use real credentials or network access.

Create an isolated environment, install the SDK and the package being changed,
then run the repository checks:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip pytest ruff
python -m pip install -e ../packages/broker-sdk
python -m pip install -e packages/ibkr-paper -e packages/alpaca-paper
pytest
ruff check .
```

The `../packages/broker-sdk` path is the sibling SDK location in the standard
algo-trader workspace. For a standalone clone, replace it with the path to a
compatible Broker SDK 1.x checkout or install the SDK from your configured
package index:

```bash
python -m pip install "algo-trader-broker-sdk>=1,<2"
```

To add an adapter:

1. Read [Adding an adapter](docs/adding-an-adapter.md) and start from
   `templates/adapter-package/`.
2. Choose a stable snake-case adapter/profile ID and register it under the
   `algo_trader.broker_adapters` entry-point group.
3. Implement only against public Broker SDK 1.x types. Keep vendor code,
   configuration, and tests isolated inside the package.
4. Declare capabilities conservatively, map vendor errors to SDK errors, reject
   unsupported operations explicitly, and never add an automatic fallback.
5. Add unit, contract, packaging, and import-isolation tests. Tests must pass
   when the private main-application source is unavailable.
6. Document configuration, supported operations, hard limits, Paper-only
   verification, rollback, compatibility, and release checks.

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution rules and
[SECURITY.md](SECURITY.md) for private vulnerability reporting. Never commit
credentials, account identifiers, order payloads, or captured broker data.

## Codex skills

- [`develop-broker-adapter`](skills/develop-broker-adapter/SKILL.md): scaffold,
  implement, test, package, and document a new Broker SDK 1.x adapter.
- [`convert-adapter-to-live`](skills/convert-adapter-to-live/SKILL.md): convert
  an existing Paper adapter into a separate Live profile and install it into ATI
  Local Runtime without activation or real-order verification.

## Disclaimer

- This repository is intended only for software development, research,
  education, and simulated trading. It does not provide investment or trading
  advice, brokerage services, an offer or solicitation, or any guarantee of
  performance.
- Automated trading involves substantial risk, including software and
  configuration errors, stale or incomplete data, latency, connectivity
  failures, duplicate or missed orders, third-party failures, and loss of all
  capital. Paper-trading behavior and results may differ materially from live
  markets.
- Users must independently validate every adapter, strategy, order, permission,
  and risk control, and are solely responsible for compliance with applicable
  law, broker agreements, market-data licenses, and exchange rules. References
  to Interactive Brokers, Alpaca, or other third parties do not imply
  affiliation, endorsement, or warranty.
- The software and documentation are provided "as is" and "as available". To
  the fullest extent permitted by law, maintainers and contributors disclaim
  warranties and liability for trading losses, lost profits, lost data, and
  direct, indirect, incidental, or consequential damages arising from use of,
  or inability to use, these packages.

## User Agreement

By downloading, installing, modifying, accessing, or using this repository or
any adapter, you agree that:

1. You have legal capacity to accept these terms and authority over every
   account and data source you connect.
2. You will use the published adapters only for lawful Paper/simulated trading.
   You will not enable live trading or bypass capability checks, subscriptions,
   licensing, risk confirmations, security controls, or broker restrictions.
3. You are responsible for credentials, local and deployment security,
   configurations, strategies, orders, and all resulting activity.
4. You will comply with applicable law and with each broker's and data
   provider's separate terms, fees, permissions, and policies. Third-party
   services are outside the maintainers' control.
5. Copying, modification, and distribution are also governed by the
   [Apache License 2.0](LICENSE). ATI products or hosted services used with these
   adapters may have separate product and subscription terms.
6. If you do not accept these terms, do not download, install, access, or use
   the repository or its adapters.
