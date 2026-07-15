# Adapter package template

A new adapter package must provide its own `pyproject.toml`, source package, tests,
README and changelog. It must expose exactly one allowlisted
`algo_trader.broker_adapters` entry point and depend only on the public Broker SDK,
not on the main application's `src` tree.
