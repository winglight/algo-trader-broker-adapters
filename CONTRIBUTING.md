# Contributing

Adapter changes must remain isolated to their package, use Broker SDK public types,
and include contract and unit tests. Do not commit broker credentials or captured
account payloads. A package must pass its tests in an environment where the main
algo-trader repository is not importable.
