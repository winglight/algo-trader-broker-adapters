# IBKR extraction source map

The initial package was mechanically extracted from the following main-repository
modules so parity can be audited:

| Package module | Original module |
| --- | --- |
| `adapter.py` | `src/broker_adapters/ibkr_paper/adapter.py` |
| `client.py` | `src/ib/client.py` |
| `settings.py` | `src/common/ib_gateway.py` |
| `supervisor.py` | `src/ib/supervisor.py` |
| `orders.py` | `src/ib/orders.py` |
| `contract_builder.py` | `src/common/broker/contract_builder.py` |
| `market_data.py` | `src/ib/market_data.py` |

Package-local imports and Broker SDK error/DTO imports are the intended boundary
changes. Functional changes require explicit parity-test updates.
