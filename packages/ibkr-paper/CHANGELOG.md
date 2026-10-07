# Changelog

## 0.2.0

- Declare the unified `supports_screener` capability and IB native Screener metadata.
- Add Scanner-backed Screener discovery support.

## 0.1.0

- Initial isolated IBKR Paper package implementing Broker SDK protocol 1.0.

## Unreleased — V9.2 option reads

- Add explicit-account SPY/QQQ SMART discovery, conId qualification and live
  bid/ask snapshots on the current IB connection, without reconnect/retry.
- Isolate finite quote subscriptions from existing contract subscriptions.
- Keep full options protocol activation and trading certification disabled.
