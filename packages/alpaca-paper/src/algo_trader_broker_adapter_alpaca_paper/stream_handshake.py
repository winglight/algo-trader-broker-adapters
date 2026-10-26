"""Preserve safe control-frame failures at the pinned alpaca-py boundary.

alpaca-py 0.43.5 has no handshake parser hook. Its private _connect/_auth
implementations discard numeric error codes. This mixin preserves their
connector, headers, parameters and authentication exchange; SDK upgrades must
run the offline handshake compatibility tests before changing this boundary.
"""

from algo_trader_broker_sdk import BrokerError


# Fixed codes from https://docs.alpaca.markets/us/docs/streaming-market-data .
# Unknown numeric values and provider message text are never retained.
NATIVE_STREAM_REASONS = {
    400: "INVALID_SYNTAX",
    401: "NOT_AUTHENTICATED",
    402: "AUTHENTICATION_FAILED",
    403: "ALREADY_AUTHENTICATED",
    404: "AUTHENTICATION_TIMEOUT",
    405: "SYMBOL_LIMIT",
    406: "CONNECTION_LIMIT",
    407: "SLOW_CLIENT",
    409: "INSUFFICIENT_SUBSCRIPTION",
    410: "SUBSCRIBE_ACTION_UNSUPPORTED",
    500: "PROVIDER_INTERNAL_ERROR",
}


def require_handshake_ack(raw, expected, phase):
    from alpaca.data.live import websocket as sdk

    messages = sdk.msgpack.unpackb(raw)
    first = messages[0]
    if first.get("T") == "error":
        native_code = first.get("code")
        known = type(native_code) is int and native_code in NATIVE_STREAM_REASONS
        details = {"recovery": "SOURCE_CONFIGURATION_CHANGED", "phase": phase,
            "reason": NATIVE_STREAM_REASONS[native_code] if known else "NATIVE_ERROR_UNCLASSIFIED"}
        if known:
            details["native_code"] = native_code
        if known and native_code == 406:
            code, message = "MARKET_DATA_CONNECTION_LIMIT", "Alpaca market-data connection limit exceeded"
        elif known and native_code == 409:
            code, message = "FEED_NOT_ENTITLED", "Alpaca market-data feed is not entitled"
        else:
            code, message = "MARKET_DATA_STREAM_REJECTED", "Alpaca market-data handshake was rejected"
        raise BrokerError(message, code=code, details=details)
    if first.get("T") != "success" or first.get("msg") != expected:
        raise ValueError("connected message not received" if expected == "connected" else "failed to authenticate")


class SingleAttemptMarketHandshake:
    """Minimal private-method compatibility override for alpaca-py 0.43.5."""

    async def _connect(self):
        from alpaca.data.live import websocket as sdk

        self._ws = await sdk.websockets_legacy.connect(
            self._endpoint,
            extra_headers={"Content-Type": "application/msgpack", "User-Agent": "APCA-PY/" + sdk.__version__},
            **self._websocket_params,
        )
        require_handshake_ack(await self._ws.recv(), "connected", "CONNECT")

    async def _auth(self):
        from alpaca.data.live import websocket as sdk

        await self._ws.send(sdk.msgpack.packb({"action": "auth", "key": self._api_key, "secret": self._secret_key}))
        require_handshake_ack(await self._ws.recv(), "authenticated", "AUTH")
