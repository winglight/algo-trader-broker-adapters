"""Pinned alpaca-py transport: reads may retry, native writes never do."""

from alpaca.trading.client import TradingClient


class SingleAttemptTradingClient(TradingClient):
    """Keep SDK request/response handling without its delayed write retries.

    alpaca-py 0.43.5 ignores retry_attempts=0 and retries rate-limited requests
    internally. Setting this per request avoids mutating a shared retry setting
    while reads and writes run concurrently.
    """

    def __init__(self, *args, request_timeout_seconds: float, **kwargs):
        super().__init__(*args, **kwargs)
        self._write_timeout = request_timeout_seconds

    def _one_request(self, method, url, opts, retry):
        if method.upper() != "GET":
            retry = 0
            opts = {**opts, "timeout": self._write_timeout}
        return super()._one_request(method, url, opts, retry)
