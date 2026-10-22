"""Coordinate native reads using the provider's actual quota headers."""

import asyncio
from contextlib import asynccontextmanager
from email.utils import parsedate_to_datetime
import math
import time


class NativeReadPacing:
    def __init__(self, *, clock=time.monotonic, wall_clock=time.time, sleep=asyncio.sleep):
        self.lock = asyncio.Lock()
        self.clock, self.wall_clock, self.sleep = clock, wall_clock, sleep
        self.not_before = 0.0
        self.failures = 0

    @asynccontextmanager
    async def slot(self):
        async with self.lock:
            remaining = self.not_before-self.clock()
            if remaining > 0:
                await self.sleep(remaining)
            yield

    def observed(self, headers, status):
        delay = 0.0
        try:
            remaining = int(headers['x-ratelimit-remaining'])
            until_reset = float(headers['x-ratelimit-reset'])-self.wall_clock()
            if remaining >= 0 and math.isfinite(until_reset) and until_reset > 0:
                delay = until_reset/max(1, remaining)
        except (KeyError, TypeError, ValueError, OverflowError):
            pass
        if status == 429:
            self.failures += 1
            # A failed read is returned to its caller once. This schedules the
            # NEXT independent read; it never replays that call or an order.
            delay = max(delay, min(30.0, 2.0**min(self.failures-1, 5)))
            retry = headers.get('retry-after')
            if retry is not None:
                try:
                    retry_delay = float(retry)
                except (TypeError, ValueError):
                    try:
                        retry_delay = parsedate_to_datetime(retry).timestamp()-self.wall_clock()
                    except (TypeError, ValueError, OverflowError):
                        retry_delay = 0
                if math.isfinite(retry_delay):
                    delay = max(delay, retry_delay)
            # On rejection, do not divide the actual reset time by remaining.
            try:
                reset_delay = float(headers['x-ratelimit-reset'])-self.wall_clock()
                if math.isfinite(reset_delay):
                    delay = max(delay, reset_delay)
            except (KeyError, TypeError, ValueError, OverflowError):
                pass
        elif 200 <= status < 300:
            self.failures = 0
        self.not_before = max(self.not_before, self.clock()+min(300.0, max(0.0, delay)))
