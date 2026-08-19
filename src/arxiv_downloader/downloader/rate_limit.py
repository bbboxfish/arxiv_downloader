from __future__ import annotations

import asyncio
import time


class AsyncRateLimiter:
    """Enforce a process-wide minimum interval between request starts."""

    def __init__(self, minimum_interval_seconds: float) -> None:
        self.minimum_interval = minimum_interval_seconds
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0

    async def wait(self) -> int:
        started = time.monotonic()
        async with self._lock:
            loop = asyncio.get_running_loop()
            delay = self._next_allowed - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            self._next_allowed = loop.time() + self.minimum_interval
        return max(0, int((time.monotonic() - started) * 1000))
