"""Concurrency control for K8s-scaled ETL pods.

- RunSemaphore: limits total concurrent Singer chains per pod.
- SourceRateLimiter: limits concurrent taps per source host:port.
"""

from __future__ import annotations

import asyncio
import os
from collections import defaultdict
from typing import Any

MAX_CONCURRENT_RUNS = int(os.environ.get("MAX_CONCURRENT_RUNS", "20"))
MAX_TAPS_PER_SOURCE = int(os.environ.get("MAX_TAPS_PER_SOURCE", "3"))


class RunSemaphore:
    """Global semaphore bounding the number of active Singer subprocess chains."""

    def __init__(self, max_runs: int = MAX_CONCURRENT_RUNS) -> None:
        self._max = max_runs
        self._sem = asyncio.Semaphore(max_runs)
        self._active = 0
        self._lock = asyncio.Lock()

    @property
    def active(self) -> int:
        return self._active

    @property
    def max_runs(self) -> int:
        return self._max

    @property
    def available(self) -> int:
        return self._max - self._active

    async def acquire(self) -> bool:
        """Try to acquire a run slot. Returns False if full (non-blocking check)."""
        if self._sem.locked():
            return False
        await self._sem.acquire()
        async with self._lock:
            self._active += 1
        return True

    async def release(self) -> None:
        async with self._lock:
            self._active = max(0, self._active - 1)
        self._sem.release()


class SourceRateLimiter:
    """Per-source-host concurrency limiter.

    Prevents overwhelming a single user's Postgres instance with too many
    concurrent tap-postgres processes.
    """

    def __init__(self, max_per_source: int = MAX_TAPS_PER_SOURCE) -> None:
        self._max = max_per_source
        self._counts: dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()

    def _key(self, host: str, port: int) -> str:
        return f"{host}:{port}"

    async def acquire(self, host: str, port: int) -> bool:
        """Try to acquire a slot for the given source. Returns False if at limit."""
        key = self._key(host, port)
        async with self._lock:
            if self._counts[key] >= self._max:
                return False
            self._counts[key] += 1
            return True

    async def release(self, host: str, port: int) -> None:
        key = self._key(host, port)
        async with self._lock:
            self._counts[key] = max(0, self._counts[key] - 1)
            if self._counts[key] == 0:
                del self._counts[key]

    def snapshot(self) -> dict[str, int]:
        return dict(self._counts)


# Module-level singletons, created once per uvicorn worker
run_semaphore = RunSemaphore()
source_limiter = SourceRateLimiter()


def get_capacity() -> dict[str, Any]:
    return {
        "active_runs": run_semaphore.active,
        "max_runs": run_semaphore.max_runs,
        "available": run_semaphore.available,
        "active_sources": source_limiter.snapshot(),
    }
