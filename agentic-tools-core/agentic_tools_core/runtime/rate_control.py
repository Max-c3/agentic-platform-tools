from __future__ import annotations

import threading
from typing import Optional
import time
from collections import defaultdict


class TokenBucket:
    def __init__(self, rate_per_minute: int, capacity: Optional[int] = None) -> None:
        self.rate_per_second = rate_per_minute / 60.0
        self.capacity = float(capacity if capacity is not None else max(rate_per_minute, 1))
        self.tokens = self.capacity
        self.updated_at = time.monotonic()
        self.lock = threading.Lock()

    def take(self, tokens: float = 1.0) -> bool:
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.updated_at
            self.updated_at = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_second)
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False


class RateController:
    def __init__(self) -> None:
        self._buckets: dict[str, TokenBucket] = {}
        self._semaphores: dict[str, threading.Semaphore] = {}
        self._global_lock = threading.Lock()

    def configure_tool(self, tool_id: str, rate_per_minute: int, concurrency: int) -> None:
        with self._global_lock:
            self._buckets[tool_id] = TokenBucket(rate_per_minute=rate_per_minute)
            self._semaphores[tool_id] = threading.Semaphore(max(1, concurrency))

    def allow(self, tool_id: str) -> bool:
        bucket = self._buckets[tool_id]
        return bucket.take(1.0)

    def semaphore(self, tool_id: str) -> threading.Semaphore:
        return self._semaphores[tool_id]
