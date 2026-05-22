from __future__ import annotations

import asyncio
import logging
import math

logger = logging.getLogger(__name__)


class ReconnectPolicy:
    """
    Exponential backoff reconnect policy with jitter.
    max_attempts=0 means unlimited retries.
    """

    def __init__(
        self,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_factor: float = 2.0,
        max_attempts: int = 0,
    ):
        self._initial_delay = initial_delay
        self._max_delay = max_delay
        self._backoff_factor = backoff_factor
        self._max_attempts = max_attempts
        self._attempt = 0

    @property
    def attempt(self) -> int:
        return self._attempt

    def should_reconnect(self) -> bool:
        if self._max_attempts == 0:
            return True
        return self._attempt < self._max_attempts

    def next_delay(self) -> float:
        delay = min(
            self._initial_delay * (self._backoff_factor ** self._attempt),
            self._max_delay,
        )
        import random
        jitter = random.uniform(0, delay * 0.1)
        return delay + jitter

    async def wait(self) -> None:
        self._attempt += 1
        delay = self.next_delay()
        logger.info(
            "Reconnect attempt %d in %.1fs (max=%s)",
            self._attempt,
            delay,
            self._max_attempts if self._max_attempts > 0 else "unlimited",
        )
        await asyncio.sleep(delay)

    def reset(self) -> None:
        self._attempt = 0
        logger.debug("Reconnect policy reset")
