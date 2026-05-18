import asyncio
import time
from typing import Callable, Optional, TypeVar

from utils.logger import setup_logger

logger = setup_logger("RetryHandler")

T = TypeVar("T")


class RetryHandler:
    def __init__(self, max_retries: int = 3, max_total_seconds: Optional[float] = None):
        # max_retries = number of retries AFTER the initial attempt;
        # total attempts = max_retries + 1.
        self.max_retries = max_retries
        self.max_total_seconds = max_total_seconds
        self.retry_delays = [1, 2, 5]

    async def execute_with_retry(self, func: Callable[..., T], *args, **kwargs) -> T:
        last_error = None
        total_attempts = self.max_retries + 1
        started_at = time.monotonic()

        for attempt in range(total_attempts):
            elapsed = time.monotonic() - started_at
            if self.max_total_seconds is not None and elapsed >= self.max_total_seconds:
                raise TimeoutError(
                    f"retry total time {elapsed:.0f}s exceeded limit {self.max_total_seconds:.0f}s"
                )
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt < self.max_retries:
                    delay = self.retry_delays[min(attempt, len(self.retry_delays) - 1)]
                    remaining = (
                        self.max_total_seconds - (time.monotonic() - started_at) - delay
                        if self.max_total_seconds is not None
                        else None
                    )
                    if remaining is not None and remaining <= 0:
                        raise TimeoutError(
                            f"retry would exceed total limit of {self.max_total_seconds:.0f}s, "
                            f"last error: {last_error}"
                        )
                    logger.warning(
                        "Attempt %d failed: %s, retrying in %ds...", attempt + 1, e, delay
                    )
                    await asyncio.sleep(delay)

        logger.error("All %d attempts failed: %s", total_attempts, last_error)
        raise last_error
