"""Connector base with unified throttling/retry/error semantics (Upgrade7 P1-D1)."""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, TypeVar

import aiohttp

T = TypeVar("T")


class ConnectorErrorCode(str, Enum):
    TIMEOUT = "TIMEOUT"
    RATE_LIMIT = "RATE_LIMIT"
    AUTH = "AUTH"
    DATA_INVALID = "DATA_INVALID"
    UPSTREAM_5XX = "UPSTREAM_5XX"
    PARSE = "PARSE"
    NETWORK = "NETWORK"
    UNKNOWN = "UNKNOWN"


class ConnectorError(RuntimeError):
    def __init__(
        self,
        *,
        code: ConnectorErrorCode,
        message: str,
        retriable: bool,
        status_code: int | None = None,
    ):
        self.code = code
        self.retriable = retriable
        self.status_code = status_code
        super().__init__(message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code.value,
            "message": str(self),
            "retriable": self.retriable,
            "status_code": self.status_code,
        }


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 2
    base_backoff_seconds: float = 0.2
    max_backoff_seconds: float = 1.0
    jitter_seconds: float = 0.0

    def backoff_seconds(self, attempt_index: int) -> float:
        attempt = max(0, int(attempt_index))
        backoff = min(self.max_backoff_seconds, self.base_backoff_seconds * (2**attempt))
        if self.jitter_seconds > 0:
            backoff += random.uniform(0.0, self.jitter_seconds)
        return max(0.0, backoff)


class Throttler:
    """Simple async rate limiter by minimum interval between requests."""

    def __init__(self, rate_per_sec: float):
        normalized = max(0.001, float(rate_per_sec))
        self._min_interval = 1.0 / normalized
        self._next_allowed = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        wait_seconds = 0.0
        async with self._lock:
            now = time.perf_counter()
            wait_seconds = max(0.0, self._next_allowed - now)
            base = now + wait_seconds
            self._next_allowed = base + self._min_interval
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)


class BaseConnector:
    def __init__(
        self,
        *,
        name: str,
        timeout_seconds: float,
        retry_policy: RetryPolicy,
        throttler: Throttler | None = None,
    ):
        self._name = name
        self._timeout_seconds = max(0.01, float(timeout_seconds))
        self._retry_policy = retry_policy
        self._throttler = throttler

    def _map_exception(self, exc: Exception) -> ConnectorError:
        if isinstance(exc, ConnectorError):
            return exc
        if isinstance(exc, asyncio.TimeoutError):
            return ConnectorError(
                code=ConnectorErrorCode.TIMEOUT,
                message=f"{self._name} request timed out",
                retriable=True,
            )
        if isinstance(exc, aiohttp.ClientResponseError):
            status = int(exc.status)
            if status == 429:
                return ConnectorError(
                    code=ConnectorErrorCode.RATE_LIMIT,
                    message=f"{self._name} rate limited ({status})",
                    retriable=True,
                    status_code=status,
                )
            if status in {401, 403}:
                return ConnectorError(
                    code=ConnectorErrorCode.AUTH,
                    message=f"{self._name} auth failed ({status})",
                    retriable=False,
                    status_code=status,
                )
            if 500 <= status <= 599:
                return ConnectorError(
                    code=ConnectorErrorCode.UPSTREAM_5XX,
                    message=f"{self._name} upstream server error ({status})",
                    retriable=True,
                    status_code=status,
                )
            return ConnectorError(
                code=ConnectorErrorCode.DATA_INVALID,
                message=f"{self._name} request rejected ({status})",
                retriable=False,
                status_code=status,
            )
        if isinstance(exc, aiohttp.ClientError):
            return ConnectorError(
                code=ConnectorErrorCode.NETWORK,
                message=f"{self._name} network error: {exc}",
                retriable=True,
            )
        if isinstance(exc, ValueError):
            return ConnectorError(
                code=ConnectorErrorCode.PARSE,
                message=f"{self._name} parse error: {exc}",
                retriable=False,
            )
        return ConnectorError(
            code=ConnectorErrorCode.UNKNOWN,
            message=f"{self._name} unexpected error: {exc}",
            retriable=False,
        )

    async def call(
        self,
        operation: str,
        fn: Callable[[], Awaitable[T]],
    ) -> T:
        attempts = max(1, int(self._retry_policy.max_attempts))
        last_error: ConnectorError | None = None
        for attempt in range(attempts):
            if self._throttler is not None:
                await self._throttler.acquire()
            try:
                return await asyncio.wait_for(fn(), timeout=self._timeout_seconds)
            except Exception as exc:  # pragma: no cover - classification path is covered by tests.
                mapped = self._map_exception(exc)
                last_error = mapped
                if (not mapped.retriable) or attempt >= attempts - 1:
                    raise mapped
                await asyncio.sleep(self._retry_policy.backoff_seconds(attempt))
        assert last_error is not None
        raise last_error
