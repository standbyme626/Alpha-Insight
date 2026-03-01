from __future__ import annotations

import time

import pytest

from core.connectors import BaseConnector, ConnectorError, ConnectorErrorCode, RetryPolicy, Throttler
from tools.news_data import RSSNewsConnector


@pytest.mark.asyncio
async def test_base_connector_retries_retriable_errors() -> None:
    connector = BaseConnector(
        name="test_connector",
        timeout_seconds=1.0,
        retry_policy=RetryPolicy(max_attempts=3, base_backoff_seconds=0.0, max_backoff_seconds=0.0, jitter_seconds=0.0),
        throttler=None,
    )
    attempts = {"count": 0}

    async def flaky_call() -> str:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ConnectorError(code=ConnectorErrorCode.RATE_LIMIT, message="429", retriable=True, status_code=429)
        return "ok"

    result = await connector.call("flaky", flaky_call)
    assert result == "ok"
    assert attempts["count"] == 3


@pytest.mark.asyncio
async def test_base_connector_stops_on_non_retriable_error() -> None:
    connector = BaseConnector(
        name="test_connector",
        timeout_seconds=1.0,
        retry_policy=RetryPolicy(max_attempts=5, base_backoff_seconds=0.0, max_backoff_seconds=0.0, jitter_seconds=0.0),
        throttler=None,
    )
    attempts = {"count": 0}

    async def bad_call() -> str:
        attempts["count"] += 1
        raise ConnectorError(code=ConnectorErrorCode.DATA_INVALID, message="400", retriable=False, status_code=400)

    with pytest.raises(ConnectorError) as exc_info:
        await connector.call("bad", bad_call)

    assert attempts["count"] == 1
    assert exc_info.value.code == ConnectorErrorCode.DATA_INVALID


@pytest.mark.asyncio
async def test_throttler_enforces_minimum_interval() -> None:
    throttler = Throttler(rate_per_sec=10)  # 100ms interval
    started = time.perf_counter()
    await throttler.acquire()
    await throttler.acquire()
    elapsed = time.perf_counter() - started
    assert elapsed >= 0.09


class _FakeResponse:
    def __init__(self, status: int, text: str = ""):
        self.status = status
        self._text = text

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None

    async def text(self) -> str:
        return self._text


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]):
        self._responses = responses
        self.calls = 0

    def get(self, url: str) -> _FakeResponse:  # noqa: ARG002
        idx = min(self.calls, len(self._responses) - 1)
        self.calls += 1
        return self._responses[idx]


@pytest.mark.asyncio
async def test_rss_connector_uses_standard_error_semantics_and_retry() -> None:
    connector = RSSNewsConnector(
        timeout_seconds=1.0,
        throttle_rate_per_sec=1000,
        retry_policy=RetryPolicy(max_attempts=2, base_backoff_seconds=0.0, max_backoff_seconds=0.0, jitter_seconds=0.0),
    )
    session = _FakeSession([_FakeResponse(503), _FakeResponse(200, "<rss></rss>")])
    text = await connector.fetch_feed(session=session, url="https://example.com/feed")

    assert text == "<rss></rss>"
    assert session.calls == 2
