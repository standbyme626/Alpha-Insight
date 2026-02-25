"""Observability helpers with optional Arize Phoenix integration."""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generator


@dataclass
class SpanEvent:
    name: str
    duration_ms: float
    status: str


class QuantTelemetry:
    def __init__(self) -> None:
        self._events: list[SpanEvent] = []
        self._enabled = bool(os.getenv("PHOENIX_COLLECTOR_ENDPOINT"))

    @contextmanager
    def span(self, name: str) -> Generator[None, None, None]:
        start = time.perf_counter()
        status = "ok"
        try:
            yield
        except Exception:
            status = "error"
            raise
        finally:
            duration_ms = (time.perf_counter() - start) * 1000
            self._events.append(SpanEvent(name=name, duration_ms=duration_ms, status=status))

    def add_token_usage(self, prompt_tokens: int, completion_tokens: int) -> None:
        total = prompt_tokens + completion_tokens
        self._events.append(SpanEvent(name=f"tokens:{total}", duration_ms=0.0, status="ok"))

    def flush(self) -> list[SpanEvent]:
        # In real deployment, this is where Phoenix exporter hooks in.
        events = list(self._events)
        self._events.clear()
        return events

    @property
    def enabled(self) -> bool:
        return self._enabled
