"""Shared execution result contract across sandbox runtimes and workflow orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class StructuredTraceback:
    error_type: str
    message: str
    frames: list[dict[str, Any]]
    raw: str


@dataclass
class ExecutionResult:
    stdout: str
    stderr: str
    exit_code: int
    traceback: StructuredTraceback | None
    backend: str
    duration_ms: float
    resource_usage: dict[str, Any] | None = None
    images: list[str] = field(default_factory=list)
    output_files: list[str] = field(default_factory=list)

    @property
    def execution_backend(self) -> str:
        # Backward-compatible alias for legacy call sites.
        return self.backend


# Backward-compatible alias used by older modules.
SandboxResult = ExecutionResult
