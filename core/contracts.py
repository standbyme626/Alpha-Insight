"""Runtime-level contracts for workflow/sandbox interoperability."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from core.execution_result import ExecutionResult

RUNTIME_CONTRACT_SCHEMA_VERSION = "upgrade11.runtime_contract.v1"
EXECUTION_RESULT_SCHEMA_VERSION = "upgrade11.execution_result.v1"


@dataclass(frozen=True)
class SandboxRuntimeContract:
    backend: str
    timeout_seconds: int
    tool_permissions: tuple[str, ...] = ("python_exec",)
    schema_version: str = RUNTIME_CONTRACT_SCHEMA_VERSION


@dataclass
class WorkflowExecutionEnvelope:
    result: ExecutionResult
    runtime: SandboxRuntimeContract
    meta: dict[str, Any] = field(default_factory=dict)
    schema_version: str = EXECUTION_RESULT_SCHEMA_VERSION
