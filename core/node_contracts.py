"""Node I/O contracts for Week2 planner-coder-executor-debugger pipeline."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

NODE_CONTRACT_SCHEMA_VERSION = "upgrade10.node_contract.v1"


class NodeErrorCode(str, Enum):
    DATA = "data"
    SANDBOX = "sandbox"
    NETWORK = "network"
    LOGIC = "logic"
    TIMEOUT = "timeout"
    RATE_LIMIT = "rate_limit"
    UNKNOWN = "unknown"


class RetryDecision(str, Enum):
    SUCCESS = "success"
    RETRY = "retry"
    STOP = "stop"


class TracebackContract(BaseModel):
    model_config = ConfigDict(extra="ignore")

    error_type: str
    message: str
    frames: list[dict[str, Any]] = Field(default_factory=list)
    raw: str
    error_code: NodeErrorCode = NodeErrorCode.UNKNOWN


class PlannerNodeInput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    request: str
    interval: str = "1d"
    need_chart: bool = False
    retry_count: int = 0
    max_retries: int = 2

    @field_validator("retry_count", "max_retries")
    @classmethod
    def _non_negative(cls, value: int) -> int:
        if value < 0:
            raise ValueError("retry_count/max_retries must be non-negative")
        return value


class PlannerNodeOutput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    plan_steps: list[str]
    data_source: str
    planner_reason: str
    planner_provider: str
    interval: str
    need_chart: bool
    retry_count: int
    max_retries: int


class CoderNodeInput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    traceback: dict[str, Any] | None = None
    request: str = ""
    symbol: str = ""
    period: str = ""


class CoderNodeOutput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    sandbox_code: str | None = None


class ExecutorNodeInput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    sandbox_code: str
    retry_count: int = 0
    max_retries: int = 2


class ExecutorNodeOutput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    sandbox_stdout: str
    sandbox_stderr: str
    sandbox_backend: str
    sandbox_duration_ms: float = 0.0
    sandbox_resource_usage: dict[str, Any] | None = None
    sandbox_images: list[str] = Field(default_factory=list)
    sandbox_output_files: list[str] = Field(default_factory=list)
    traceback: TracebackContract | None = None
    retry_count: int
    success: bool
    executor_latency_ms: float = 0.0
    fallback_used: bool = False
    failure_events: list[dict[str, Any]] = Field(default_factory=list)


class DebuggerNodeInput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    traceback: dict[str, Any] | None = None


class DebuggerNodeOutput(BaseModel):
    model_config = ConfigDict(extra="ignore")

    debug_advice: str


def classify_node_error_code(
    *,
    error_type: str,
    message: str,
    backend: str = "",
) -> NodeErrorCode:
    blob = " ".join([error_type, message, backend]).lower()
    if "timeout" in blob or "timed out" in blob or "deadline exceeded" in blob:
        return NodeErrorCode.TIMEOUT
    if "rate limit" in blob or "too many requests" in blob:
        return NodeErrorCode.RATE_LIMIT
    if "network" in blob or "connection" in blob or "http" in blob or "dns" in blob or "ssl" in blob:
        return NodeErrorCode.NETWORK
    if (
        "datafetcherror" in blob
        or "no data" in blob
        or "not found" in blob
        or "invalid close" in blob
        or "insufficient" in blob
    ):
        return NodeErrorCode.DATA
    if "keyerror" in blob or "valueerror" in blob or "typeerror" in blob or "indexerror" in blob:
        return NodeErrorCode.LOGIC
    if "guardrail" in blob or "sandbox" in blob or "policy" in blob or "traceback" in blob:
        return NodeErrorCode.SANDBOX
    return NodeErrorCode.UNKNOWN


def resolve_retry_decision(*, traceback: dict[str, Any] | None, retry_count: int, max_retries: int) -> RetryDecision:
    if traceback is None:
        return RetryDecision.SUCCESS
    if retry_count < max_retries:
        return RetryDecision.RETRY
    return RetryDecision.STOP


def validate_planner_contract(state: dict[str, Any], output: dict[str, Any]) -> dict[str, Any]:
    PlannerNodeInput.model_validate(state)
    return PlannerNodeOutput.model_validate(output).model_dump(mode="python")


def validate_coder_contract(state: dict[str, Any], output: dict[str, Any]) -> dict[str, Any]:
    parsed_input = CoderNodeInput.model_validate(state)
    parsed_output = CoderNodeOutput.model_validate(output)
    if parsed_input.traceback is None and not (parsed_output.sandbox_code or "").strip():
        raise ValueError("sandbox_code is required when traceback is absent")
    payload = parsed_output.model_dump(mode="python", exclude_none=True)
    return payload


def validate_executor_contract(state: dict[str, Any], output: dict[str, Any]) -> dict[str, Any]:
    ExecutorNodeInput.model_validate(state)
    parsed = ExecutorNodeOutput.model_validate(output)
    return parsed.model_dump(mode="json")


def validate_debugger_contract(state: dict[str, Any], output: dict[str, Any]) -> dict[str, Any]:
    DebuggerNodeInput.model_validate(state)
    return DebuggerNodeOutput.model_validate(output).model_dump(mode="python")
