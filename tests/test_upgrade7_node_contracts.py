from __future__ import annotations

from dataclasses import dataclass

import pytest
from pydantic import ValidationError

from agents.workflow_engine import _after_executor, executor_node
from core.node_contracts import (
    NodeErrorCode,
    RetryDecision,
    classify_node_error_code,
    resolve_retry_decision,
    validate_coder_contract,
    validate_executor_contract,
    validate_planner_contract,
)


def test_planner_contract_requires_request_field() -> None:
    with pytest.raises(ValidationError):
        validate_planner_contract(
            {},
            {
                "plan_steps": ["Data Fetch"],
                "data_source": "api",
                "planner_reason": "ok",
                "planner_provider": "fallback",
                "interval": "1d",
                "need_chart": False,
                "retry_count": 0,
                "max_retries": 2,
            },
        )


def test_coder_contract_required_and_optional_paths() -> None:
    with pytest.raises(Exception):
        validate_coder_contract({"traceback": None, "request": "analyze"}, {})
    allowed = validate_coder_contract({"traceback": {"error_type": "KeyError"}}, {})
    assert allowed == {}


def test_executor_contract_required_fields_and_optional_resource_usage() -> None:
    payload = validate_executor_contract(
        {"sandbox_code": "print('ok')", "retry_count": 0, "max_retries": 2},
        {
            "sandbox_stdout": "",
            "sandbox_stderr": "Traceback ...",
            "sandbox_backend": "docker:test",
            "sandbox_duration_ms": 12.5,
            "sandbox_resource_usage": {"memory_limit": "512m"},
            "sandbox_images": [],
            "sandbox_output_files": [],
            "traceback": {
                "error_type": "KeyError",
                "message": "missing Close",
                "frames": [],
                "raw": "KeyError: missing Close",
                "error_code": "logic",
            },
            "retry_count": 1,
            "success": False,
            "executor_latency_ms": 12.5,
            "fallback_used": False,
            "failure_events": [],
        },
    )
    assert payload["traceback"]["error_code"] == "logic"
    assert payload["sandbox_resource_usage"]["memory_limit"] == "512m"


def test_error_code_classification_matrix() -> None:
    assert classify_node_error_code(error_type="DataFetchError", message="data not found") == NodeErrorCode.DATA
    assert classify_node_error_code(error_type="TimeoutError", message="timed out after 30s") == NodeErrorCode.TIMEOUT
    assert classify_node_error_code(error_type="RuntimeError", message="rate limit exceeded") == NodeErrorCode.RATE_LIMIT
    assert classify_node_error_code(error_type="ConnectionError", message="network broken") == NodeErrorCode.NETWORK
    assert classify_node_error_code(error_type="KeyError", message="missing field") == NodeErrorCode.LOGIC


def test_retry_semantics_contract() -> None:
    assert resolve_retry_decision(traceback=None, retry_count=0, max_retries=2) == RetryDecision.SUCCESS
    assert resolve_retry_decision(traceback={"error_type": "X"}, retry_count=1, max_retries=2) == RetryDecision.RETRY
    assert resolve_retry_decision(traceback={"error_type": "X"}, retry_count=2, max_retries=2) == RetryDecision.STOP
    assert _after_executor({"traceback": None, "retry_count": 0, "max_retries": 2}) == "done"
    assert _after_executor({"traceback": {"error_type": "X"}, "retry_count": 1, "max_retries": 2}) == "debugger"
    assert _after_executor({"traceback": {"error_type": "X"}, "retry_count": 2, "max_retries": 2}) == "done"


@dataclass
class _FakeTraceback:
    error_type: str
    message: str
    frames: list[dict]
    raw: str


@dataclass
class _FakeExecutionResult:
    stdout: str
    stderr: str
    exit_code: int
    traceback: _FakeTraceback | None
    backend: str
    duration_ms: float
    resource_usage: dict | None
    images: list[str]
    output_files: list[str]


@pytest.mark.asyncio
async def test_executor_node_appends_error_code(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_create_session(self) -> str:  # noqa: ANN001
        return "fake"

    async def fake_destroy_session(self) -> None:  # noqa: ANN001
        return None

    async def fake_execute(self, code: str) -> _FakeExecutionResult:  # noqa: ANN001
        return _FakeExecutionResult(
            stdout="",
            stderr="KeyError: missing Close",
            exit_code=1,
            traceback=_FakeTraceback(
                error_type="KeyError",
                message="missing Close",
                frames=[{"file": "x.py", "line": 1, "function": "<module>"}],
                raw="KeyError: missing Close",
            ),
            backend="docker:test",
            duration_ms=9.1,
            resource_usage={"memory_limit": "512m"},
            images=[],
            output_files=[],
        )

    monkeypatch.setattr("core.sandbox_manager.SandboxManager.create_session", fake_create_session)
    monkeypatch.setattr("core.sandbox_manager.SandboxManager.destroy_session", fake_destroy_session)
    monkeypatch.setattr("core.sandbox_manager.SandboxManager.execute", fake_execute)

    output = await executor_node({"sandbox_code": "print('x')", "retry_count": 0, "max_retries": 2})
    assert output["retry_count"] == 1
    assert output["success"] is False
    assert output["traceback"]["error_code"] == "logic"
