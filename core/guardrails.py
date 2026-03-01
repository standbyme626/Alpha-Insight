"""Backward-compatible guardrail API that delegates to sandbox policy."""

from __future__ import annotations

from core.sandbox_policy import GuardrailError, GuardrailViolation, SandboxPolicy

_DEFAULT_POLICY = SandboxPolicy()


def validate_sandbox_code(code: str) -> None:
    _DEFAULT_POLICY.enforce(
        code,
        backend="validation-only",
        timeout_seconds=30,
        tool_permissions=("python_exec",),
    )
