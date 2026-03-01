"""Unified sandbox policy enforcement shared by all runtimes."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import ClassVar, Sequence


@dataclass(frozen=True)
class GuardrailViolation:
    rule: str
    detail: str


class GuardrailError(RuntimeError):
    def __init__(self, violations: list[GuardrailViolation]) -> None:
        self.violations = violations
        msg = "; ".join(f"{v.rule}: {v.detail}" for v in violations)
        super().__init__(f"Sandbox guardrail violation(s): {msg}")


@dataclass(frozen=True)
class SandboxPolicy:
    """Policy layer independent from runtime implementation."""

    allow_network: bool = False
    allowed_tool_permissions: tuple[str, ...] = ("python_exec",)

    _FORBIDDEN_IMPORT_PATTERNS: ClassVar[tuple[tuple[str, str], ...]] = (
        (
            "forbidden_import",
            r"\bimport\s+(socket|subprocess|pexpect|paramiko|requests|httpx|urllib|aiohttp|yfinance)\b",
        ),
        (
            "forbidden_import",
            r"\bfrom\s+(socket|subprocess|pexpect|paramiko|requests|httpx|urllib|aiohttp|yfinance)\s+import\b",
        ),
    )
    _FORBIDDEN_CALL_PATTERNS: ClassVar[tuple[tuple[str, str], ...]] = (
        ("forbidden_call", r"\bos\.system\s*\("),
        ("forbidden_call", r"\bsubprocess\.(run|Popen|call)\s*\("),
        ("dangerous_eval", r"\b(eval|exec)\s*\("),
    )
    _FORBIDDEN_NETWORK_PATTERNS: ClassVar[tuple[tuple[str, str], ...]] = (
        ("forbidden_network", r"\brequests\.(get|post|put|delete)\s*\("),
        ("forbidden_network", r"\bhttpx\.(get|post|put|delete)\s*\("),
        ("forbidden_network", r"\burllib\.request\.(urlopen|Request)\s*\("),
        ("forbidden_network", r"\bsocket\.(socket|create_connection)\s*\("),
        ("forbidden_network", r"\baiohttp\.ClientSession\s*\("),
        ("forbidden_network", r"\byf\.Ticker\s*\("),
    )
    _FORBIDDEN_INSTALL_PATTERNS: ClassVar[tuple[tuple[str, str], ...]] = (
        ("forbidden_install", r"(^|\n)\s*!\s*(pip|uv|poetry|conda)\s+install\b"),
        ("forbidden_install", r"\bpython\s+-m\s+pip\s+install\b"),
        ("forbidden_install", r"\bpip\s+install\b"),
        ("forbidden_install", r"\b(apt|apt-get|apk|yum)\s+install\b"),
    )
    _ABSOLUTE_FILE_IO_PATTERNS: ClassVar[tuple[tuple[str, str], ...]] = (
        ("forbidden_path", r"\bopen\s*\(\s*['\"]/(?!app/).*"),
        ("forbidden_path", r"\bPath\s*\(\s*['\"]/(?!app/).*"),
    )

    def enforce(
        self,
        code: str,
        *,
        backend: str,
        timeout_seconds: int,
        tool_permissions: Sequence[str] | None = None,
    ) -> None:
        """Validate code and runtime constraints before execution."""
        violations: list[GuardrailViolation] = []

        if not backend.strip():
            violations.append(GuardrailViolation(rule="invalid_backend", detail="backend is empty"))

        if timeout_seconds <= 0:
            violations.append(
                GuardrailViolation(
                    rule="invalid_timeout",
                    detail=f"timeout_seconds must be > 0, got {timeout_seconds}",
                )
            )

        requested_permissions = tuple(tool_permissions or ())
        disallowed = sorted(set(requested_permissions) - set(self.allowed_tool_permissions))
        if disallowed:
            violations.append(
                GuardrailViolation(
                    rule="forbidden_tool_permission",
                    detail="requested permissions not allowed: " + ",".join(disallowed),
                )
            )

        patterns = [
            *self._FORBIDDEN_IMPORT_PATTERNS,
            *self._FORBIDDEN_CALL_PATTERNS,
            *self._FORBIDDEN_INSTALL_PATTERNS,
            *self._ABSOLUTE_FILE_IO_PATTERNS,
        ]
        if not self.allow_network:
            patterns.extend(self._FORBIDDEN_NETWORK_PATTERNS)

        for rule, pattern in patterns:
            match = re.search(pattern, code)
            if match:
                violations.append(GuardrailViolation(rule=rule, detail=match.group(0)))

        if violations:
            raise GuardrailError(violations)
