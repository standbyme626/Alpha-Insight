"""Code guardrails for sandbox execution safety policy."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class GuardrailViolation:
    rule: str
    detail: str


class GuardrailError(RuntimeError):
    def __init__(self, violations: list[GuardrailViolation]) -> None:
        self.violations = violations
        msg = "; ".join(f"{v.rule}: {v.detail}" for v in violations)
        super().__init__(f"Sandbox guardrail violation(s): {msg}")


_FORBIDDEN_PATTERNS: list[tuple[str, str]] = [
    ("forbidden_import", r"\bimport\s+(socket|subprocess|pexpect|paramiko)\b"),
    ("forbidden_call", r"\bos\.system\s*\("),
    ("forbidden_call", r"\bsubprocess\.(run|Popen|call)\s*\("),
    ("forbidden_network", r"\brequests\.(get|post|put|delete)\s*\("),
    ("forbidden_network", r"\baiohttp\.ClientSession\s*\("),
    ("dangerous_eval", r"\b(eval|exec)\s*\("),
]


_ABSOLUTE_FILE_IO_PATTERNS: list[tuple[str, str]] = [
    ("forbidden_path", r"\bopen\s*\(\s*['\"]/(?!app/).*"),
    ("forbidden_path", r"\bPath\s*\(\s*['\"]/(?!app/).*"),
]


def validate_sandbox_code(code: str) -> None:
    violations: list[GuardrailViolation] = []
    for rule, pattern in _FORBIDDEN_PATTERNS + _ABSOLUTE_FILE_IO_PATTERNS:
        match = re.search(pattern, code)
        if match:
            violations.append(GuardrailViolation(rule=rule, detail=match.group(0)))

    if violations:
        raise GuardrailError(violations)
