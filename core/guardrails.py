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
    ("forbidden_import", r"\bimport\s+(socket|subprocess|pexpect|paramiko|requests|httpx|urllib|aiohttp|yfinance)\b"),
    ("forbidden_import", r"\bfrom\s+(socket|subprocess|pexpect|paramiko|requests|httpx|urllib|aiohttp|yfinance)\s+import\b"),
    ("forbidden_call", r"\bos\.system\s*\("),
    ("forbidden_call", r"\bsubprocess\.(run|Popen|call)\s*\("),
    ("forbidden_network", r"\brequests\.(get|post|put|delete)\s*\("),
    ("forbidden_network", r"\bhttpx\.(get|post|put|delete)\s*\("),
    ("forbidden_network", r"\burllib\.request\.(urlopen|Request)\s*\("),
    ("forbidden_network", r"\bsocket\.(socket|create_connection)\s*\("),
    ("forbidden_network", r"\baiohttp\.ClientSession\s*\("),
    ("forbidden_network", r"\byf\.Ticker\s*\("),
    ("forbidden_install", r"(^|\n)\s*!\s*(pip|uv|poetry|conda)\s+install\b"),
    ("forbidden_install", r"\bpython\s+-m\s+pip\s+install\b"),
    ("forbidden_install", r"\bpip\s+install\b"),
    ("forbidden_install", r"\b(apt|apt-get|apk|yum)\s+install\b"),
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
