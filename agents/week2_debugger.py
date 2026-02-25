"""Week2 debugger: parse traceback and produce repair guidance."""

from __future__ import annotations

from typing import Any


def build_debug_advice(traceback_payload: dict[str, Any] | None) -> str:
    if not traceback_payload:
        return ""

    error_type = str(traceback_payload.get("error_type", "")).strip()
    message = str(traceback_payload.get("message", "")).strip().lower()

    if error_type == "KeyError":
        if "clsoe" in message or "close" in message:
            return "use_close_column"
        return "check_dataframe_column_names"

    if error_type == "ModuleNotFoundError":
        return "avoid_unavailable_dependency"

    if error_type == "SyntaxError":
        return "fix_python_syntax"

    if error_type in {"ValueError", "TypeError"}:
        return "validate_data_schema_before_calc"

    return "generic_sandbox_fix"
