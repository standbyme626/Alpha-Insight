"""Async sandbox manager with E2B-first execution and structured traceback."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from typing import Any

from core.guardrails import validate_sandbox_code
from core.sandbox import LocalDockerSandbox

try:  # pragma: no cover - optional dependency
    from e2b_code_interpreter import CodeInterpreter
except Exception:  # pragma: no cover - optional dependency
    CodeInterpreter = None  # type: ignore[assignment]


@dataclass
class StructuredTraceback:
    error_type: str
    message: str
    frames: list[dict[str, Any]]
    raw: str


@dataclass
class SandboxResult:
    stdout: str
    stderr: str
    exit_code: int
    images: list[str]
    output_files: list[str]
    traceback: StructuredTraceback | None


class SandboxManager:
    """Manage isolated Python execution with E2B SDK first, Docker fallback second."""

    def __init__(self, api_key: str | None = None, use_local_fallback: bool = True) -> None:
        print("[DEBUG] QuantNode SandboxManager.__init__ Start")
        self.api_key = api_key
        self.use_local_fallback = use_local_fallback
        self._session: Any | None = None
        self._local = LocalDockerSandbox()

    async def create_session(self) -> str:
        print("[DEBUG] QuantNode SandboxManager.create_session Start")
        if CodeInterpreter is not None:
            kwargs = {"api_key": self.api_key} if self.api_key else {}
            self._session = await asyncio.to_thread(CodeInterpreter, **kwargs)
            return "e2b-session"

        if not self.use_local_fallback:
            raise RuntimeError("E2B SDK unavailable and local fallback disabled.")

        self._session = "local-docker"
        return "local-session"

    async def execute(self, code: str) -> SandboxResult:
        print("[DEBUG] QuantNode SandboxManager.execute Start")
        validate_sandbox_code(code)
        if not self._session:
            await self.create_session()

        if self._session == "local-docker":
            payload = await self._local.execute_code(code)
            tb = self._parse_traceback(payload.get("stderr", ""))
            return SandboxResult(
                stdout=payload.get("stdout", ""),
                stderr=payload.get("stderr", ""),
                exit_code=payload.get("exit_code", 1),
                images=payload.get("images", []),
                output_files=payload.get("output_files", payload.get("images", [])),
                traceback=tb,
            )

        payload = await asyncio.to_thread(self._execute_e2b_code, code)
        tb = self._parse_traceback(payload.get("stderr", ""))
        return SandboxResult(
            stdout=payload.get("stdout", ""),
            stderr=payload.get("stderr", ""),
            exit_code=payload.get("exit_code", 0),
            images=payload.get("images", []),
            output_files=payload.get("output_files", payload.get("images", [])),
            traceback=tb,
        )

    async def destroy_session(self) -> None:
        print("[DEBUG] QuantNode SandboxManager.destroy_session Start")
        if self._session and self._session != "local-docker":
            await asyncio.to_thread(self._safe_close_e2b_session)
        self._session = None

    def _execute_e2b_code(self, code: str) -> dict[str, Any]:
        session = self._session
        if session is None:
            raise RuntimeError("No E2B session available.")

        result: Any | None = None
        if hasattr(session, "run_code"):
            result = session.run_code(code)
        elif hasattr(session, "notebook") and hasattr(session.notebook, "exec_cell"):
            result = session.notebook.exec_cell(code)
        elif hasattr(session, "commands") and hasattr(session.commands, "run"):
            quoted = code.replace("'''", "\\'\\'\\'")
            result = session.commands.run(f"python - <<'PY'\\n{quoted}\\nPY")
        else:
            raise RuntimeError("Unsupported E2B SDK interface. Please pin e2b-code-interpreter.")

        stdout = self._read_attr(result, "stdout", default="")
        stderr = self._read_attr(result, "stderr", default="")
        exit_code = self._read_attr(result, "exit_code", default=0)

        if isinstance(stdout, list):
            stdout = "\n".join(str(line) for line in stdout)
        if isinstance(stderr, list):
            stderr = "\n".join(str(line) for line in stderr)

        return {
            "stdout": str(stdout),
            "stderr": str(stderr),
            "exit_code": int(exit_code),
            "images": [],
            "output_files": [],
        }

    def _safe_close_e2b_session(self) -> None:
        session = self._session
        if session is None:
            return
        if hasattr(session, "close"):
            session.close()
        elif hasattr(session, "kill"):
            session.kill()

    @staticmethod
    def _read_attr(obj: Any, name: str, default: Any) -> Any:
        return getattr(obj, name, default) if obj is not None else default

    @staticmethod
    def _parse_traceback(stderr: str) -> StructuredTraceback | None:
        if "Traceback" not in stderr:
            return None

        lines = stderr.strip().splitlines()
        frames: list[dict[str, Any]] = []
        for line in lines:
            match = re.search(r'File "(?P<file>.+)", line (?P<line>\d+), in (?P<func>.+)$', line.strip())
            if match:
                frames.append(
                    {
                        "file": match.group("file"),
                        "line": int(match.group("line")),
                        "function": match.group("func"),
                    }
                )

        last = lines[-1] if lines else ""
        if ":" in last:
            error_type, message = last.split(":", 1)
            error_type = error_type.strip()
            message = message.strip()
        else:
            error_type = "ExecutionError"
            message = last.strip()

        return StructuredTraceback(
            error_type=error_type,
            message=message,
            frames=frames,
            raw=stderr,
        )
