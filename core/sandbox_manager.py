"""Async sandbox manager with unified policy and execution result contract."""

from __future__ import annotations

import asyncio
import re
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from core.execution_result import ExecutionResult, StructuredTraceback
from core.fault_injection import FaultInjectionEvent, FaultInjector, fault_semantic
from core.sandbox import LocalDockerSandbox
from core.sandbox_policy import SandboxPolicy

try:  # pragma: no cover - optional dependency
    from e2b_code_interpreter import CodeInterpreter
except Exception:  # pragma: no cover - optional dependency
    CodeInterpreter = None  # type: ignore[assignment]


# Backward-compatible alias.
SandboxResult = ExecutionResult


class SandboxManager:
    """Manage isolated Python execution with E2B SDK first, Docker fallback second."""

    def __init__(
        self,
        api_key: str | None = None,
        use_local_fallback: bool = True,
        *,
        policy: SandboxPolicy | None = None,
        local_runtime: LocalDockerSandbox | None = None,
        e2b_timeout_seconds: int = 30,
        fault_injector: FaultInjector | None = None,
    ) -> None:
        print("[DEBUG] QuantNode SandboxManager.__init__ Start")
        self.api_key = api_key
        self.use_local_fallback = use_local_fallback
        self._session: Any | None = None
        self._local = local_runtime if local_runtime is not None else LocalDockerSandbox()
        self._policy = policy if policy is not None else SandboxPolicy()
        self._e2b_timeout_seconds = e2b_timeout_seconds
        self._fault_injector = fault_injector if fault_injector is not None else FaultInjector.disabled()

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

    async def execute(self, code: str) -> ExecutionResult:
        print("[DEBUG] QuantNode SandboxManager.execute Start")
        if not self._session:
            await self.create_session()

        if self._session == "local-docker":
            self._policy.enforce(
                code,
                backend="local-docker",
                timeout_seconds=self._local.timeout_seconds,
                tool_permissions=("python_exec",),
            )
            injected = self._fault_injector.maybe_inject(
                node="runtime.sandbox_execute",
                allowed_faults=("timeout", "sandbox_failure"),
            )
            if injected is not None:
                payload = self._build_fault_injected_payload(injected, backend="local-docker")
                return self._to_execution_result(payload, default_backend="local-docker", default_exit_code=1)
            try:
                payload = await self._local.execute_code(code)
            except Exception as exc:
                if not self._should_fallback_to_local_process(exc):
                    raise
                self._policy.enforce(
                    code,
                    backend="local-process-fallback",
                    timeout_seconds=self._local.timeout_seconds,
                    tool_permissions=("python_exec",),
                )
                payload = await self._execute_local_process(
                    code,
                    reason=str(exc),
                    timeout_seconds=self._local.timeout_seconds,
                )
            return self._to_execution_result(payload, default_backend="local-docker", default_exit_code=1)

        self._policy.enforce(
            code,
            backend="e2b",
            timeout_seconds=self._e2b_timeout_seconds,
            tool_permissions=("python_exec",),
        )
        injected = self._fault_injector.maybe_inject(
            node="runtime.sandbox_execute",
            allowed_faults=("timeout", "sandbox_failure"),
        )
        if injected is not None:
            payload = self._build_fault_injected_payload(injected, backend="e2b")
            return self._to_execution_result(payload, default_backend="e2b", default_exit_code=1)
        payload = await asyncio.to_thread(self._execute_e2b_code, code)
        return self._to_execution_result(payload, default_backend="e2b", default_exit_code=0)

    async def destroy_session(self) -> None:
        print("[DEBUG] QuantNode SandboxManager.destroy_session Start")
        if self._session and self._session != "local-docker":
            await asyncio.to_thread(self._safe_close_e2b_session)
        self._session = None

    def _execute_e2b_code(self, code: str) -> dict[str, Any]:
        started_at = time.perf_counter()
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
            "backend": "e2b",
            "execution_backend": "e2b",
            "duration_ms": (time.perf_counter() - started_at) * 1000,
            "resource_usage": None,
        }

    @staticmethod
    def _to_execution_result(
        payload: dict[str, Any],
        *,
        default_backend: str,
        default_exit_code: int,
    ) -> ExecutionResult:
        stderr = str(payload.get("stderr", ""))
        tb = SandboxManager._parse_traceback(stderr)
        backend = str(payload.get("backend") or payload.get("execution_backend") or default_backend)
        images = [str(item) for item in payload.get("images", []) if str(item).strip()]
        output_files = [str(item) for item in payload.get("output_files", images) if str(item).strip()]
        duration_ms = float(payload.get("duration_ms", 0.0))
        raw_resource_usage = payload.get("resource_usage")
        resource_usage = raw_resource_usage if isinstance(raw_resource_usage, dict) else None
        return ExecutionResult(
            stdout=str(payload.get("stdout", "")),
            stderr=stderr,
            exit_code=int(payload.get("exit_code", default_exit_code)),
            traceback=tb,
            backend=backend,
            duration_ms=duration_ms,
            resource_usage=resource_usage,
            images=images,
            output_files=output_files,
        )

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

    @staticmethod
    def _build_fault_injected_payload(event: FaultInjectionEvent, *, backend: str) -> dict[str, Any]:
        semantic = fault_semantic(event.fault, node="runtime.sandbox_execute")
        exit_code = 124 if event.fault == "timeout" else 1
        stderr = (
            "Traceback (most recent call last):\n"
            '  File "runtime_fault_injection.py", line 1, in <module>\n'
            f"{semantic.error_type}: {semantic.message}"
        )
        return {
            "stdout": "",
            "stderr": stderr,
            "exit_code": exit_code,
            "images": [],
            "output_files": [],
            "backend": f"{backend}:fault-injected",
            "execution_backend": f"{backend}:fault-injected",
            "duration_ms": 0.0,
            "resource_usage": {"fault_injection": event.to_dict()},
        }

    @staticmethod
    def _should_fallback_to_local_process(exc: Exception) -> bool:
        text = str(exc).lower()
        missing_docker_binary = (
            ("no such file or directory" in text or "errno 2" in text)
            and "docker" in text
        )
        if missing_docker_binary:
            return True
        markers = (
            "docker.sock",
            "permission denied",
            "cannot connect to the docker daemon",
            "docker image",
            "not found",
            "failed to create sandbox container",
        )
        return any(marker in text for marker in markers)

    async def _execute_local_process(
        self,
        code: str,
        *,
        reason: str,
        timeout_seconds: int,
    ) -> dict[str, Any]:
        started_at = time.perf_counter()
        with tempfile.TemporaryDirectory(prefix="quant-local-fallback-") as temp_dir:
            script_path = Path(temp_dir) / "user_code.py"
            script_path.write_text(code, encoding="utf-8")
            proc = await asyncio.create_subprocess_exec(
                sys.executable,
                str(script_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout_seconds)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.communicate()
                return {
                    "stdout": "",
                    "stderr": f"Local fallback timed out after {timeout_seconds}s.",
                    "exit_code": 124,
                    "images": [],
                    "output_files": [],
                    "backend": "local-process-fallback",
                    "execution_backend": "local-process-fallback",
                    "duration_ms": (time.perf_counter() - started_at) * 1000,
                    "resource_usage": None,
                }

            fallback_note = (
                "[WARN] Docker sandbox unavailable; used local-process fallback.\n"
                f"reason={reason}\n"
            )
            stderr = fallback_note + stderr_b.decode("utf-8", errors="replace")
            return {
                "stdout": stdout_b.decode("utf-8", errors="replace"),
                "stderr": stderr,
                "exit_code": proc.returncode or 0,
                "images": [],
                "output_files": [],
                "backend": "local-process-fallback",
                "execution_backend": "local-process-fallback",
                "duration_ms": (time.perf_counter() - started_at) * 1000,
                "resource_usage": None,
            }
