"""Local Docker sandbox for secure Python code execution."""

from __future__ import annotations

import asyncio
import tempfile
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


class SandboxError(Exception):
    """Base sandbox exception (L1)."""

    level = 1

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class SandboxInfrastructureError(SandboxError):
    """Docker/runtime infrastructure error (L2)."""

    level = 2


class SandboxResourceError(SandboxError):
    """Resource guardrail error (L3)."""

    level = 3


class SandboxTimeoutError(SandboxResourceError):
    """Execution timed out."""


class SandboxMemoryOverflowError(SandboxResourceError):
    """Container process was OOM-killed."""


@dataclass
class SandboxExecutionResult:
    stdout: str
    stderr: str
    exit_code: int
    images: list[str]
    output_files: list[str]
    execution_backend: str


class LocalDockerSandbox:
    """Execute user Python code inside a local Docker container."""

    def __init__(
        self,
        image: str = "quant-sandbox:latest",
        timeout_seconds: int = 30,
        memory_limit: str = "512m",
        outputs_dir: str = "storage/outputs",
    ) -> None:
        print("[DEBUG] QuantNode LocalDockerSandbox.__init__ Start")
        self.image = image
        self.timeout_seconds = timeout_seconds
        self.memory_limit = memory_limit
        self.outputs_dir = Path(outputs_dir)
        self.outputs_dir.mkdir(parents=True, exist_ok=True)

    async def execute_code(self, code: str) -> dict[str, Any]:
        """Run Python code in Docker and return stdout/stderr + extracted output files."""
        print("[DEBUG] QuantNode LocalDockerSandbox.execute_code Start")
        if not code.strip():
            raise SandboxInfrastructureError("Received empty Python code.")

        await self._ensure_image_exists()

        run_id = uuid.uuid4().hex[:8]
        container_name = f"quant-sandbox-{run_id}"
        script_name = f"user_code_{run_id}.py"

        with tempfile.TemporaryDirectory(prefix="quant-sandbox-") as temp_dir:
            script_path = Path(temp_dir) / script_name
            script_path.write_text(code, encoding="utf-8")

            try:
                await self._run_cmd(
                    [
                        "docker",
                        "create",
                        "--name",
                        container_name,
                        "--network",
                        "none",
                        "--memory",
                        self.memory_limit,
                        "--workdir",
                        "/app",
                        self.image,
                        "sh",
                        "-lc",
                        f"python /app/{script_name} > /tmp/stdout.txt 2> /tmp/stderr.txt",
                    ],
                    err_prefix="Failed to create sandbox container",
                )
                await self._run_cmd(
                    ["docker", "cp", str(script_path), f"{container_name}:/app/{script_name}"],
                    err_prefix="Failed to copy code into container",
                )

                try:
                    await asyncio.wait_for(
                        self._run_cmd(
                            ["docker", "start", "-a", container_name],
                            err_prefix="Failed to start container",
                            allow_nonzero=True,
                        ),
                        timeout=self.timeout_seconds,
                    )
                except asyncio.TimeoutError as exc:
                    await self._cleanup_container(container_name, force=True)
                    raise SandboxTimeoutError(
                        f"Sandbox execution exceeded {self.timeout_seconds} seconds."
                    ) from exc

                oom_killed, exit_code = await self._inspect_container_state(container_name)
                if oom_killed:
                    stderr = await self._read_file_from_container(container_name, "/tmp/stderr.txt")
                    await self._cleanup_container(container_name)
                    raise SandboxMemoryOverflowError(
                        f"Sandbox memory limit exceeded ({self.memory_limit}). stderr: {stderr.strip()}"
                    )

                stdout = await self._read_file_from_container(container_name, "/tmp/stdout.txt")
                stderr = await self._read_file_from_container(container_name, "/tmp/stderr.txt")
                output_files = await self._extract_output_files(container_name, run_id)
                images = [p for p in output_files if p.lower().endswith(".png")]

                await self._cleanup_container(container_name)
                return asdict(
                    SandboxExecutionResult(
                        stdout=stdout,
                        stderr=stderr,
                        exit_code=exit_code,
                        images=images,
                        output_files=output_files,
                        execution_backend=f"docker:{self.image}",
                    )
                )
            except SandboxError:
                raise
            except Exception as exc:  # pragma: no cover - defensive path
                await self._cleanup_container(container_name, force=True)
                raise SandboxInfrastructureError(str(exc)) from exc

    async def _ensure_image_exists(self) -> None:
        print("[DEBUG] QuantNode LocalDockerSandbox._ensure_image_exists Start")
        await self._run_cmd(
            ["docker", "image", "inspect", self.image],
            err_prefix=f"Docker image '{self.image}' not found",
        )

    async def _inspect_container_state(self, container_name: str) -> tuple[bool, int]:
        print("[DEBUG] QuantNode LocalDockerSandbox._inspect_container_state Start")
        result = await self._run_cmd(
            [
                "docker",
                "inspect",
                "-f",
                "{{.State.OOMKilled}} {{.State.ExitCode}}",
                container_name,
            ],
            err_prefix="Failed to inspect container state",
        )
        parts = result.stdout.strip().split()
        if len(parts) != 2:
            raise SandboxInfrastructureError(f"Unexpected inspect output: {result.stdout!r}")
        oom_killed = parts[0].lower() == "true"
        exit_code = int(parts[1])
        return oom_killed, exit_code

    async def _read_file_from_container(self, container_name: str, path: str) -> str:
        print("[DEBUG] QuantNode LocalDockerSandbox._read_file_from_container Start")
        result = await self._run_cmd(
            ["docker", "cp", f"{container_name}:{path}", "-"],
            err_prefix=f"Failed to read file {path} from container",
            allow_nonzero=True,
        )
        if result.returncode != 0:
            return ""
        return self._extract_tar_single_file(result.stdout_bytes)

    async def _extract_output_files(self, container_name: str, run_id: str) -> list[str]:
        print("[DEBUG] QuantNode LocalDockerSandbox._extract_output_files Start")
        with tempfile.TemporaryDirectory(prefix="quant-sandbox-out-") as temp_dir:
            copied_root = Path(temp_dir) / "app"
            copied_root.mkdir(parents=True, exist_ok=True)
            copy_result = await self._run_cmd(
                ["docker", "cp", f"{container_name}:/app/.", str(copied_root)],
                err_prefix="Failed to copy /app directory for image extraction",
                allow_nonzero=True,
            )
            if copy_result.returncode != 0:
                return []

            allowed_suffixes = {".png", ".html", ".pdf"}
            artifact_paths = [
                p for p in copied_root.rglob("*") if p.is_file() and p.suffix.lower() in allowed_suffixes
            ]
            saved_files: list[str] = []
            for artifact_path in artifact_paths:
                target_name = f"{run_id}_{artifact_path.name}"
                target_path = self.outputs_dir / target_name
                target_path.write_bytes(artifact_path.read_bytes())
                saved_files.append(str(target_path))
            return saved_files

    async def _cleanup_container(self, container_name: str, force: bool = False) -> None:
        print("[DEBUG] QuantNode LocalDockerSandbox._cleanup_container Start")
        cmd = ["docker", "rm"]
        if force:
            cmd.append("-f")
        cmd.append(container_name)
        await self._run_cmd(cmd, err_prefix="Failed to cleanup container", allow_nonzero=True)

    @staticmethod
    def _extract_tar_single_file(raw_bytes: bytes) -> str:
        import io
        import tarfile

        if not raw_bytes:
            return ""
        file_obj = io.BytesIO(raw_bytes)
        with tarfile.open(fileobj=file_obj, mode="r:*") as tar:
            members = [m for m in tar.getmembers() if m.isfile()]
            if not members:
                return ""
            extracted = tar.extractfile(members[0])
            if extracted is None:
                return ""
            return extracted.read().decode("utf-8", errors="replace")

    async def _run_cmd(
        self,
        cmd: list[str],
        err_prefix: str,
        allow_nonzero: bool = False,
    ) -> _CmdResult:
        print("[DEBUG] QuantNode LocalDockerSandbox._run_cmd Start")
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_b, stderr_b = await proc.communicate()

        result = _CmdResult(
            returncode=proc.returncode,
            stdout=stdout_b.decode("utf-8", errors="replace"),
            stderr=stderr_b.decode("utf-8", errors="replace"),
            stdout_bytes=stdout_b,
            stderr_bytes=stderr_b,
        )
        if not allow_nonzero and result.returncode != 0:
            raise SandboxInfrastructureError(
                f"{err_prefix}. code={result.returncode}, stderr={result.stderr.strip()}"
            )
        return result


@dataclass
class _CmdResult:
    returncode: int
    stdout: str
    stderr: str
    stdout_bytes: bytes
    stderr_bytes: bytes
