from __future__ import annotations

import pytest

from core.sandbox_manager import SandboxManager
from core.sandbox_policy import GuardrailError, SandboxPolicy


class _RecordingPolicy:
    def __init__(self) -> None:
        self.backends: list[str] = []
        self._delegate = SandboxPolicy()

    def enforce(
        self,
        code: str,
        *,
        backend: str,
        timeout_seconds: int,
        tool_permissions: tuple[str, ...] | None = None,
    ) -> None:
        self.backends.append(backend)
        self._delegate.enforce(
            code,
            backend=backend,
            timeout_seconds=timeout_seconds,
            tool_permissions=tool_permissions,
        )


@pytest.mark.asyncio
async def test_policy_runtime_consistency_for_docker_and_local_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    policy = _RecordingPolicy()
    manager = SandboxManager(use_local_fallback=True, policy=policy)  # type: ignore[arg-type]
    manager._session = "local-docker"

    async def fake_execute_code(code: str) -> dict:  # noqa: ANN001
        raise RuntimeError("permission denied while trying to connect to the docker API at unix:///var/run/docker.sock")

    async def fake_local_process(code: str, *, reason: str, timeout_seconds: int) -> dict:  # noqa: ANN001
        return {
            "stdout": "ok",
            "stderr": "",
            "exit_code": 0,
            "backend": "local-process-fallback",
            "duration_ms": 7.0,
            "resource_usage": {"max_rss_kb": 25600},
            "output_files": [],
            "images": [],
        }

    monkeypatch.setattr(manager._local, "execute_code", fake_execute_code)
    monkeypatch.setattr(manager, "_execute_local_process", fake_local_process)

    result = await manager.execute("print('ok')")

    assert result.exit_code == 0
    assert result.backend == "local-process-fallback"
    assert policy.backends == ["local-docker", "local-process-fallback"]


@pytest.mark.asyncio
async def test_policy_blocks_unsafe_code_before_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = SandboxManager(use_local_fallback=True)
    manager._session = "local-docker"
    invoked = {"runtime_called": False}

    async def fake_execute_code(code: str) -> dict:  # noqa: ANN001
        invoked["runtime_called"] = True
        return {"stdout": "", "stderr": "", "exit_code": 0}

    monkeypatch.setattr(manager._local, "execute_code", fake_execute_code)

    with pytest.raises(GuardrailError):
        await manager.execute("import requests\nrequests.get('https://example.com')")

    assert invoked["runtime_called"] is False


@pytest.mark.asyncio
async def test_execution_result_contract_normalized_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = SandboxManager(use_local_fallback=True)
    manager._session = "local-docker"

    async def fake_execute_code(code: str) -> dict:  # noqa: ANN001
        return {
            "stdout": "done",
            "stderr": "",
            "exit_code": 0,
            "backend": "docker:test",
            "duration_ms": 11.2,
            "resource_usage": {"memory_limit": "512m"},
            "images": ["storage/outputs/chart.png"],
            "output_files": ["storage/outputs/chart.png"],
        }

    monkeypatch.setattr(manager._local, "execute_code", fake_execute_code)
    result = await manager.execute("print('done')")

    assert result.stdout == "done"
    assert result.stderr == ""
    assert result.exit_code == 0
    assert result.traceback is None
    assert result.backend == "docker:test"
    assert result.duration_ms == pytest.approx(11.2)
    assert result.resource_usage == {"memory_limit": "512m"}
    assert result.output_files == ["storage/outputs/chart.png"]
    assert result.images == ["storage/outputs/chart.png"]
