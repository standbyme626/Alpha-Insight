from __future__ import annotations

import pytest

from core.sandbox_manager import SandboxManager


@pytest.mark.asyncio
async def test_sandbox_manager_fallback_to_local_process_on_docker_permission() -> None:
    manager = SandboxManager(use_local_fallback=True)
    manager._session = "local-docker"

    async def fake_execute_code(code: str) -> dict:  # noqa: ANN001
        raise RuntimeError("permission denied while trying to connect to the docker API at unix:///var/run/docker.sock")

    manager._local.execute_code = fake_execute_code  # type: ignore[assignment]
    result = await manager.execute("print('ok-from-local-fallback')")
    assert result.exit_code == 0
    assert "ok-from-local-fallback" in result.stdout
    assert "local-process fallback" in result.stderr


@pytest.mark.asyncio
async def test_sandbox_manager_fallback_to_local_process_when_docker_binary_missing() -> None:
    manager = SandboxManager(use_local_fallback=True)
    manager._session = "local-docker"

    async def fake_execute_code(code: str) -> dict:  # noqa: ANN001
        raise FileNotFoundError(2, "No such file or directory", "docker")

    manager._local.execute_code = fake_execute_code  # type: ignore[assignment]
    result = await manager.execute("print('ok-missing-docker-bin')")
    assert result.exit_code == 0
    assert "ok-missing-docker-bin" in result.stdout
    assert "local-process fallback" in result.stderr
    assert "no such file or directory" in result.stderr.lower()
