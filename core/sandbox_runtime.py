"""Thin runtime wrapper around SandboxManager for explicit runtime wiring."""

from __future__ import annotations

from dataclasses import dataclass

from core.execution_result import ExecutionResult
from core.sandbox_manager import SandboxManager
from core.sandbox_policy import SandboxPolicy


@dataclass
class SandboxRuntime:
    manager: SandboxManager

    async def create_session(self) -> str:
        return await self.manager.create_session()

    async def execute(self, code: str) -> ExecutionResult:
        return await self.manager.execute(code)

    async def destroy_session(self) -> None:
        await self.manager.destroy_session()


def build_sandbox_runtime(
    *,
    policy: SandboxPolicy | None = None,
    use_local_fallback: bool = True,
    e2b_timeout_seconds: int = 30,
) -> SandboxRuntime:
    manager = SandboxManager(
        policy=policy,
        use_local_fallback=use_local_fallback,
        e2b_timeout_seconds=e2b_timeout_seconds,
    )
    return SandboxRuntime(manager=manager)
