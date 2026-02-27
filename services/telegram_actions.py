from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol

from agents.workflow_engine import run_unified_research
from services.telegram_store import TelegramTaskStore


class MessageSender(Protocol):
    async def send_text(self, chat_id: str, text: str) -> dict[str, Any]:
        ...


ResearchRunner = Callable[..., Awaitable[dict[str, Any]]]


@dataclass
class ActionResult:
    command: str
    request_id: str | None = None


class TelegramActions:
    def __init__(
        self,
        *,
        store: TelegramTaskStore,
        notifier: MessageSender,
        research_runner: ResearchRunner = run_unified_research,
        analysis_timeout_seconds: float = 90.0,
    ):
        self._store = store
        self._notifier = notifier
        self._research_runner = research_runner
        self._analysis_timeout_seconds = analysis_timeout_seconds

    async def handle_help(self, *, chat_id: str) -> ActionResult:
        await self._notifier.send_text(
            chat_id,
            "Available commands:\n"
            "/analyze <symbol> - run unified research and return run_id\n"
            "/monitor <symbol> <interval> - create monitor job (e.g. 1h)\n"
            "/list - list monitor jobs\n"
            "/stop <job_id|symbol> - disable monitor job\n"
            "/help - show this help",
        )
        return ActionResult(command="help")

    async def send_error_message(self, *, chat_id: str, text: str) -> None:
        await self._notifier.send_text(chat_id, text)

    async def handle_analyze(
        self,
        *,
        update_id: int,
        chat_id: str,
        symbol: str,
        request_id: str | None = None,
    ) -> ActionResult:
        rid = request_id or f"tg-{update_id}"
        payload = {"symbol": symbol}
        self._store.create_analysis_request_if_new(
            request_id=rid,
            update_id=update_id,
            chat_id=chat_id,
            payload=payload,
            status="queued",
        )

        await self._notifier.send_text(chat_id, f"Request accepted. request_id={rid}")
        await self._run_analysis_request(
            request_id=rid,
            symbol=symbol,
            request_text=f"Analyze {symbol}",
            chat_id=chat_id,
        )
        return ActionResult(command="analyze", request_id=rid)

    async def handle_monitor(self, *, chat_id: str, symbol: str, interval_sec: int) -> ActionResult:
        if not self._store.can_chat_monitor(chat_id=chat_id):
            await self._notifier.send_text(
                chat_id,
                "Permission denied: this chat is not allowed to create monitor jobs.",
            )
            return ActionResult(command="monitor")

        job = self._store.create_watch_job(
            chat_id=chat_id,
            symbol=symbol,
            interval_sec=interval_sec,
            market="auto",
            threshold=0.03,
            mode="anomaly",
        )
        await self._notifier.send_text(
            chat_id,
            f"Monitor created: {job.symbol} every {job.interval_sec}s, job_id={job.job_id}, next_run_at={job.next_run_at}",
        )
        return ActionResult(command="monitor")

    async def handle_list(self, *, chat_id: str) -> ActionResult:
        jobs = self._store.list_watch_jobs(chat_id=chat_id, include_disabled=False)
        if not jobs:
            await self._notifier.send_text(chat_id, "No active monitor jobs. Use /monitor <symbol> <interval>.")
            return ActionResult(command="list")

        lines = ["Active monitor jobs:"]
        for job in jobs:
            last_triggered_at, last_pct_change = self._store.get_recent_watch_event_summary(job_id=job.job_id)
            recent = "none"
            if last_triggered_at is not None and last_pct_change is not None:
                recent = f"{last_triggered_at} ({round(last_pct_change * 100, 2)}%)"
            lines.append(
                f"- {job.job_id} {job.symbol} every {job.interval_sec}s "
                f"next={job.next_run_at} last_triggered={recent}"
            )
        await self._notifier.send_text(chat_id, "\n".join(lines))
        return ActionResult(command="list")

    async def handle_stop(self, *, chat_id: str, target: str, target_type: str) -> ActionResult:
        if not self._store.can_chat_monitor(chat_id=chat_id):
            await self._notifier.send_text(
                chat_id,
                "Permission denied: this chat is not allowed to stop monitor jobs.",
            )
            return ActionResult(command="stop")

        disabled = self._store.disable_watch_job(chat_id=chat_id, target=target, target_type=target_type)
        if disabled <= 0:
            await self._notifier.send_text(chat_id, f"No active monitor job matched: {target}")
            return ActionResult(command="stop")

        await self._notifier.send_text(chat_id, f"Stopped {disabled} monitor job(s) for target={target}")
        return ActionResult(command="stop")

    async def _run_analysis_request(
        self,
        *,
        request_id: str,
        symbol: str,
        request_text: str,
        chat_id: str,
    ) -> None:
        transitioned = self._store.transition_analysis_request_status(
            request_id=request_id,
            from_statuses=("queued",),
            to_status="running",
        )
        if not transitioned:
            return
        try:
            result = await asyncio.wait_for(
                self._research_runner(request=request_text, symbol=symbol),
                timeout=self._analysis_timeout_seconds,
            )
            run_id = str(result.get("run_id", ""))
            self._store.transition_analysis_request_status(
                request_id=request_id,
                from_statuses=("running",),
                to_status="completed",
                run_id=run_id,
                last_error=None,
            )
            await self._notifier.send_text(chat_id, f"Analysis completed. request_id={request_id}, run_id={run_id}")
        except Exception as exc:
            self._store.transition_analysis_request_status(
                request_id=request_id,
                from_statuses=("running",),
                to_status="failed",
                run_id=None,
                last_error=str(exc),
            )
            await self._notifier.send_text(chat_id, f"Analysis failed. request_id={request_id}, error={exc}")
