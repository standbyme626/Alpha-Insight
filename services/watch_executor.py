from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Protocol

from agents.scanner_engine import ScanConfig, WatchlistRunResult, build_scan_trigger, format_signal_message, run_watchlist_cycle
from services.telegram_store import DueWatchJob, TelegramTaskStore


class ChatSender(Protocol):
    async def send_text(self, chat_id: str, text: str) -> dict[str, Any]:
        ...


ScanRunner = Callable[..., Awaitable[WatchlistRunResult]]


@dataclass
class WatchExecutionResult:
    job_id: str
    symbol: str
    triggered_count: int
    pushed_count: int
    dedupe_suppressed_count: int


class WatchExecutor:
    def __init__(
        self,
        *,
        store: TelegramTaskStore,
        notifier: ChatSender,
        scan_runner: ScanRunner = run_watchlist_cycle,
        dedupe_bucket_minutes: int = 15,
        enable_triggered_research: bool = True,
    ):
        self._store = store
        self._notifier = notifier
        self._scan_runner = scan_runner
        self._dedupe_bucket_minutes = dedupe_bucket_minutes
        self._enable_triggered_research = enable_triggered_research

    async def execute_job(self, job: DueWatchJob) -> WatchExecutionResult:
        config = ScanConfig(
            watchlist=[job.symbol],
            market=job.market,
            interval=self._interval_for_job(job.interval_sec),
            pct_alert_threshold=job.threshold,
        )
        trigger = build_scan_trigger(trigger_type="scheduled", trigger_id=f"watch-{job.job_id}")
        result = await self._scan_runner(
            config,
            trigger=trigger,
            mode=job.mode,
            notifier=None,
            enable_triggered_research=self._enable_triggered_research,
        )

        snapshot_by_symbol = {snapshot.signal.symbol: snapshot for snapshot in result.snapshots}
        pushed_count = 0
        dedupe_suppressed_count = 0

        for signal in result.selected_alerts:
            snapshot = snapshot_by_symbol.get(signal.symbol)
            run_id = snapshot.research_run_id if snapshot is not None else None
            event_id, created = self._store.record_watch_event_if_new(
                job_id=job.job_id,
                symbol=signal.symbol,
                trigger_ts=signal.timestamp,
                price=signal.price,
                pct_change=signal.pct_change,
                reason=signal.reason,
                rule=signal.reason,
                run_id=run_id,
                bucket_minutes=self._dedupe_bucket_minutes,
            )
            if not created:
                dedupe_suppressed_count += 1
                continue

            text = format_signal_message(signal)
            if run_id:
                text += f"\nrun_id={run_id}"

            try:
                await self._notifier.send_text(job.chat_id, text)
                self._store.mark_watch_event_pushed(event_id=event_id)
                self._store.upsert_notification_state(
                    event_id=event_id,
                    channel="telegram",
                    state="delivered",
                    retry_count=0,
                    delivered_at=datetime.now(timezone.utc).isoformat(),
                )
                self._store.mark_watch_job_triggered(job_id=job.job_id)
                pushed_count += 1
            except Exception as exc:  # pragma: no cover - defensive runtime branch.
                self._store.upsert_notification_state(
                    event_id=event_id,
                    channel="telegram",
                    state="failed",
                    retry_count=1,
                    last_error=str(exc),
                )

        return WatchExecutionResult(
            job_id=job.job_id,
            symbol=job.symbol,
            triggered_count=len(result.selected_alerts),
            pushed_count=pushed_count,
            dedupe_suppressed_count=dedupe_suppressed_count,
        )

    @staticmethod
    def _interval_for_job(interval_sec: int) -> str:
        if interval_sec <= 300:
            return "5m"
        if interval_sec <= 3600:
            return "60m"
        return "1d"
