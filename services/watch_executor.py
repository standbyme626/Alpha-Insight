from __future__ import annotations

import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Protocol

import aiohttp

from agents.scanner_engine import ScanConfig, WatchlistRunResult, build_scan_trigger, format_signal_message, run_watchlist_cycle
from services.notification_channels import MultiChannelNotifier
from services.runtime_controls import GlobalConcurrencyGate, RuntimeLimits
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
        limits: RuntimeLimits | None = None,
        global_gate: GlobalConcurrencyGate | None = None,
        multi_channel_notifier: MultiChannelNotifier | None = None,
    ):
        self._store = store
        self._scan_runner = scan_runner
        self._dedupe_bucket_minutes = dedupe_bucket_minutes
        self._enable_triggered_research = enable_triggered_research
        self._limits = limits or RuntimeLimits()
        self._global_gate = global_gate or GlobalConcurrencyGate(self._limits.global_concurrency)
        if multi_channel_notifier is None:
            self._multi_channel = MultiChannelNotifier(
                telegram=_TelegramTargetedSender(notifier),
                webhook=_WebhookTargetedSender(store=store),
            )
        else:
            self._multi_channel = multi_channel_notifier

    async def execute_job(self, job: DueWatchJob) -> WatchExecutionResult:
        self._store.record_metric(metric_name="monitor_executed", metric_value=1.0, tags={"job_id": job.job_id})

        enable_research = self._enable_triggered_research and (
            not self._store.is_degradation_active(state_key="disable_critical_research")
        )
        if not enable_research:
            self._store.add_audit_event(
                event_type="degrade_skip",
                chat_id=job.chat_id,
                update_id=None,
                action="critical_research",
                reason="disable_critical_research",
                metadata={"job_id": job.job_id, "symbol": job.symbol},
            )

        config = ScanConfig(
            watchlist=self._job_watchlist(job),
            market=job.market,
            interval=self._interval_for_job(job.interval_sec),
            pct_alert_threshold=job.threshold,
        )
        trigger = build_scan_trigger(trigger_type="scheduled", trigger_id=f"watch-{job.job_id}")
        async with self._global_gate.acquire():
            result = await self._scan_runner(
                config,
                trigger=trigger,
                mode=job.mode,
                notifier=None,
                enable_triggered_research=enable_research,
            )

        snapshot_by_symbol = {snapshot.signal.symbol: snapshot for snapshot in result.snapshots}
        pushed_count = 0
        dedupe_suppressed_count = 0

        ordered_alerts = self._sort_signals_for_dispatch(result.selected_alerts)
        for signal in ordered_alerts:
            self._store.record_metric(metric_name="monitor_trigger", metric_value=1.0, tags={"job_id": job.job_id})
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
                priority=str(signal.priority),
                run_id=run_id,
                bucket_minutes=self._dedupe_bucket_minutes,
            )
            if not created:
                dedupe_suppressed_count += 1
                self._store.record_metric(metric_name="dedupe_suppressed_count", metric_value=1.0)
                continue

            notification_payload = self._store.get_watch_event(event_id=event_id)
            if not notification_payload:
                continue
            delivered = await self._dispatch_notification(notification_payload, retry_count=0, channel_filter=None)
            if delivered:
                self._store.mark_watch_job_triggered(job_id=job.job_id)
                pushed_count += 1

        return WatchExecutionResult(
            job_id=job.job_id,
            symbol=job.symbol,
            triggered_count=len(result.selected_alerts),
            pushed_count=pushed_count,
            dedupe_suppressed_count=dedupe_suppressed_count,
        )

    async def process_retry_queue(self, *, limit: int = 20) -> int:
        due = self._store.claim_due_notification_retries(limit=limit)
        pending: list[tuple[int, DueNotification, dict[str, Any]]] = []
        for item in due:
            payload = self._store.get_watch_event(event_id=item.event_id)
            if not payload:
                continue
            pending.append((self._priority_rank(str(payload.get("priority", "medium"))), item, payload))
        pending.sort(key=lambda item: item[0])

        delivered = 0
        for _rank, item, payload in pending:
            if await self._dispatch_notification(payload, retry_count=item.retry_count, channel_filter=item.channel):
                delivered += 1
        return delivered

    async def _dispatch_notification(
        self,
        payload: dict[str, Any],
        *,
        retry_count: int,
        channel_filter: str | None,
    ) -> bool:
        event_id = str(payload["event_id"])
        chat_id = str(payload["chat_id"])
        job_id = str(payload["job_id"])
        symbol = str(payload["symbol"])
        routes = self._resolve_routes(chat_id=chat_id, payload=payload, channel_filter=channel_filter)
        prefs = self._store.get_chat_preferences(chat_id=chat_id)
        priority = str(payload.get("priority", "medium")).lower()
        lane = self._lane_for_priority(priority)
        routes = self._sort_routes_for_priority(routes=routes, priority=priority)

        if self._store.is_degradation_active(state_key="no_monitor_push"):
            for channel, _target in routes:
                self._store.upsert_notification_state(
                    event_id=event_id,
                    channel=channel,
                    state="suppressed",
                    retry_count=retry_count,
                    suppressed_reason="no_monitor_push",
                    reason="no_monitor_push",
                )
            self._store.add_audit_event(
                event_type="degrade_skip",
                chat_id=chat_id,
                update_id=None,
                action="monitor_push",
                reason="no_monitor_push",
                metadata={"event_id": event_id, "job_id": job_id},
            )
            return False

        summary_mode = self._store.is_degradation_active(state_key="summary_mode")
        summary_mode = summary_mode or prefs.summary_mode == "short"
        if self._in_quiet_hours(prefs.quiet_hours) and priority != "critical":
            for channel, _target in routes:
                self._store.upsert_notification_state(
                    event_id=event_id,
                    channel=channel,
                    state="suppressed",
                    retry_count=retry_count,
                    suppressed_reason="quiet_hours",
                    reason="quiet_hours",
                )
            return False
        if not self._priority_allowed(min_priority=prefs.min_priority, event_priority=priority):
            for channel, _target in routes:
                self._store.upsert_notification_state(
                    event_id=event_id,
                    channel=channel,
                    state="suppressed",
                    retry_count=retry_count,
                    suppressed_reason="preference_priority",
                    reason="preference_priority",
                )
            return False

        if summary_mode and self._is_throttled(job_id=job_id, interval_sec=900):
            for channel, _target in routes:
                self._store.upsert_notification_state(
                    event_id=event_id,
                    channel=channel,
                    state="suppressed",
                    retry_count=retry_count,
                    suppressed_reason="summary_mode_throttle",
                    reason="summary_mode_throttle",
                )
            self._store.add_audit_event(
                event_type="degrade_skip",
                chat_id=chat_id,
                update_id=None,
                action="monitor_push",
                reason="summary_mode_throttle",
                metadata={"event_id": event_id, "symbol": symbol},
            )
            return False

        text = self._build_message(payload, summary_mode=summary_mode)
        webhook_body = self._build_webhook_payload(payload)
        delivered_any = False
        for channel, target in routes:
            self._record_dispatch_attempt(job_id=job_id, channel=channel, lane=lane)

            dispatch_text = webhook_body if channel == "webhook" else text
            dispatch_started = time.perf_counter()
            result = await self._multi_channel.dispatch(channel=channel, target=target, text=dispatch_text)
            immediate_retry_budget = 0
            if retry_count == 0 and lane == "fast":
                immediate_retry_budget = max(0, int(self._limits.critical_fast_lane_immediate_retries))

            while (not result.delivered) and immediate_retry_budget > 0:
                immediate_retry_budget -= 1
                self._store.record_metric(
                    metric_name="fast_lane_immediate_retry_total",
                    metric_value=1.0,
                    tags={"job_id": job_id, "channel": channel},
                )
                self._record_dispatch_attempt(job_id=job_id, channel=channel, lane=lane)
                result = await self._multi_channel.dispatch(channel=channel, target=target, text=dispatch_text)
                if result.delivered:
                    self._store.record_metric(
                        metric_name="fast_lane_immediate_retry_recovered_total",
                        metric_value=1.0,
                        tags={"job_id": job_id, "channel": channel},
                    )

            self._store.record_metric(
                metric_name="lane_dispatch_latency_ms",
                metric_value=(time.perf_counter() - dispatch_started) * 1000,
                tags={"job_id": job_id, "channel": channel, "lane": lane},
            )
            if result.delivered:
                self._store.upsert_notification_state(
                    event_id=event_id,
                    channel=channel,
                    state="delivered",
                    retry_count=retry_count,
                    delivered_at=datetime.now(timezone.utc).isoformat(),
                    reason="delivery_success",
                )
                self._record_dispatch_success(job_id=job_id, channel=channel, lane=lane)
                delivered_any = True
                continue
            self._record_channel_failure(
                event_id=event_id,
                chat_id=chat_id,
                channel=channel,
                retry_count=retry_count,
                priority=priority,
                error=result.error or "delivery_failed",
            )

        if delivered_any:
            self._store.mark_watch_event_pushed(event_id=event_id)
        return delivered_any

    def _resolve_routes(self, *, chat_id: str, payload: dict[str, Any], channel_filter: str | None) -> list[tuple[str, str]]:
        route_strategy = str(payload.get("route_strategy", "dual_channel"))
        telegram_routes = self._store.list_notification_routes(chat_id=chat_id, enabled_only=True)
        selected: list[tuple[str, str]] = []
        if route_strategy == "telegram_only":
            selected.extend((item.channel, item.target) for item in telegram_routes if item.channel == "telegram")
            if not any(channel == "telegram" for channel, _ in selected):
                selected.append(("telegram", chat_id))
        elif route_strategy == "dual_channel":
            selected.extend((item.channel, item.target) for item in telegram_routes if item.channel != "webhook")
            if not selected:
                selected.append(("telegram", chat_id))
        if route_strategy in {"webhook_only", "dual_channel"}:
            hooks = self._store.list_outbound_webhooks(chat_id=chat_id, enabled_only=True)
            selected.extend(("webhook", hook.webhook_id) for hook in hooks)
        if channel_filter:
            selected = [item for item in selected if item[0] == channel_filter]
            if not selected:
                fallback = self._store.get_notification_route_target(chat_id=chat_id, channel=channel_filter)
                if fallback:
                    selected = [(channel_filter, fallback)]
                elif channel_filter == "telegram":
                    selected = [("telegram", chat_id)]
        return selected

    def _record_channel_failure(
        self,
        *,
        event_id: str,
        chat_id: str,
        channel: str,
        retry_count: int,
        priority: str,
        error: str,
    ) -> None:
        next_retry = retry_count + 1
        if next_retry >= self._limits.notification_max_retry:
            self._store.upsert_notification_state(
                event_id=event_id,
                channel=channel,
                state="dlq",
                retry_count=next_retry,
                last_error=error,
                reason="max_retry_exceeded",
            )
            self._store.add_audit_event(
                event_type="notification_dlq",
                chat_id=chat_id,
                update_id=None,
                action=f"push:{channel}",
                reason=error,
                metadata={"event_id": event_id, "retries": next_retry, "channel": channel},
            )
            return
        lane = self._lane_for_priority(priority)
        if lane == "fast":
            backoff_seconds = min(120, 5 * (2 ** max(0, next_retry - 1)))
        else:
            backoff_seconds = min(300, 15 * (2 ** max(0, next_retry - 1)))
        self._store.upsert_notification_state(
            event_id=event_id,
            channel=channel,
            state="retry_pending",
            retry_count=next_retry,
            next_retry_at=(datetime.now(timezone.utc) + timedelta(seconds=backoff_seconds)).isoformat(),
            last_error=error,
            reason="delivery_failed",
        )
        if lane == "fast":
            self._store.record_metric(
                metric_name="fast_lane_retry_queue_depth",
                metric_value=float(self._store.count_retry_queue_depth()),
            )

    def _is_throttled(self, *, job_id: str, interval_sec: int) -> bool:
        job = self._store.get_watch_job(job_id=job_id)
        if not job.last_triggered_at:
            return False
        last = datetime.fromisoformat(job.last_triggered_at)
        return (datetime.now(timezone.utc) - last).total_seconds() < interval_sec

    @staticmethod
    def _build_message(payload: dict[str, Any], *, summary_mode: bool) -> str:
        if summary_mode:
            return (
                f"[降级摘要 (Degraded Summary)] {payload['symbol']} {round(float(payload['pct_change']) * 100, 2)}% "
                f"price={round(float(payload['price']), 4)}"
            )
        pseudo_signal = type(
            "Signal",
            (),
            {
                "symbol": payload["symbol"],
                "timestamp": datetime.fromisoformat(str(payload["trigger_ts"])),
                "price": float(payload["price"]),
                "pct_change": float(payload["pct_change"]),
                "rsi": 0.0,
                "priority": "medium",
                "reason": str(payload["reason"]),
                "company_name": str(payload["symbol"]),
            },
        )()
        text = format_signal_message(pseudo_signal)
        if payload.get("run_id"):
            text += f"\nrun_id={payload['run_id']}"
        return text

    @staticmethod
    def _build_webhook_payload(payload: dict[str, Any]) -> str:
        event = {
            "event_id": payload["event_id"],
            "dedupe_key": payload.get("dedupe_key"),
            "run_id": payload.get("run_id"),
            "priority": payload.get("priority", "medium"),
            "symbol": payload["symbol"],
            "job_id": payload["job_id"],
            "trigger_ts": payload["trigger_ts"],
            "metrics": {
                "price": payload["price"],
                "pct_change": payload["pct_change"],
                "reason": payload["reason"],
                "rule": payload["rule"],
            },
        }
        return json.dumps(event, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _priority_allowed(*, min_priority: str, event_priority: str) -> bool:
        ranking = {"all": 0, "high": 1, "critical": 2}
        event_rank = {"low": 0, "medium": 1, "high": 1, "critical": 2}
        return event_rank.get(event_priority, 1) >= ranking.get(min_priority, 1)

    @staticmethod
    def _in_quiet_hours(quiet_hours: str | None) -> bool:
        if not quiet_hours:
            return False
        try:
            start_raw, end_raw = quiet_hours.split("-")
            start = int(start_raw)
            end = int(end_raw)
        except ValueError:
            return False
        now_hour = datetime.now(timezone.utc).hour
        if start == end:
            return True
        if start < end:
            return start <= now_hour < end
        return now_hour >= start or now_hour < end

    def _job_watchlist(self, job: DueWatchJob) -> list[str]:
        if job.scope == "group" and job.group_id:
            symbols = self._store.get_watchlist_group_symbols(group_id=job.group_id)
            if symbols:
                return symbols
        return [job.symbol]

    @staticmethod
    def _interval_for_job(interval_sec: int) -> str:
        if interval_sec <= 300:
            return "5m"
        if interval_sec <= 3600:
            return "60m"
        return "1d"

    @staticmethod
    def _lane_for_priority(priority: str) -> str:
        return "fast" if str(priority).lower() == "critical" else "batch"

    @staticmethod
    def _priority_rank(priority: str) -> int:
        ranking = {"critical": 0, "high": 1, "medium": 2, "normal": 3, "low": 4}
        return ranking.get(str(priority).lower(), 9)

    @classmethod
    def _sort_signals_for_dispatch(cls, signals: list[Any]) -> list[Any]:
        return sorted(signals, key=lambda signal: cls._priority_rank(str(getattr(signal, "priority", "medium"))))

    @staticmethod
    def _sort_routes_for_priority(*, routes: list[tuple[str, str]], priority: str) -> list[tuple[str, str]]:
        if str(priority).lower() != "critical":
            return routes
        channel_order = {"telegram": 0, "wecom": 1, "email": 2, "webhook": 3}
        return sorted(routes, key=lambda item: channel_order.get(item[0], 9))

    def _record_dispatch_attempt(self, *, job_id: str, channel: str, lane: str) -> None:
        self._store.record_metric(metric_name="push_attempt", metric_value=1.0, tags={"job_id": job_id, "channel": channel})
        self._store.record_metric(metric_name="channel_dispatch_attempt", metric_value=1.0, tags={"job_id": job_id, "channel": channel})
        self._store.record_metric(
            metric_name="lane_dispatch_attempt",
            metric_value=1.0,
            tags={"job_id": job_id, "channel": channel, "lane": lane},
        )

    def _record_dispatch_success(self, *, job_id: str, channel: str, lane: str) -> None:
        self._store.record_metric(metric_name="push_success", metric_value=1.0, tags={"job_id": job_id, "channel": channel})
        self._store.record_metric(metric_name="channel_dispatch_success", metric_value=1.0, tags={"job_id": job_id, "channel": channel})
        self._store.record_metric(
            metric_name="lane_dispatch_success",
            metric_value=1.0,
            tags={"job_id": job_id, "channel": channel, "lane": lane},
        )


class _TelegramTargetedSender:
    def __init__(self, sender: ChatSender):
        self._sender = sender

    async def send_text(self, target: str, text: str) -> dict[str, Any]:
        return await self._sender.send_text(target, text)


class _WebhookTargetedSender:
    def __init__(self, *, store: TelegramTaskStore) -> None:
        self._store = store

    async def send_text(self, target: str, text: str) -> dict[str, Any]:
        hook = self._store.get_outbound_webhook(webhook_id=target)
        if hook is None or not hook.enabled:
            raise RuntimeError(f"webhook not found: {target}")
        signature = hmac.new(hook.secret.encode("utf-8"), text.encode("utf-8"), hashlib.sha256).hexdigest()
        headers = {"content-type": "application/json", "x-alpha-insight-signature": signature}
        timeout = aiohttp.ClientTimeout(total=max(0.5, hook.timeout_ms / 1000))
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(hook.url, data=text.encode("utf-8"), headers=headers) as response:
                if response.status >= 400:
                    body = await response.text()
                    raise RuntimeError(f"webhook_http_{response.status}:{body[:120]}")
        return {"ok": True}
