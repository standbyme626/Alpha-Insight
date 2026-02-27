from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Protocol

from agents.workflow_engine import run_unified_research
from services.runtime_controls import GlobalConcurrencyGate, RuntimeLimits
from services.telegram_chart_service import TelegramChartService
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
        limits: RuntimeLimits | None = None,
        global_gate: GlobalConcurrencyGate | None = None,
        chart_service: TelegramChartService | None = None,
    ):
        self._store = store
        self._notifier = notifier
        self._research_runner = research_runner
        self._analysis_timeout_seconds = analysis_timeout_seconds
        self._limits = limits or RuntimeLimits()
        self._global_gate = global_gate or GlobalConcurrencyGate(self._limits.global_concurrency)
        self._chart_service = chart_service or TelegramChartService()

    async def handle_help(self, *, chat_id: str) -> ActionResult:
        await self._notifier.send_text(
            chat_id,
            "可用命令 (Available commands):\n"
            "/analyze <symbol> - 运行统一研究并返回 run_id (run unified research)\n"
            "/monitor <symbol> <interval> [volatility|price|rsi] - 创建监控任务 (create monitor job, legacy format)\n"
            "/monitor <symbol|sym1,sym2> <interval> [volatility|price|rsi] [telegram_only|webhook_only|dual_channel]\n"
            "/list - 查看监控任务列表 (list monitor jobs)\n"
            "/stop <job_id|symbol> - 停止监控任务 (disable monitor job)\n"
            "/report <run_id|request_id> [short|full] - 查询报告摘要 (query report summary)\n"
            "/digest daily - 获取近24小时摘要 (last-24h summary)\n"
            "/alerts [triggered|failed|suppressed] [limit] - 告警中心视图 (alert hub views)\n"
            "/bulk <enable|disable|interval|threshold> <target|all> [value] - 批量任务操作 (bulk job operations)\n"
            "/webhook <set|disable|list> ... - 管理 webhook 路由 (manage webhook route)\n"
            "/pref <summary|quiet|priority> <value> - 通知偏好设置 (notification preferences)\n"
            "合规提示 (Compliance): 仅用于研究与提醒，不支持自动交易 (no auto-trading).\n"
            "/help - 显示帮助 (show this help)",
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

        await self._notifier.send_text(chat_id, f"请求已受理 (Request accepted). request_id={rid}")
        await self._run_analysis_request(
            request_id=rid,
            symbol=symbol,
            request_text=f"Analyze {symbol}",
            chat_id=chat_id,
            timeout_seconds=self._analysis_timeout_seconds,
            timeout_retry_count=0,
        )
        return ActionResult(command="analyze", request_id=rid)

    async def handle_analyze_snapshot(
        self,
        *,
        chat_id: str,
        symbol: str,
        period: str,
        interval: str,
        need_chart: bool,
        need_news: bool,
        request_id: str | None = None,
    ) -> ActionResult:
        request_text = (
            f"Analyze snapshot for {symbol} period={period} interval={interval} "
            f"need_news={str(bool(need_news)).lower()}"
        )
        async with self._global_gate.acquire():
            result = await asyncio.wait_for(
                self._research_runner(
                    request=request_text,
                    symbol=symbol,
                    period=period,
                    interval=interval,
                    news_limit=8 if need_news else 0,
                ),
                timeout=self._analysis_timeout_seconds,
            )

        run_id = str(result.get("run_id", "")).strip()
        summary = str(((result.get("fused_insights") or {}).get("summary") or "")).strip() or "No summary."
        metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
        if run_id:
            self._store.upsert_analysis_report(
                run_id=run_id,
                request_id=request_id or run_id,
                chat_id=chat_id,
                symbol=symbol,
                summary=summary,
                key_metrics=metrics,
            )
        close_price = metrics.get("data_close")
        rsi = metrics.get("technical_rsi_14")
        metric_parts: list[str] = []
        if close_price is not None:
            metric_parts.append(f"close={round(float(close_price), 4)}")
        if rsi is not None:
            metric_parts.append(f"rsi14={round(float(rsi), 2)}")
        metric_line = " ".join(metric_parts) if metric_parts else "N/A"
        report_text = (
            f"快照分析 (Snapshot Analysis): {symbol}\n"
            f"关键指标 (Key metrics): {metric_line}\n"
            f"结论 (Conclusion): {summary[:300]}\n"
            f"run_id={run_id or 'N/A'}\n"
            f"下一步 (Next): /report {run_id or '<run_id>'} full"
        )

        await self._notifier.send_text(chat_id, report_text)

        if need_chart:
            chart_candidate = self._chart_service.extract_chart_path(result)
            chart_path, chart_size, chart_error = self._chart_service.ensure_chart_within_limit(chart_candidate)
            if chart_size is not None:
                self._store.record_metric(metric_name="chart_payload_bytes", metric_value=float(chart_size))
            if chart_path is None:
                self._store.record_metric(metric_name="chart_render_fail_rate", metric_value=1.0, tags={"reason": chart_error or "unknown"})
            else:
                sender = self._notifier
                if hasattr(sender, "send_photo"):
                    try:
                        await sender.send_photo(chat_id, str(chart_path), caption=f"{symbol} {period}/{interval} chart")  # type: ignore[attr-defined]
                    except Exception as exc:  # noqa: BLE001
                        self._store.record_metric(metric_name="chart_render_fail_rate", metric_value=1.0, tags={"reason": str(exc)[:64]})
                else:
                    self._store.record_metric(metric_name="chart_render_fail_rate", metric_value=1.0, tags={"reason": "send_photo_unavailable"})

        return ActionResult(command="analyze_snapshot", request_id=request_id)

    async def handle_monitor(
        self,
        *,
        chat_id: str,
        symbol: str,
        symbols: list[str] | None = None,
        interval_sec: int,
        mode: str = "anomaly",
        threshold: float = 0.03,
        template: str = "volatility",
        route_strategy: str = "dual_channel",
    ) -> ActionResult:
        if not self._store.can_chat_monitor(chat_id=chat_id):
            await self._notifier.send_text(
                chat_id,
                "无权限 (Permission denied): 当前 chat 不允许创建监控任务。",
            )
            return ActionResult(command="monitor")

        active_jobs = self._store.count_active_watch_jobs(chat_id=chat_id)
        if active_jobs >= self._limits.max_watch_jobs_per_chat:
            await self._notifier.send_text(
                chat_id,
                f"配额超限 (Quota exceeded): 单 chat 最大监控任务数为 {self._limits.max_watch_jobs_per_chat}。",
            )
            self._store.add_audit_event(
                event_type="quota_exceeded",
                chat_id=chat_id,
                update_id=None,
                action="monitor",
                reason="max_watch_jobs_per_chat",
                metadata={"active_jobs": active_jobs},
            )
            return ActionResult(command="monitor")

        symbol_list = symbols or [symbol]
        scope = "group" if len(symbol_list) > 1 else "single"
        group_id: str | None = None
        job_symbol = symbol
        if scope == "group":
            group_name = f"watch-{symbol_list[0]}-{len(symbol_list)}"
            group = self._store.create_or_replace_watchlist_group(chat_id=chat_id, name=group_name, symbols=symbol_list)
            group_id = group.group_id
            job_symbol = symbol_list[0]
        job = self._store.create_watch_job(
            chat_id=chat_id,
            symbol=job_symbol,
            scope=scope,
            group_id=group_id,
            route_strategy=route_strategy,
            template_id=template,
            interval_sec=interval_sec,
            market="auto",
            threshold=threshold,
            mode=mode,
        )
        await self._notifier.send_text(
            chat_id,
            f"监控已创建 (Monitor created): scope={scope} symbols={','.join(symbol_list)} every {job.interval_sec}s template={template} "
            f"mode={job.mode} threshold={job.threshold} route={route_strategy} "
            f"job_id={job.job_id} next_run_at={job.next_run_at}",
        )
        return ActionResult(command="monitor")

    async def handle_report(self, *, chat_id: str, target_id: str, detail: str = "short") -> ActionResult:
        self._store.record_metric(metric_name="report_lookup_total", metric_value=1.0)
        report = self._store.get_analysis_report(report_id=target_id, chat_id=chat_id)
        if report is None:
            await self._notifier.send_text(chat_id, f"未找到报告 (No report found) `{target_id}`。")
            return ActionResult(command="report")

        close_price = report.key_metrics.get("data_close")
        rsi = report.key_metrics.get("technical_rsi_14")
        metrics_text = []
        if close_price is not None:
            metrics_text.append(f"close={round(float(close_price), 4)}")
        if rsi is not None:
            metrics_text.append(f"rsi14={round(float(rsi), 2)}")
        metrics_suffix = f"\n关键指标 (Key metrics): {' '.join(metrics_text)}" if metrics_text else ""
        summary = report.summary[:220] if detail == "short" else report.summary[:1200]
        help_suffix = "" if detail == "full" else f"\n查看完整摘要请用 (Use) `/report {target_id} full`."
        await self._notifier.send_text(
            chat_id,
            f"报告摘要 (Report summary)\nrun_id={report.run_id}\nrequest_id={report.request_id}\n"
            f"symbol={report.symbol}\nsummary={summary}{metrics_suffix}{help_suffix}",
        )
        self._store.record_metric(metric_name="report_lookup_success", metric_value=1.0)
        return ActionResult(command="report")

    async def handle_digest(self, *, chat_id: str, period: str) -> ActionResult:
        digest = self._store.build_daily_digest(chat_id=chat_id) if period == "daily" else {}
        if not digest:
            await self._notifier.send_text(chat_id, "摘要周期不支持 (Digest period is not supported).")
            return ActionResult(command="digest")

        latest = digest.get("latest_reports") or []
        lines = [
            "每日报告 / Daily digest (last 24h)",
            f"active_jobs={digest['active_jobs']}",
            f"alerts_triggered={digest['alerts_triggered']}",
            f"delivered_notifications={digest['delivered_notifications']}",
            f"completed_analyses={digest['completed_analyses']}",
        ]
        if latest:
            lines.append("最新报告 (latest_reports):")
            for item in latest:
                lines.append(f"- {item['symbol']} run_id={item['run_id']}")
        await self._notifier.send_text(chat_id, "\n".join(lines))
        self._store.record_metric(metric_name="digest_generated", metric_value=1.0)
        return ActionResult(command="digest")

    async def handle_list(self, *, chat_id: str) -> ActionResult:
        jobs = self._store.list_watch_jobs(chat_id=chat_id, include_disabled=False)
        if not jobs:
            await self._notifier.send_text(chat_id, "当前无活跃监控任务 (No active monitor jobs)。可用 /monitor <symbol> <interval> 创建。")
            return ActionResult(command="list")

        lines = ["活跃监控任务 (Active monitor jobs):"]
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

    async def handle_alerts(self, *, chat_id: str, view: str, limit: int) -> ActionResult:
        rows = self._store.list_alert_hub(chat_id=chat_id, view=view, limit=limit)
        if not rows:
            await self._notifier.send_text(chat_id, f"视图 {view} 暂无告警记录 (No alert records).")
            return ActionResult(command="alerts")
        lines = [f"告警中心 (Alert Hub) view={view} (latest {limit})"]
        for row in rows:
            extra = ""
            if row.suppressed_reason:
                extra = f" suppressed={row.suppressed_reason}"
            elif row.last_error:
                extra = f" error={row.last_error[:80]}"
            lines.append(f"- {row.event_id} {row.symbol} {row.priority} {row.channel}:{row.status}{extra}")
        await self._notifier.send_text(chat_id, "\n".join(lines))
        return ActionResult(command="alerts")

    async def handle_bulk(self, *, chat_id: str, action: str, target: str, value: str = "") -> ActionResult:
        changed = self._store.bulk_update_watch_jobs(chat_id=chat_id, action=action, target=target, value=value)
        await self._notifier.send_text(chat_id, f"批量更新完成 (Bulk update done): action={action} target={target} changed={changed}")
        return ActionResult(command="bulk")

    async def handle_webhook(
        self,
        *,
        chat_id: str,
        action: str,
        url: str = "",
        secret: str = "",
        webhook_id: str = "",
    ) -> ActionResult:
        if action == "set":
            hook = self._store.upsert_outbound_webhook(chat_id=chat_id, url=url, secret=secret)
            await self._notifier.send_text(chat_id, f"Webhook 已启用 (Webhook enabled): {hook.webhook_id} -> {hook.url}")
            return ActionResult(command="webhook")
        if action == "disable":
            ok = self._store.disable_outbound_webhook(chat_id=chat_id, webhook_id=webhook_id)
            await self._notifier.send_text(chat_id, "Webhook 已禁用 (Webhook disabled)." if ok else "未找到 Webhook (Webhook not found).")
            return ActionResult(command="webhook")
        hooks = self._store.list_outbound_webhooks(chat_id=chat_id, enabled_only=False)
        if not hooks:
            await self._notifier.send_text(chat_id, "未配置 Webhook (No webhook configured).")
            return ActionResult(command="webhook")
        lines = ["Webhook 路由 (Webhook routes):"]
        for hook in hooks:
            lines.append(f"- {hook.webhook_id} enabled={hook.enabled} timeout_ms={hook.timeout_ms} url={hook.url}")
        await self._notifier.send_text(chat_id, "\n".join(lines))
        return ActionResult(command="webhook")

    async def handle_pref(self, *, chat_id: str, setting: str, value: str) -> ActionResult:
        if setting == "summary":
            pref = self._store.upsert_chat_preferences(chat_id=chat_id, summary_mode=value)
        elif setting == "priority":
            pref = self._store.upsert_chat_preferences(chat_id=chat_id, min_priority=value)
        elif setting == "quiet":
            pref = self._store.upsert_chat_preferences(chat_id=chat_id, quiet_hours=None if value == "off" else value)
        else:
            await self._notifier.send_text(chat_id, "不支持的偏好设置 (Unsupported preference setting).")
            return ActionResult(command="pref")
        await self._notifier.send_text(
            chat_id,
            f"偏好已更新 (Preference updated): summary={pref.summary_mode} priority={pref.min_priority} quiet={pref.quiet_hours or 'off'}",
        )
        return ActionResult(command="pref")

    async def handle_stop(self, *, chat_id: str, target: str, target_type: str) -> ActionResult:
        if not self._store.can_chat_monitor(chat_id=chat_id):
            await self._notifier.send_text(
                chat_id,
                "无权限 (Permission denied): 当前 chat 不允许停止监控任务。",
            )
            return ActionResult(command="stop")

        disabled = self._store.disable_watch_job(chat_id=chat_id, target=target, target_type=target_type)
        if disabled <= 0:
            await self._notifier.send_text(chat_id, f"未匹配到活跃监控任务 (No active monitor job matched): {target}")
            return ActionResult(command="stop")

        await self._notifier.send_text(chat_id, f"已停止监控任务 (Stopped) {disabled} 个，target={target}")
        return ActionResult(command="stop")

    async def process_due_analysis_recovery(self, *, limit: int = 10) -> int:
        due = self._store.claim_due_analysis_recovery(limit=limit)
        handled = 0
        for item in due:
            await self._run_analysis_request(
                request_id=item.request_id,
                symbol=item.symbol,
                request_text=f"Analyze {item.symbol}",
                chat_id=item.chat_id,
                timeout_seconds=max(self._analysis_timeout_seconds, 180.0),
                timeout_retry_count=item.retry_count,
            )
            handled += 1
        return handled

    async def _run_analysis_request(
        self,
        *,
        request_id: str,
        symbol: str,
        request_text: str,
        chat_id: str,
        timeout_seconds: float,
        timeout_retry_count: int,
    ) -> None:
        transitioned = self._store.transition_analysis_request_status(
            request_id=request_id,
            from_statuses=("queued", "timeout"),
            to_status="running",
        )
        if not transitioned:
            return
        start = time.perf_counter()
        try:
            async with self._global_gate.acquire():
                result = await asyncio.wait_for(
                    self._research_runner(request=request_text, symbol=symbol),
                    timeout=timeout_seconds,
                )
            run_id = str(result.get("run_id", ""))
            summary = str(((result.get("fused_insights") or {}).get("summary") or "")).strip()
            key_metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
            self._store.transition_analysis_request_status(
                request_id=request_id,
                from_statuses=("running",),
                to_status="completed",
                run_id=run_id,
                last_error=None,
            )
            if run_id:
                self._store.upsert_analysis_report(
                    run_id=run_id,
                    request_id=request_id,
                    chat_id=chat_id,
                    symbol=symbol,
                    summary=summary or "No summary",
                    key_metrics=key_metrics,
                )
            await self._notifier.send_text(chat_id, f"分析完成 (Analysis completed). request_id={request_id}, run_id={run_id}")
        except TimeoutError:
            self._store.transition_analysis_request_status(
                request_id=request_id,
                from_statuses=("running",),
                to_status="timeout",
                run_id=None,
                last_error="analysis timeout",
            )
            retry_count = timeout_retry_count + 1
            backoff_seconds = min(300, 30 * (2 ** max(0, retry_count - 1)))
            self._store.enqueue_analysis_recovery(
                request_id=request_id,
                chat_id=chat_id,
                symbol=symbol,
                retry_count=retry_count,
                next_retry_at=datetime.now(timezone.utc) + timedelta(seconds=backoff_seconds),
                last_error="analysis timeout",
            )
            await self._notifier.send_text(
                chat_id,
                f"分析超时 (Analysis timeout). request_id={request_id}. 结果将后台重试 (retried in background).",
            )
        except Exception as exc:
            self._store.transition_analysis_request_status(
                request_id=request_id,
                from_statuses=("running",),
                to_status="failed",
                run_id=None,
                last_error=str(exc),
            )
            await self._notifier.send_text(chat_id, f"分析失败 (Analysis failed). request_id={request_id}, error={exc}")
        finally:
            latency_ms = (time.perf_counter() - start) * 1000
            self._store.record_metric(metric_name="analysis_latency_ms", metric_value=latency_ms, tags={"chat_id": chat_id})
