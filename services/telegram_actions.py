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

    @staticmethod
    def _render_key_metrics(metrics: dict[str, Any]) -> tuple[str, str]:
        close_price = metrics.get("data_close", metrics.get("latest_close"))
        rsi = metrics.get("technical_rsi_14", metrics.get("rsi14"))
        metric_parts: list[str] = []
        missing: list[str] = []
        if close_price is not None:
            metric_parts.append(f"close={round(float(close_price), 4)}")
        else:
            missing.append("close")
        if rsi is not None:
            metric_parts.append(f"rsi14={round(float(rsi), 2)}")
        else:
            missing.append("rsi14")
        if metric_parts:
            return " ".join(metric_parts), ""
        reason = f"缺失原因: {','.join(missing)}"
        return reason, reason

    @staticmethod
    def _chart_reason_text(reason: str) -> str:
        mapping = {
            "artifact_missing": "未生成图表产物",
            "data_empty": "缺少可绘图的数据区间",
            "chart_oversize": "图表文件过大",
            "send_photo_error": "图表发送失败",
        }
        return mapping.get(reason, "图表生成失败")

    @staticmethod
    def _metric_float(metrics: dict[str, Any], *keys: str) -> float | None:
        for key in keys:
            value = metrics.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _period_window(period: str) -> str:
        now = datetime.now(timezone.utc).date()
        days = {"5d": 5, "1mo": 30, "3mo": 90, "6mo": 180, "1y": 365}.get(str(period).lower(), 30)
        start = now - timedelta(days=days)
        return f"{start.isoformat()} ~ {now.isoformat()}"

    @staticmethod
    def _extract_news(result: dict[str, Any], *, default_days: int = 7) -> tuple[int, str, str]:
        news = result.get("news")
        count = 0
        source = "aggregated_news"
        if isinstance(news, list):
            count = len(news)
            if news and isinstance(news[0], dict):
                source = str(news[0].get("source") or source)
        return count, f"近{default_days}天", source

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
        summary = str(((result.get("fused_insights") or {}).get("summary") or "")).strip() or "暂无摘要。"
        metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
        latest_close = self._metric_float(metrics, "latest_close", "data_close")
        data_window = str(metrics.get("data_window") or self._period_window(period))
        rsi = self._metric_float(metrics, "technical_rsi_14", "rsi14")
        ma = self._metric_float(metrics, "technical_ma_20", "ma20")
        high = self._metric_float(metrics, "window_high", "period_high")
        low = self._metric_float(metrics, "window_low", "period_low")
        ret30 = self._metric_float(metrics, "return_30d", "change_30d")
        news_count, news_window, news_source = self._extract_news(result)
        has_technical = latest_close is not None and bool(data_window) and (rsi is not None or ma is not None)
        has_range = (high is not None and low is not None) or (ret30 is not None)
        has_news = news_count >= 1
        safe_summary = summary if has_technical else "这次缺少价格证据，先不给趋势/情绪结论。"
        if run_id:
            self._store.upsert_analysis_report(
                run_id=run_id,
                request_id=request_id or run_id,
                chat_id=chat_id,
                symbol=symbol,
                summary=summary,
                key_metrics=metrics,
            )
        metric_line, missing_reason = self._render_key_metrics(metrics)
        evidence_lines: list[str] = []
        if latest_close is not None:
            evidence_lines.append(f"当前价={round(latest_close, 4)}")
        if has_range and high is not None and low is not None:
            evidence_lines.append(f"区间高低={round(low, 4)}~{round(high, 4)}")
        elif has_range and ret30 is not None:
            evidence_lines.append(f"近30天涨跌={round(ret30 * 100, 2)}%")
        evidence_lines.append(f"news_count={news_count} 时间窗={news_window} 来源={news_source}")
        if not evidence_lines:
            evidence_lines.append("图表状态=未生成")
        report_text = (
            f"快照分析 (Snapshot Analysis): {symbol}\n"
            f"分析区间={data_window}\n"
            f"关键指标 (Key metrics): {metric_line}\n"
            f"结论 (Conclusion): {safe_summary[:300]}\n"
            f"证据 (Evidence): {' | '.join(evidence_lines[:3])}\n"
            f"request_id(short)={(request_id or run_id or 'n/a')[-6:]}\n"
            f"run_id={run_id or 'N/A'}\n"
            f"下一步: 📈K线 📰新闻 🗓️1个月 🔁重试 📄报告"
        )

        await self._notifier.send_text(chat_id, report_text)
        self._store.record_metric(metric_name="analysis_response_total", metric_value=1.0)
        if missing_reason:
            self._store.record_metric(metric_name="analysis_explainable_total", metric_value=1.0)
        elif metric_line:
            self._store.record_metric(metric_name="analysis_explainable_total", metric_value=1.0)

        if need_chart:
            chart_candidate = self._chart_service.extract_chart_path(result)
            chart_path, chart_size, chart_error = self._chart_service.ensure_chart_within_limit(chart_candidate)
            if chart_size is not None:
                self._store.record_metric(metric_name="chart_payload_bytes", metric_value=float(chart_size))
            chart_reason = "data_empty" if latest_close is None else ("artifact_missing" if chart_error else "")
            if chart_path is None and chart_reason == "artifact_missing":
                self._store.record_metric(metric_name="chart_retry_attempted", metric_value=1.0)
                await self._notifier.send_text(chat_id, "首次未取到图表产物，已自动重试一次。")
                async with self._global_gate.acquire():
                    retry_result = await asyncio.wait_for(
                        self._research_runner(
                            request=request_text,
                            symbol=symbol,
                            period=period,
                            interval=interval,
                            news_limit=8 if need_news else 0,
                        ),
                        timeout=self._analysis_timeout_seconds,
                    )
                retry_candidate = self._chart_service.extract_chart_path(retry_result)
                retry_path, retry_size, retry_error = self._chart_service.ensure_chart_within_limit(retry_candidate)
                if retry_size is not None:
                    self._store.record_metric(metric_name="chart_payload_bytes", metric_value=float(retry_size))
                if retry_path is not None:
                    chart_path = retry_path
                    chart_error = None
                    self._store.record_metric(metric_name="chart_retry_success", metric_value=1.0)
                else:
                    chart_error = retry_error
            if chart_path is None:
                reason = "data_empty" if latest_close is None else "artifact_missing"
                self._store.record_metric(metric_name="chart_render_fail_rate", metric_value=1.0, tags={"reason": reason})
                await self._notifier.send_text(
                    chat_id,
                    (
                        "这次没能生成价格图表，我可以重试图表或只做新闻解读。\n"
                        f"原因: {self._chart_reason_text(reason)}\n"
                        "可选: 📈K线 📰新闻 🗓️1个月 🔁重试 📄报告"
                    ),
                )
            else:
                sender = self._notifier
                if hasattr(sender, "send_photo"):
                    try:
                        await sender.send_photo(chat_id, str(chart_path), caption=f"{symbol} {period}/{interval} chart")  # type: ignore[attr-defined]
                    except Exception:  # noqa: BLE001
                        reason = "send_photo_error"
                        self._store.record_metric(metric_name="chart_render_fail_rate", metric_value=1.0, tags={"reason": reason})
                        await self._notifier.send_text(
                            chat_id,
                            (
                                "图表发送失败，已降级为文本说明。\n"
                                f"原因: {self._chart_reason_text(reason)}\n"
                                f"request_id(short)={(request_id or run_id or 'n/a')[-6:]}"
                            ),
                        )
                else:
                    reason = "send_photo_error"
                    self._store.record_metric(metric_name="chart_render_fail_rate", metric_value=1.0, tags={"reason": reason})
                    await self._notifier.send_text(
                        chat_id,
                        (
                            "图表发送能力不可用，已降级为文本说明。\n"
                            f"原因: {self._chart_reason_text(reason)}"
                        ),
                    )
            if not has_news:
                await self._notifier.send_text(
                    chat_id,
                    "新闻回显: news_count=0 时间窗=近7天 来源=aggregated_news\n近7天未抓到新闻，可选：扩到30天 / 重试。",
                )

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

        metrics_line, _ = self._render_key_metrics(report.key_metrics)
        metrics_suffix = f"\n关键指标 (Key metrics): {metrics_line}" if metrics_line else ""
        summary = report.summary[:220] if detail == "short" else report.summary[:1200]
        help_suffix = "" if detail == "full" else f"\n查看完整摘要请用 (Use) `/report {target_id} full`."
        evidence_suffix = ""
        if detail == "full":
            evidence = self._store.list_nl_execution_evidence(request_id=report.request_id, limit=10)
            latest = evidence[0] if evidence else {}
            confidence = "high" if metrics_line and "Missing" not in metrics_line else "medium"
            metric_source_keys = sorted([str(key) for key in report.key_metrics.keys()])
            evidence_suffix = (
                "\n证据块 (Evidence):\n"
                f"- data_window={report.created_at} -> {report.updated_at}\n"
                f"- source=request_id:{report.request_id}\n"
                f"- metric_source_keys={','.join(metric_source_keys)}\n"
                f"- confidence_label={confidence}\n"
                f"- schema_version={latest.get('schema_version', 'unknown')}\n"
                f"- action_version={latest.get('action_version', 'unknown')}"
            )
        await self._notifier.send_text(
            chat_id,
            f"报告摘要 (Report summary)\nrun_id={report.run_id}\nrequest_id={report.request_id}\n"
            f"symbol={report.symbol}\nsummary={summary}{metrics_suffix}{evidence_suffix}{help_suffix}",
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
