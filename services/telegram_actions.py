from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Protocol

from agents.workflow_engine import run_unified_research
from services.notification_channels import TelegramChannelAdapter
from services.runtime_controls import GlobalConcurrencyGate, RuntimeLimits
from services.telegram_chart_service import TelegramChartService
from services.telegram_store import TelegramTaskStore


class MessageSender(Protocol):
    async def send_text(
        self,
        chat_id: str,
        text: str,
        reply_markup: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        ...


ResearchRunner = Callable[..., Awaitable[dict[str, Any]]]


@dataclass
class ActionResult:
    command: str
    request_id: str | None = None


class TelegramActions:
    _FORBIDDEN_USER_COPY = (
        "metrics unavailable",
        "chart_missing",
        "traceback",
        "_plan_steps",
        "_schema_version",
    )
    _ANALYZE_PROGRESS_TEXT = {
        "identify_symbol": "阶段进度 1/4：识别标的",
        "fetch_market_data": "阶段进度 2/4：拉取行情",
        "merge_news": "阶段进度 3/4：融合新闻",
        "process_chart": "阶段进度 4/4：图表处理",
    }

    def __init__(
        self,
        *,
        store: TelegramTaskStore,
        notifier: MessageSender,
        research_runner: ResearchRunner = run_unified_research,
        analysis_timeout_seconds: float | None = None,
        limits: RuntimeLimits | None = None,
        global_gate: GlobalConcurrencyGate | None = None,
        chart_service: TelegramChartService | None = None,
    ):
        self._store = store
        self._notifier = notifier
        self._channel = TelegramChannelAdapter(notifier)
        self._research_runner = research_runner
        self._limits = limits or RuntimeLimits()
        resolved_timeout = (
            float(analysis_timeout_seconds)
            if analysis_timeout_seconds is not None
            else float(self._limits.analysis_command_timeout_seconds)
        )
        self._analysis_command_timeout_seconds = resolved_timeout
        self._analysis_snapshot_timeout_seconds = (
            resolved_timeout
            if analysis_timeout_seconds is not None
            else float(self._limits.analysis_snapshot_timeout_seconds)
        )
        self._analysis_recovery_timeout_seconds = max(
            self._analysis_command_timeout_seconds,
            float(self._limits.analysis_recovery_timeout_seconds),
        )
        self._photo_send_timeout_seconds = max(0.001, float(self._limits.photo_send_timeout_seconds))
        self._typing_heartbeat_seconds = max(1.0, float(self._limits.typing_heartbeat_seconds))
        self._global_gate = global_gate or GlobalConcurrencyGate(self._limits.global_concurrency)
        self._chart_service = chart_service or TelegramChartService()

    async def _send_text(
        self,
        *,
        chat_id: str,
        text: str,
        buttons: list[list[tuple[str, str]]] | None = None,
    ) -> None:
        safe_text = self._sanitize_user_copy(text)
        blocked = safe_text != text
        if blocked:
            self._store.record_metric(metric_name="user_copy_guard_hit_total", metric_value=1.0)
        reply_markup = self._build_inline_keyboard(buttons) if buttons else None
        dispatch = await self._channel.send_text(chat_id=chat_id, text=safe_text, reply_markup=reply_markup)
        if not dispatch.delivered:
            raise RuntimeError(f"telegram_send_text_failed:{dispatch.error or 'unknown_error'}")

    @classmethod
    def _sanitize_user_copy(cls, text: str) -> str:
        sanitized = str(text or "")
        lowered = sanitized.lower()
        for token in cls._FORBIDDEN_USER_COPY:
            if token in lowered:
                sanitized = sanitized.replace(token, "内部细节")
                sanitized = sanitized.replace(token.upper(), "内部细节")
                sanitized = sanitized.replace(token.title(), "内部细节")
                lowered = sanitized.lower()
        return sanitized

    @staticmethod
    def _build_inline_keyboard(buttons: list[list[tuple[str, str]]] | None) -> dict[str, Any] | None:
        if not buttons:
            return None
        rows: list[list[dict[str, str]]] = []
        for row in buttons:
            items: list[dict[str, str]] = []
            for label, callback_data in row:
                label_text = str(label).strip()
                callback_text = str(callback_data).strip()
                if not label_text or not callback_text:
                    continue
                items.append({"text": label_text, "callback_data": callback_text[:64]})
            if items:
                rows.append(items)
        if not rows:
            return None
        return {"inline_keyboard": rows}

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
    def _resolve_chart_failure_reason(
        *,
        latest_close: float | None,
        chart_error: str | None,
    ) -> str:
        if latest_close is None:
            return "data_empty"
        if chart_error in {"artifact_missing", "chart_oversize"}:
            return chart_error
        return "artifact_missing"

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
    def _chart_state_label(state: str) -> str:
        value = str(state or "none").strip().lower()
        if value == "rendering":
            return "📈K线(生成中…)"
        if value == "ready":
            return "📈K线(已生成)"
        return "📈K线"

    def _snapshot_buttons(
        self,
        *,
        request_id: str | None,
        chart_state: str,
    ) -> list[list[tuple[str, str]]]:
        if not request_id:
            return []
        ref = request_id[-6:]
        return [
            [
                (self._chart_state_label(chart_state), f"act|{ref}|chart"),
                ("📰新闻(7天)", f"act|{ref}|news7"),
                ("📰新闻(30天)", f"act|{ref}|news30"),
            ],
            [
                ("🔁重试", f"act|{ref}|retry"),
                ("📄报告", f"act|{ref}|report"),
            ],
            [
                ("🗓️近3个月", f"act|{ref}|period3mo"),
                ("📰只看新闻", f"act|{ref}|news_only"),
                ("🔔设置监控", f"act|{ref}|set_monitor"),
            ],
            [
                ("❓为什么不给K线", f"act|{ref}|why_no_chart"),
                ("❓为什么不给RSI", f"act|{ref}|why_no_rsi"),
            ],
        ]

    def build_snapshot_buttons(
        self,
        *,
        request_id: str | None,
        chart_state: str,
    ) -> list[list[tuple[str, str]]]:
        return self._snapshot_buttons(request_id=request_id, chart_state=chart_state)

    @staticmethod
    def _extract_news(result: dict[str, Any], *, default_days: int = 7) -> tuple[int, str, str]:
        news = result.get("news")
        if not isinstance(news, list):
            news = result.get("news_items")
        if not isinstance(news, list):
            fused = result.get("fused_insights")
            if isinstance(fused, dict):
                raw = fused.get("raw")
                if isinstance(raw, dict):
                    news = raw.get("news_items")
        count = 0
        source = "aggregated_news"
        if isinstance(news, list):
            count = len(news)
            sources = [
                str(item.get("source", "")).strip()
                for item in news
                if isinstance(item, dict) and str(item.get("source", "")).strip()
            ]
            if sources:
                source = ",".join(list(dict.fromkeys(sources))[:2])
        return count, f"近{default_days}天", source

    async def send_inline_buttons(
        self,
        *,
        chat_id: str,
        text: str,
        buttons: list[list[tuple[str, str]]],
    ) -> None:
        await self._send_text(chat_id=chat_id, text=text, buttons=buttons)

    @staticmethod
    def _short_request_id(request_id: str | None) -> str:
        if not request_id:
            return "n/a"
        return request_id[-6:]

    @staticmethod
    def _capability_card_text(*, intent: str) -> str:
        header = "你好，我是 Alpha-Insight 投研助手。"
        if intent == "capability":
            header = "我可以帮你做这些事情："
        elif intent == "how_to_start":
            header = "从这里开始最简单："
        elif intent == "help":
            header = "你可以这样使用我："
        return (
            f"{header}\n"
            "能力卡 (Capability Card)\n"
            "1) 快速分析：输入“分析 TSLA 一个月走势”\n"
            "2) 监控提醒：输入“帮我盯 TSLA 每小时”\n"
            "3) 查询任务：输入“看看我的监控列表”\n"
            "示例提问：\n"
            "- 分析 0700.HK 近30天并给我K线\n"
            "- 给我 TSLA 最近7天新闻\n"
            "- 帮我设置 AAPL 每日波动提醒"
        )

    @staticmethod
    def _capability_buttons() -> list[list[tuple[str, str]]]:
        return [
            [("📈 快速分析", "guide|analyze"), ("🔔 创建监控", "guide|monitor")],
            [("📋 查看命令", "guide|help"), ("🧭 怎么开始", "guide|start")],
        ]

    async def handle_general_conversation(self, *, chat_id: str, intent: str) -> ActionResult:
        await self._send_text(
            chat_id=chat_id,
            text=self._capability_card_text(intent=intent),
            buttons=self._capability_buttons(),
        )
        return ActionResult(command=f"conversation_{intent}")

    async def send_analysis_ack(self, *, chat_id: str, request_id: str) -> None:
        await self._send_text(
            chat_id=chat_id,
            text=f"已受理请求，开始分析。request_id(short)={self._short_request_id(request_id)}",
        )

    async def send_analysis_progress(
        self,
        *,
        chat_id: str,
        request_id: str,
        stage: str,
    ) -> None:
        if not self._limits.send_progress_updates:
            return
        stage_text = self._ANALYZE_PROGRESS_TEXT.get(stage)
        if not stage_text:
            return
        dispatch = await self._channel.send_progress(
            chat_id=chat_id,
            text=f"{stage_text}\nrequest_id(short)={self._short_request_id(request_id)}",
            reply_markup=None,
        )
        if not dispatch.delivered:
            raise RuntimeError(f"telegram_send_progress_failed:{dispatch.error or 'unknown_error'}")

    async def _send_chat_action(self, *, chat_id: str, action: str = "typing") -> None:
        dispatch = await self._channel.send_chat_action(chat_id=chat_id, action=action)
        if not dispatch.delivered:
            return

    @contextlib.asynccontextmanager
    async def _typing_heartbeat(
        self,
        *,
        chat_id: str,
        action: str = "typing",
        interval_seconds: float | None = None,
    ):
        interval = self._typing_heartbeat_seconds if interval_seconds is None else max(0.0, float(interval_seconds))
        if interval <= 0:
            yield
            return

        stop_event = asyncio.Event()

        async def _pulse() -> None:
            while not stop_event.is_set():
                await self._send_chat_action(chat_id=chat_id, action=action)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval)
                except TimeoutError:
                    continue

        task = asyncio.create_task(_pulse())
        try:
            yield
        finally:
            stop_event.set()
            with contextlib.suppress(Exception):
                await task

    async def handle_help(self, *, chat_id: str) -> ActionResult:
        await self._send_text(
            chat_id=chat_id,
            text=(
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
            "/help - 显示帮助 (show this help)"
            ),
        )
        return ActionResult(command="help")

    async def send_error_message(self, *, chat_id: str, text: str) -> None:
        await self._send_text(chat_id=chat_id, text=text)

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

        await self._send_text(chat_id=chat_id, text=f"请求已受理 (Request accepted). request_id={rid}")
        await self._run_analysis_request(
            request_id=rid,
            symbol=symbol,
            request_text=f"Analyze {symbol}",
            chat_id=chat_id,
            timeout_seconds=self._analysis_command_timeout_seconds,
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
        news_window_days: int = 7,
        request_id: str | None = None,
    ) -> ActionResult:
        request_text = (
            f"Analyze snapshot for {symbol} period={period} interval={interval} "
            f"need_news={str(bool(need_news)).lower()}"
        )
        async with self._typing_heartbeat(chat_id=chat_id):
            async with self._global_gate.acquire():
                result = await asyncio.wait_for(
                    self._research_runner(
                        request=request_text,
                        symbol=symbol,
                        period=period,
                        interval=interval,
                        news_limit=(8 if news_window_days <= 7 else 20) if need_news else 0,
                        need_chart=need_chart,
                    ),
                    timeout=self._analysis_snapshot_timeout_seconds,
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
        news_count, news_window, news_source = self._extract_news(result, default_days=news_window_days)
        has_technical = latest_close is not None and bool(data_window) and (rsi is not None or ma is not None)
        has_range = (high is not None and low is not None) or (ret30 is not None)
        has_news = news_count >= 1
        if run_id:
            self._store.upsert_analysis_report(
                run_id=run_id,
                request_id=request_id or run_id,
                chat_id=chat_id,
                symbol=symbol,
                summary=summary,
                key_metrics=metrics,
            )
        target_request_id = request_id or run_id
        chart_state = "none"
        if target_request_id:
            self._store.upsert_request_chart_state(request_id=target_request_id, chart_state="none")
        metric_line, missing_reason = self._render_key_metrics(metrics)
        evidence_lines: list[str] = []
        if latest_close is not None:
            evidence_lines.append(f"当前价={round(latest_close, 4)}")
        if has_range and high is not None and low is not None:
            evidence_lines.append(f"区间高低={round(low, 4)}~{round(high, 4)}")
        elif has_range and ret30 is not None:
            evidence_lines.append(f"近30天涨跌={round(ret30 * 100, 2)}%")
        evidence_lines.append(f"news_count={news_count} 时间窗={news_window} 来源={news_source}")
        level_hint = ""
        if latest_close is not None and high is not None and low is not None and high > low:
            ratio = (latest_close - low) / (high - low)
            if ratio >= 0.8:
                level_hint = "当前价接近上沿"
            elif ratio <= 0.2:
                level_hint = "当前价接近下沿"
            else:
                level_hint = "当前价位于区间中部"
            evidence_lines.append(f"近30天Low/High={round(low,4)}/{round(high,4)} {level_hint}")
        if not evidence_lines:
            evidence_lines.append("图表状态=未生成")
        evidence_visible = bool(evidence_lines)
        self._store.record_metric(metric_name="evidence_visible_total", metric_value=1.0 if evidence_visible else 0.0)

        price_summary = metric_line if metric_line and not missing_reason else "缺价格数据"
        technical_line = "技术一句话: 证据不足，暂不输出技术结论。"
        if has_technical:
            if rsi is not None:
                technical_line = f"技术一句话: RSI14={round(rsi, 2)}，结合价格看中性偏观察。"
            elif ma is not None:
                technical_line = f"技术一句话: MA20={round(ma, 4)}，价格围绕均线附近波动。"
        news_line = f"新闻一句话: 近{news_window_days}天抓取 {news_count} 条，来源={news_source}。"
        if not has_news:
            news_line = f"新闻一句话: 近{news_window_days}天暂无可用新闻，建议扩窗或重试。"
        risk_line = "风险: 仅供研究参考，不构成投资建议。"

        menu_buttons = self._snapshot_buttons(request_id=target_request_id, chart_state=chart_state)
        report_text = (
            f"标的区间: {symbol} | {data_window}\n"
            f"价格摘要: {price_summary}\n"
            f"{technical_line}\n"
            f"{news_line}\n"
            f"{risk_line}\n"
            f"证据: {' | '.join(evidence_lines[:3])}\n"
            "类型: Snapshot Analysis\n"
            f"request_id(short)={(target_request_id or 'n/a')[-6:]}\n"
            f"run_id={run_id or 'N/A'}\n"
            f"菜单: {self._chart_state_label(chart_state)} / 📰新闻(7天|30天) / 🔁重试 / 📄报告 / 🗓️近3个月 / 📰只看新闻 / 🔔设置监控"
        )

        await self._send_text(chat_id=chat_id, text=report_text, buttons=menu_buttons)
        self._store.record_metric(metric_name="analysis_response_total", metric_value=1.0)
        if missing_reason:
            self._store.record_metric(metric_name="analysis_explainable_total", metric_value=1.0)
        elif metric_line:
            self._store.record_metric(metric_name="analysis_explainable_total", metric_value=1.0)

        if need_chart:
            chart_state = "rendering"
            if target_request_id:
                self._store.upsert_request_chart_state(request_id=target_request_id, chart_state=chart_state)
            await self._send_text(
                chat_id=chat_id,
                text=(
                    f"图表生成中，请稍候。request_id(short)={(target_request_id or 'n/a')[-6:]}\n"
                    "证据: 图表状态=生成中"
                ),
                buttons=self._snapshot_buttons(request_id=target_request_id, chart_state=chart_state),
            )
            chart_candidate = self._chart_service.extract_chart_path(result)
            chart_path, chart_size, chart_error = self._chart_service.ensure_chart_within_limit(chart_candidate)
            if chart_size is not None:
                self._store.record_metric(metric_name="chart_payload_bytes", metric_value=float(chart_size))
            chart_reason = self._resolve_chart_failure_reason(latest_close=latest_close, chart_error=chart_error)
            if chart_path is None and chart_reason == "artifact_missing":
                self._store.record_metric(metric_name="chart_retry_attempted", metric_value=1.0)
                await self._send_text(chat_id=chat_id, text="首次未取到图表产物，已自动重试一次。\n证据: 图表状态=重试中")
                async with self._global_gate.acquire():
                    retry_result = await asyncio.wait_for(
                        self._research_runner(
                            request=request_text,
                            symbol=symbol,
                            period=period,
                            interval=interval,
                            news_limit=(8 if news_window_days <= 7 else 20) if need_news else 0,
                            need_chart=need_chart,
                        ),
                        timeout=self._analysis_snapshot_timeout_seconds,
                    )
                retry_candidate = self._chart_service.extract_chart_path(retry_result)
                retry_path, retry_size, retry_error = self._chart_service.ensure_chart_within_limit(retry_candidate)
                if retry_size is not None:
                    self._store.record_metric(metric_name="chart_payload_bytes", metric_value=float(retry_size))
                if retry_path is not None:
                    chart_path = retry_path
                    chart_error = None
                    chart_state = "ready"
                    if target_request_id:
                        self._store.upsert_request_chart_state(request_id=target_request_id, chart_state=chart_state)
                    self._store.record_metric(metric_name="chart_retry_success", metric_value=1.0)
                else:
                    chart_error = retry_error
            if chart_path is None:
                reason = self._resolve_chart_failure_reason(latest_close=latest_close, chart_error=chart_error)
                chart_state = "failed"
                if target_request_id:
                    self._store.upsert_request_chart_state(request_id=target_request_id, chart_state=chart_state)
                self._store.record_metric(metric_name="chart_render_fail_rate", metric_value=1.0, tags={"reason": reason})
                await self._send_text(
                    chat_id=chat_id,
                    text=(
                        "这次没能生成价格图表，我可以重试图表或只做新闻解读。\n"
                        f"原因: {self._chart_reason_text(reason)}\n"
                        f"证据: 图表状态=失败 | 时间窗={data_window}\n"
                        f"可选: {self._chart_state_label(chart_state)} 📰新闻(7天|30天) 🔁重试 📄报告"
                    ),
                    buttons=self._snapshot_buttons(request_id=target_request_id, chart_state=chart_state),
                )
            else:
                try:
                    dispatch = await asyncio.wait_for(
                        self._channel.send_photo(
                            chat_id=chat_id,
                            image_path=str(chart_path),
                            caption=f"{symbol} {period}/{interval} chart",
                        ),
                        timeout=self._photo_send_timeout_seconds,
                    )
                except TimeoutError:
                    dispatch = None
                if dispatch is not None and dispatch.delivered:
                    chart_state = "ready"
                    if target_request_id:
                        self._store.upsert_request_chart_state(request_id=target_request_id, chart_state=chart_state)
                    await self._send_text(
                        chat_id=chat_id,
                        text=(
                            "图表已生成，可继续查看新闻或报告。\n"
                            f"证据: 图表状态=已生成 | request_id(short)={(target_request_id or 'n/a')[-6:]}"
                        ),
                        buttons=self._snapshot_buttons(request_id=target_request_id, chart_state=chart_state),
                    )
                else:
                    reason = "send_photo_error"
                    chart_state = "failed"
                    if target_request_id:
                        self._store.upsert_request_chart_state(request_id=target_request_id, chart_state=chart_state)
                    self._store.record_metric(metric_name="chart_render_fail_rate", metric_value=1.0, tags={"reason": reason})
                    await self._send_text(
                        chat_id=chat_id,
                        text=(
                            "图表发送能力不可用，已降级为文本说明。\n"
                            f"原因: {self._chart_reason_text(reason)}\n"
                            "证据: 图表状态=失败"
                        ),
                        buttons=self._snapshot_buttons(request_id=target_request_id, chart_state=chart_state),
                    )
            if not has_news:
                await self._send_text(
                    chat_id=chat_id,
                    text=(
                        f"新闻回显: news_count=0 时间窗={news_window} 来源=aggregated_news\n"
                        "近7天未抓到新闻，可选：扩到30天 / 重试。"
                    ),
                    buttons=self._snapshot_buttons(request_id=target_request_id, chart_state=chart_state),
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
            await self._send_text(chat_id=chat_id, text="无权限 (Permission denied): 当前 chat 不允许创建监控任务。")
            return ActionResult(command="monitor")

        active_jobs = self._store.count_active_watch_jobs(chat_id=chat_id)
        if active_jobs >= self._limits.max_watch_jobs_per_chat:
            await self._send_text(chat_id=chat_id, text=f"配额超限 (Quota exceeded): 单 chat 最大监控任务数为 {self._limits.max_watch_jobs_per_chat}。")
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
        await self._send_text(
            chat_id=chat_id,
            text=(
            f"监控已创建 (Monitor created): scope={scope} symbols={','.join(symbol_list)} every {job.interval_sec}s template={template} "
            f"mode={job.mode} threshold={job.threshold} route={route_strategy} "
            f"job_id={job.job_id} next_run_at={job.next_run_at}"
            ),
        )
        return ActionResult(command="monitor")

    async def handle_report(self, *, chat_id: str, target_id: str, detail: str = "short") -> ActionResult:
        self._store.record_metric(metric_name="report_lookup_total", metric_value=1.0)
        report = self._store.get_analysis_report(report_id=target_id, chat_id=chat_id)
        if report is None:
            await self._send_text(chat_id=chat_id, text=f"未找到报告 (No report found) `{target_id}`。")
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
            data_window = str(report.key_metrics.get("data_window") or "unknown")
            fallback_reason = str(report.key_metrics.get("fallback_reason") or "").strip()
            fallback_line = f"\n- fallback_reason={fallback_reason}" if fallback_reason else ""
            evidence_suffix = (
                "\n证据块 (Evidence):\n"
                f"- data_window={data_window}\n"
                f"- source=request_id:{report.request_id}\n"
                f"- metric_source_keys={','.join(metric_source_keys)}\n"
                f"- confidence_label={confidence}\n"
                f"- schema_version={latest.get('schema_version', 'unknown')}\n"
                f"- action_version={latest.get('action_version', 'unknown')}"
                f"{fallback_line}"
            )
        await self._send_text(
            chat_id=chat_id,
            text=(
            f"报告摘要 (Report summary)\nrun_id={report.run_id}\nrequest_id={report.request_id}\n"
            f"symbol={report.symbol}\nsummary={summary}{metrics_suffix}{evidence_suffix}{help_suffix}"
            ),
        )
        self._store.record_metric(metric_name="report_lookup_success", metric_value=1.0)
        return ActionResult(command="report")

    async def handle_digest(self, *, chat_id: str, period: str) -> ActionResult:
        digest = self._store.build_daily_digest(chat_id=chat_id) if period == "daily" else {}
        if not digest:
            await self._send_text(chat_id=chat_id, text="摘要周期不支持 (Digest period is not supported).")
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
        await self._send_text(chat_id=chat_id, text="\n".join(lines))
        self._store.record_metric(metric_name="digest_generated", metric_value=1.0)
        return ActionResult(command="digest")

    async def handle_list(self, *, chat_id: str) -> ActionResult:
        jobs = self._store.list_watch_jobs(chat_id=chat_id, include_disabled=False)
        if not jobs:
            await self._send_text(chat_id=chat_id, text="当前无活跃监控任务 (No active monitor jobs)。可用 /monitor <symbol> <interval> 创建。")
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
        await self._send_text(chat_id=chat_id, text="\n".join(lines))
        return ActionResult(command="list")

    async def handle_alerts(self, *, chat_id: str, view: str, limit: int) -> ActionResult:
        rows = self._store.list_alert_hub(chat_id=chat_id, view=view, limit=limit)
        if not rows:
            await self._send_text(chat_id=chat_id, text=f"视图 {view} 暂无告警记录 (No alert records).")
            return ActionResult(command="alerts")
        lines = [f"告警中心 (Alert Hub) view={view} (latest {limit})"]
        for row in rows:
            extra = ""
            if row.suppressed_reason:
                extra = f" suppressed={row.suppressed_reason}"
            elif row.last_error:
                extra = f" error={row.last_error[:80]}"
            lines.append(f"- {row.event_id} {row.symbol} {row.priority} {row.channel}:{row.status}{extra}")
        await self._send_text(chat_id=chat_id, text="\n".join(lines))
        return ActionResult(command="alerts")

    async def handle_bulk(self, *, chat_id: str, action: str, target: str, value: str = "") -> ActionResult:
        changed = self._store.bulk_update_watch_jobs(chat_id=chat_id, action=action, target=target, value=value)
        await self._send_text(chat_id=chat_id, text=f"批量更新完成 (Bulk update done): action={action} target={target} changed={changed}")
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
            await self._send_text(chat_id=chat_id, text=f"Webhook 已启用 (Webhook enabled): {hook.webhook_id} -> {hook.url}")
            return ActionResult(command="webhook")
        if action == "disable":
            ok = self._store.disable_outbound_webhook(chat_id=chat_id, webhook_id=webhook_id)
            await self._send_text(chat_id=chat_id, text="Webhook 已禁用 (Webhook disabled)." if ok else "未找到 Webhook (Webhook not found).")
            return ActionResult(command="webhook")
        hooks = self._store.list_outbound_webhooks(chat_id=chat_id, enabled_only=False)
        if not hooks:
            await self._send_text(chat_id=chat_id, text="未配置 Webhook (No webhook configured).")
            return ActionResult(command="webhook")
        lines = ["Webhook 路由 (Webhook routes):"]
        for hook in hooks:
            lines.append(f"- {hook.webhook_id} enabled={hook.enabled} timeout_ms={hook.timeout_ms} url={hook.url}")
        await self._send_text(chat_id=chat_id, text="\n".join(lines))
        return ActionResult(command="webhook")

    async def handle_pref(self, *, chat_id: str, setting: str, value: str) -> ActionResult:
        if setting == "summary":
            pref = self._store.upsert_chat_preferences(chat_id=chat_id, summary_mode=value)
        elif setting == "priority":
            pref = self._store.upsert_chat_preferences(chat_id=chat_id, min_priority=value)
        elif setting == "quiet":
            pref = self._store.upsert_chat_preferences(chat_id=chat_id, quiet_hours=None if value == "off" else value)
        else:
            await self._send_text(chat_id=chat_id, text="不支持的偏好设置 (Unsupported preference setting).")
            return ActionResult(command="pref")
        await self._send_text(chat_id=chat_id, text=f"偏好已更新 (Preference updated): summary={pref.summary_mode} priority={pref.min_priority} quiet={pref.quiet_hours or 'off'}")
        return ActionResult(command="pref")

    async def handle_stop(self, *, chat_id: str, target: str, target_type: str) -> ActionResult:
        if not self._store.can_chat_monitor(chat_id=chat_id):
            await self._send_text(chat_id=chat_id, text="无权限 (Permission denied): 当前 chat 不允许停止监控任务。")
            return ActionResult(command="stop")

        disabled = self._store.disable_watch_job(chat_id=chat_id, target=target, target_type=target_type)
        if disabled <= 0:
            await self._send_text(chat_id=chat_id, text=f"未匹配到活跃监控任务 (No active monitor job matched): {target}")
            return ActionResult(command="stop")

        await self._send_text(chat_id=chat_id, text=f"已停止监控任务 (Stopped) {disabled} 个，target={target}")
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
                timeout_seconds=self._analysis_recovery_timeout_seconds,
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
            async with self._typing_heartbeat(chat_id=chat_id):
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
            await self._send_text(chat_id=chat_id, text=f"分析完成 (Analysis completed). request_id={request_id}, run_id={run_id}")
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
            await self._send_text(chat_id=chat_id, text=f"分析超时 (Analysis timeout). request_id={request_id}. 结果将后台重试 (retried in background).")
        except Exception as exc:
            self._store.transition_analysis_request_status(
                request_id=request_id,
                from_statuses=("running",),
                to_status="failed",
                run_id=None,
                last_error=str(exc),
            )
            await self._send_text(chat_id=chat_id, text=f"分析失败 (Analysis failed). request_id={request_id}, error={exc}")
        finally:
            latency_ms = (time.perf_counter() - start) * 1000
            self._store.record_metric(metric_name="analysis_latency_ms", metric_value=latency_ms, tags={"chat_id": chat_id})
