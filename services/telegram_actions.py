from __future__ import annotations

import asyncio
import contextlib
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Protocol

from agents.workflow_engine import run_unified_research
from core.strategy_tier import DEFAULT_STRATEGY_TIER, normalize_strategy_tier
from services.news_digest import (
    NewsDigest,
    ThemeDigestItem,
    TopNewsItem,
    build_news_digest_from_result,
    format_cluster_lines,
    format_top_news_lines,
    redact_user_visible_payload,
)
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
        "traceback",
        "run_id",
        "request_id",
        "schema_version",
        "action_version",
        "raw_error",
        "_plan_steps",
        "_schema_version",
        "chart_missing",
        "metrics unavailable",
    )
    _FORBIDDEN_USER_KEYS = {
        "schema_version",
        "_schema_version",
        "action_version",
        "traceback",
        "raw_error",
    }
    _FINAL_SCHEMA_VERSION = "telegram_snapshot_v3"
    _MAIN_MESSAGE_CHAR_LIMIT = 1180
    _SUPPLEMENT_MESSAGE_CHAR_LIMIT = 420
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
    ) -> dict[str, Any] | None:
        safe_text = self._sanitize_user_copy(text)
        blocked = safe_text != text
        if blocked:
            self._store.record_metric(metric_name="user_copy_guard_hit_total", metric_value=1.0)
        reply_markup = self._build_inline_keyboard(buttons) if buttons else None
        dispatch = await self._channel.send_text(chat_id=chat_id, text=safe_text, reply_markup=reply_markup)
        if not dispatch.delivered:
            raise RuntimeError(f"telegram_send_text_failed:{dispatch.error or 'unknown_error'}")
        return dispatch.payload if isinstance(dispatch.payload, dict) else None

    @classmethod
    def _sanitize_user_copy(cls, text: str) -> str:
        sanitized = str(text or "")
        for token in cls._FORBIDDEN_USER_COPY:
            sanitized = re.sub(re.escape(token), "内部细节", sanitized, flags=re.IGNORECASE)
        sanitized = re.sub(
            r"\b(action|internal_[a-z0-9_]+)\s*[:=]\s*[\w.\-]+",
            "内部细节",
            sanitized,
            flags=re.IGNORECASE,
        )
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
    def _extract_message_id(payload: dict[str, Any] | None) -> int | None:
        if not isinstance(payload, dict):
            return None
        result = payload.get("result")
        if isinstance(result, dict):
            value = result.get("message_id")
        else:
            value = payload.get("message_id")
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

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

    @staticmethod
    def _summary_sentence(summary: str) -> str:
        text = str(summary or "").replace("\n", " ").strip()
        if not text:
            return "暂无可执行结论，建议先查看价格与新闻证据。"
        for sep in ("。", ".", "！", "!", "？", "?"):
            idx = text.find(sep)
            if idx != -1:
                clipped = text[: idx + 1].strip()
                if clipped:
                    return clipped[:120]
        return text[:120]

    @staticmethod
    def _clip_line(text: str, *, limit: int) -> str:
        value = str(text or "").strip()
        if len(value) <= limit:
            return value
        return f"{value[: max(1, limit - 1)]}…"

    @staticmethod
    def _format_signed_pct(value: float | None) -> str:
        if value is None:
            return "N/A"
        return f"{value * 100:+.2f}%"

    @staticmethod
    def _format_signed_price(value: float | None) -> str:
        if value is None:
            return "N/A"
        return f"{value:+.4f}"

    @staticmethod
    def _format_timestamp(value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            return "未知时间"
        candidate = normalized.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(candidate)
            return dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            return normalized[:16]

    @staticmethod
    def _position_label(ratio: float | None) -> str:
        if ratio is None:
            return "数据不足"
        if ratio >= 0.8:
            return "高位"
        if ratio <= 0.2:
            return "低位"
        return "中位"

    @staticmethod
    def _technical_sentence(*, latest_close: float | None, ma: float | None, rsi: float | None) -> str:
        if rsi is not None and rsi >= 65:
            return "短线偏强，若放量站稳前高可延续上行；若量能不足则回撤风险上升。"
        if rsi is not None and rsi <= 35:
            return "短线偏弱，若收复10日均线并放量可转中性；若失守近期低点则下行风险上升。"
        if ma is not None and latest_close is not None:
            if latest_close >= ma:
                return "短线中性偏强，若继续站稳20日均线可确认转强；若跌回均线下方则回落风险增加。"
            return "短线中性偏弱，若重回20日均线上方可改善；若持续受压则震荡下行风险偏高。"
        return "短线中性，若放量突破区间上沿可转强；若跌破区间下沿则转弱风险抬升。"

    @staticmethod
    def _news_theme_lines(news_digest: NewsDigest) -> list[str]:
        if news_digest.total_count <= 0:
            return [f"主题覆盖不足（{news_digest.window_label}），可能遗漏重大事件。"]
        if not news_digest.top_themes:
            return [f"主题覆盖不足（{news_digest.window_label}），可能遗漏重大事件。"]
        lines = [f"主题Top3：{news_digest.window_label} 共 {news_digest.total_count} 条，整体情绪{news_digest.sentiment_direction}。"]
        for index, item in enumerate(news_digest.top_themes[:3], start=1):
            lines.append(f"{index}) {item.category}：{item.impact}")
            lines.append(
                f"   代表新闻：{item.representative_title}（{item.representative_time}｜来源：{item.representative_source}）"
            )
        return lines

    @classmethod
    def _final_schema_version_for_snapshot(
        cls,
        *,
        period: str,
        news_window_days: int,
        need_chart: bool,
    ) -> str:
        return (
            f"{cls._FINAL_SCHEMA_VERSION}|period={str(period).lower()}|news_window={int(news_window_days)}|"
            f"need_chart={1 if need_chart else 0}"
        )

    @classmethod
    def _build_analysis_contract(
        cls,
        *,
        symbol: str,
        period: str,
        summary: str,
        latest_close: float | None,
        high: float | None,
        low: float | None,
        ret30: float | None,
        max_drawdown: float | None,
        rsi: float | None,
        ma: float | None,
        market_source: str,
        market_updated_at: str,
        indicator_spec: str,
        chart_note: str | None,
        news_digest: NewsDigest,
    ) -> tuple[str, str | None]:
        conclusion = cls._summary_sentence(summary)
        rise_fall_amount: float | None = None
        if latest_close is not None and ret30 is not None and abs(1.0 + ret30) > 1e-9:
            base_price = latest_close / (1.0 + ret30)
            rise_fall_amount = latest_close - base_price

        amplitude: float | None = None
        ratio: float | None = None
        if latest_close is not None and high is not None and low is not None and high > low:
            ratio = (latest_close - low) / (high - low)
            if low > 0:
                amplitude = (high - low) / low

        if max_drawdown is None and high is not None and low is not None and high > 0:
            max_drawdown = (low - high) / high

        source_line = "、".join(news_digest.source_coverage[:3]) if news_digest.source_coverage else "无"
        main_lines = [
            f"{symbol}｜近30天（{cls._period_window(period)})",
            "",
            "Card A｜区间表现",
            f"近30天：{cls._format_signed_pct(ret30)}（涨跌额 {cls._format_signed_price(rise_fall_amount)}）",
            (
                f"区间：低 {round(low, 4)} / 高 {round(high, 4)}（振幅 "
                f"{(amplitude * 100):.2f}%）｜最大回撤 {cls._format_signed_pct(max_drawdown)}"
                if low is not None and high is not None and amplitude is not None
                else f"区间：低 {round(low, 4) if low is not None else 'N/A'} / 高 {round(high, 4) if high is not None else 'N/A'}"
            ),
            f"现价区间位置：{cls._position_label(ratio)}",
            "",
            "Card B｜原因摘要",
            f"技术：{cls._technical_sentence(latest_close=latest_close, ma=ma, rsi=rsi)}",
            "新闻：",
            *cls._news_theme_lines(news_digest),
            "",
            "Card C｜证据三件套",
            f"行情源={market_source}（更新至 {cls._format_timestamp(market_updated_at)}）",
            f"新闻源覆盖={source_line}（窗口={news_digest.window_label}）",
            f"指标口径={indicator_spec}",
            *(["图表说明=" + chart_note] if chart_note else []),
            "",
            "Card D｜动作入口",
            "按钮：📈K线｜📰新闻｜🔔提醒｜更多",
            f"类型: Snapshot Analysis｜结论: {conclusion}",
        ]
        main_text = cls._clip_line("\n".join(main_lines), limit=cls._MAIN_MESSAGE_CHAR_LIMIT)
        return main_text, None

    def _snapshot_buttons(
        self,
        *,
        request_id: str | None,
        chart_state: str,
    ) -> list[list[tuple[str, str]]]:
        if not request_id:
            return []
        ref = request_id[-6:]
        return [[
            (self._chart_state_label(chart_state), f"act|{ref}|chart"),
            ("📰新闻", f"act|{ref}|news"),
            ("🔔提醒", f"act|{ref}|alert"),
            ("更多", f"act|{ref}|more"),
        ]]

    @staticmethod
    def _snapshot_news_buttons(request_id: str | None) -> list[list[tuple[str, str]]]:
        if not request_id:
            return []
        ref = request_id[-6:]
        return [
            [("📰新闻(7天)", f"act|{ref}|news7"), ("📰新闻(30天)", f"act|{ref}|news30")],
            [("📰新闻详单", f"act|{ref}|news_detail"), ("🧩事件聚类", f"act|{ref}|news_cluster")],
            [("返回主菜单", f"act|{ref}|home")],
        ]

    @staticmethod
    def _snapshot_more_buttons(request_id: str | None) -> list[list[tuple[str, str]]]:
        if not request_id:
            return []
        ref = request_id[-6:]
        return [
            [("🔁重试", f"act|{ref}|retry"), ("📄报告", f"act|{ref}|report")],
            [("📰新闻详单", f"act|{ref}|news_detail"), ("🧩事件聚类", f"act|{ref}|news_cluster")],
            [("🗓️近3个月", f"act|{ref}|period3mo"), ("📰只看新闻", f"act|{ref}|news_only")],
            [("❓为什么不给K线", f"act|{ref}|why_no_chart"), ("❓为什么不给RSI", f"act|{ref}|why_no_rsi")],
            [("返回主菜单", f"act|{ref}|home")],
        ]

    def build_snapshot_buttons(
        self,
        *,
        request_id: str | None,
        chart_state: str,
    ) -> list[list[tuple[str, str]]]:
        return self._snapshot_buttons(request_id=request_id, chart_state=chart_state)

    def build_snapshot_news_buttons(self, *, request_id: str | None) -> list[list[tuple[str, str]]]:
        return self._snapshot_news_buttons(request_id=request_id)

    def build_snapshot_more_buttons(self, *, request_id: str | None) -> list[list[tuple[str, str]]]:
        return self._snapshot_more_buttons(request_id=request_id)

    @classmethod
    def _clean_key_metrics(cls, key_metrics: dict[str, Any]) -> dict[str, Any]:
        cleaned = redact_user_visible_payload(key_metrics)
        if not isinstance(cleaned, dict):
            return {}
        return cleaned

    @staticmethod
    def _news_digest_from_metrics(key_metrics: dict[str, Any]) -> NewsDigest:
        raw = key_metrics.get("news_digest")
        if not isinstance(raw, dict):
            return NewsDigest(
                window_days=7,
                window_label="近7天",
                total_count=0,
                source_coverage=[],
                event_distribution={"财报": 0, "监管": 0, "产品": 0, "宏观": 0, "其他": 0},
                sentiment_score=50,
                sentiment_direction="中性",
                sentiment_range="45-55",
                top_themes=[],
                top_news=[],
            )
        event_distribution_raw = raw.get("event_distribution")
        if not isinstance(event_distribution_raw, dict):
            event_distribution_raw = {}
        source_raw = raw.get("source_coverage")
        source_coverage = source_raw if isinstance(source_raw, list) else []
        top_themes: list[ThemeDigestItem] = []
        for item in raw.get("top_themes", []):
            if not isinstance(item, dict):
                continue
            top_themes.append(
                ThemeDigestItem(
                    category=str(item.get("category", "")).strip(),
                    count=max(0, int(item.get("count", 0))),
                    impact=str(item.get("impact", "")).strip(),
                    representative_title=str(item.get("representative_title", "")).strip(),
                    representative_time=str(item.get("representative_time", "")).strip(),
                    representative_source=str(item.get("representative_source", "")).strip(),
                )
            )
        top_news: list[TopNewsItem] = []
        for item in raw.get("top_news", []):
            if not isinstance(item, dict):
                continue
            top_news.append(
                TopNewsItem(
                    title=str(item.get("title", "")).strip(),
                    published_at=str(item.get("published_at", "")).strip(),
                    source=str(item.get("source", "")).strip(),
                    impact=str(item.get("impact", "")).strip(),
                    category=str(item.get("category", "")).strip(),
                    sentiment=str(item.get("sentiment", "")).strip(),
                )
            )
        return NewsDigest(
            window_days=max(1, int(raw.get("window_days", 7))),
            window_label=str(raw.get("window_label", "近7天")).strip() or "近7天",
            total_count=max(0, int(raw.get("total_count", 0))),
            source_coverage=[str(item).strip() for item in source_coverage if str(item).strip()],
            event_distribution={
                "财报": int(event_distribution_raw.get("财报", 0)),
                "监管": int(event_distribution_raw.get("监管", 0)),
                "产品": int(event_distribution_raw.get("产品", 0)),
                "宏观": int(event_distribution_raw.get("宏观", 0)),
                "其他": int(event_distribution_raw.get("其他", 0)),
            },
            sentiment_score=max(0, min(100, int(raw.get("sentiment_score", 50)))),
            sentiment_direction=str(raw.get("sentiment_direction", "中性")).strip() or "中性",
            sentiment_range=str(raw.get("sentiment_range", "45-55")).strip() or "45-55",
            top_themes=top_themes[:3],
            top_news=top_news[:5],
        )

    @staticmethod
    def _extract_news(result: dict[str, Any], *, default_days: int = 7) -> tuple[int, str, str]:
        digest = build_news_digest_from_result(result, window_days=default_days)
        sources = digest.source_coverage[:2]
        source_text = ",".join(sources) if sources else "aggregated_news"
        return digest.total_count, digest.window_label, source_text

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
        header = "你好，我是 Alpha-Insight 投研助手（7x24 实时扫描）。"
        if intent == "capability":
            header = "我可以帮你做这些事情："
        elif intent == "how_to_start":
            header = "从这里开始最简单："
        elif intent == "help":
            header = "你可以这样使用我："
        return (
            f"{header}\n"
            "能力卡\n"
            "1) 快速分析：输入“分析 TSLA 一个月走势”\n"
            "2) 7x24 监控提醒：输入“帮我盯 TSLA 每小时”\n"
            "3) 查询任务：输入“看看我的监控列表”\n"
            "4) 运行状态：输入“/status”查看告警与降级状态\n"
            "示例提问：\n"
            "- 分析 0700.HK 近30天并给我K线\n"
            "- 给我 TSLA 最近7天新闻\n"
            "- 帮我设置 AAPL 每日波动提醒\n"
            "- 看看系统现在状态"
        )

    @staticmethod
    def _capability_buttons() -> list[list[tuple[str, str]]]:
        return [
            [("📈 快速分析", "guide|analyze"), ("🔔 创建监控", "guide|monitor")],
            [("📊 运行状态", "guide|status"), ("📋 查看命令", "guide|help")],
            [("🧭 怎么开始", "guide|start"), ("🆕 新对话", "guide|new")],
        ]

    async def handle_general_conversation(self, *, chat_id: str, intent: str) -> ActionResult:
        await self._send_text(
            chat_id=chat_id,
            text=self._capability_card_text(intent=intent),
            buttons=self._capability_buttons(),
        )
        return ActionResult(command=f"conversation_{intent}")

    async def send_analysis_ack(self, *, chat_id: str, request_id: str) -> None:
        if not self._limits.send_progress_updates:
            await self._send_text(
                chat_id=chat_id,
                text="已受理请求，开始分析。",
            )
            return
        progress_state = self._store.get_request_progress_message(request_id=request_id)
        progress_message_id = progress_state.message_id if progress_state is not None else None
        dispatch = await self._channel.send_progress(
            chat_id=chat_id,
            text="已受理请求，开始分析。",
            message_id=progress_message_id,
            reply_markup=None,
        )
        if not dispatch.delivered:
            raise RuntimeError(f"telegram_send_progress_failed:{dispatch.error or 'unknown_error'}")
        resolved_message_id = progress_message_id
        if resolved_message_id is None:
            resolved_message_id = self._extract_message_id(dispatch.payload if isinstance(dispatch.payload, dict) else None)
        self._store.upsert_request_progress_message(
            request_id=request_id,
            chat_id=chat_id,
            message_id=resolved_message_id,
            last_stage="ack",
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
        progress_state = self._store.get_request_progress_message(request_id=request_id)
        progress_message_id = progress_state.message_id if progress_state is not None else None
        dispatch = await self._channel.send_progress(
            chat_id=chat_id,
            text=stage_text,
            message_id=progress_message_id,
            reply_markup=None,
        )
        if not dispatch.delivered:
            raise RuntimeError(f"telegram_send_progress_failed:{dispatch.error or 'unknown_error'}")
        resolved_message_id = progress_message_id
        if resolved_message_id is None:
            resolved_message_id = self._extract_message_id(dispatch.payload if isinstance(dispatch.payload, dict) else None)
        self._store.upsert_request_progress_message(
            request_id=request_id,
            chat_id=chat_id,
            message_id=resolved_message_id,
            last_stage=stage,
        )

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
            "可用命令：\n"
            "/start - 显示能力卡与快速入口\n"
            "/new - 开启新对话并清空上下文\n"
            "/status - 查看监控/告警/降级运行状态\n"
            "/analyze <symbol> - 运行统一研究\n"
            "/monitor <symbol> <interval> [volatility|price|rsi] - 创建监控任务（兼容旧格式）\n"
            "/monitor <symbol|sym1,sym2> <interval> [volatility|price|rsi] "
            "[telegram_only|webhook_only|dual_channel|email_only|wecom_only|multi_channel] "
            "[research-only|alert-only|execution-ready]\n"
            "/list - 查看监控任务列表\n"
            "/stop - 取消当前分析任务\n"
            "/stop <job_id|symbol> - 停止监控任务\n"
            "/report <run_id|request_id> [short|full] - 查询报告摘要\n"
            "/digest daily - 获取近24小时摘要\n"
            "/alerts [triggered|failed|suppressed] [limit] - 告警中心视图\n"
            "/bulk <enable|disable|interval|threshold> <target|all> [value] - 批量任务操作\n"
            "/webhook <set|disable|list> ... - 管理 webhook 路由\n"
            "/route <set|disable|list> ... - 管理 telegram/email/wecom 路由\n"
            "/pref <summary|quiet|priority> <value> - 通知偏好设置\n"
            "合规提示：仅用于研究与提醒，不支持自动交易。\n"
            "/help - 显示帮助"
            ),
        )
        return ActionResult(command="help")

    async def handle_status(self, *, chat_id: str) -> ActionResult:
        active_jobs = self._store.count_active_watch_jobs(chat_id=chat_id)
        digest = self._store.build_daily_digest(chat_id=chat_id)
        triggered = int(digest.get("alerts_triggered", 0))
        delivered = int(digest.get("delivered_notifications", 0))
        retry_depth = int(self._store.count_retry_queue_depth())
        chart_degrade = "是" if self._store.is_degradation_active(state_key="chart_text_only") else "否"
        nl_degrade = "是" if self._store.is_degradation_active(state_key="nl_command_hint_mode") else "否"
        monitor_degrade = "是" if self._store.is_degradation_active(state_key="no_monitor_push") else "否"
        text = (
            "运行状态（7x24）\n"
            f"- 活跃监控任务：{active_jobs}\n"
            f"- 近24h触发告警：{triggered}\n"
            f"- 近24h已投递通知：{delivered}\n"
            f"- 重试队列深度：{retry_depth}\n"
            f"- 图表降级中：{chart_degrade}\n"
            f"- NL兜底模式：{nl_degrade}\n"
            f"- 监控推送降级中：{monitor_degrade}\n"
            "可用：/monitor 创建监控，/alerts 查看告警，/report 查看报告。"
        )
        await self._send_text(chat_id=chat_id, text=text)
        return ActionResult(command="status")

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

        await self._send_text(chat_id=chat_id, text="请求已受理，正在分析。")
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
        cancel_event: asyncio.Event | None = None,
    ) -> ActionResult:
        request_text = (
            f"Analyze snapshot for {symbol} period={period} interval={interval} "
            f"need_news={str(bool(need_news)).lower()}"
        )
        async with self._typing_heartbeat(chat_id=chat_id):
            async with self._global_gate.acquire():
                result = await self._run_research_with_cancel(
                    cancel_event=cancel_event,
                    timeout_seconds=self._analysis_snapshot_timeout_seconds,
                    request=request_text,
                    symbol=symbol,
                    period=period,
                    interval=interval,
                    news_limit=(8 if news_window_days <= 7 else 20) if need_news else 0,
                    need_chart=need_chart,
                )

        run_id = str(result.get("run_id", "")).strip()
        summary = str(((result.get("fused_insights") or {}).get("summary") or "")).strip() or "暂无摘要。"
        metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
        latest_close = self._metric_float(metrics, "latest_close", "data_close")
        rsi = self._metric_float(metrics, "technical_rsi_14", "rsi14")
        ma = self._metric_float(metrics, "technical_ma_20", "ma20")
        high = self._metric_float(metrics, "window_high", "period_high")
        low = self._metric_float(metrics, "window_low", "period_low")
        ret30 = self._metric_float(metrics, "return_30d", "change_30d")
        max_drawdown = self._metric_float(metrics, "max_drawdown_30d", "max_drawdown")
        news_digest = build_news_digest_from_result(result, window_days=news_window_days)
        report_metrics = self._clean_key_metrics(metrics)
        report_metrics.update(
            {
                "news_digest": news_digest.to_dict(),
                "news_total_count": news_digest.total_count,
                "news_window_label": news_digest.window_label,
                "news_sentiment_direction": news_digest.sentiment_direction,
                "news_sentiment_range": news_digest.sentiment_range,
            }
        )
        target_request_id = request_id or run_id
        if run_id and target_request_id:
            self._store.upsert_analysis_report(
                run_id=run_id,
                request_id=target_request_id,
                chat_id=chat_id,
                symbol=symbol,
                summary=summary,
                key_metrics=report_metrics,
            )

        chart_state = "none"
        chart_path = None
        chart_error = None
        chart_note: str | None = None
        if target_request_id:
            self._store.upsert_request_chart_state(request_id=target_request_id, chart_state="none")

        if need_chart:
            chart_state = "rendering"
            if target_request_id:
                self._store.upsert_request_chart_state(request_id=target_request_id, chart_state=chart_state)
            chart_candidate = self._chart_service.extract_chart_path(result)
            chart_path, chart_size, chart_error = self._chart_service.ensure_chart_within_limit(chart_candidate)
            if chart_size is not None:
                self._store.record_metric(metric_name="chart_payload_bytes", metric_value=float(chart_size))
            chart_reason = self._resolve_chart_failure_reason(latest_close=latest_close, chart_error=chart_error)
            if chart_path is None and chart_reason == "artifact_missing":
                self._store.record_metric(metric_name="chart_retry_attempted", metric_value=1.0)
                async with self._global_gate.acquire():
                    retry_result = await self._run_research_with_cancel(
                        cancel_event=cancel_event,
                        timeout_seconds=self._analysis_snapshot_timeout_seconds,
                        request=request_text,
                        symbol=symbol,
                        period=period,
                        interval=interval,
                        news_limit=(8 if news_window_days <= 7 else 20) if need_news else 0,
                        need_chart=need_chart,
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
                chart_state = "failed"
                chart_reason = self._resolve_chart_failure_reason(latest_close=latest_close, chart_error=chart_error)
                self._store.record_metric(metric_name="chart_render_fail_rate", metric_value=1.0, tags={"reason": chart_reason})
                chart_note = f"没能生成价格图表（{self._chart_reason_text(chart_reason)}）"
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
                    chart_note = "图表已生成，可点击 📈K线 再次查看。"
                else:
                    chart_state = "failed"
                    self._store.record_metric(metric_name="chart_render_fail_rate", metric_value=1.0, tags={"reason": "send_photo_error"})
                    chart_note = f"没能生成价格图表（{self._chart_reason_text('send_photo_error')}）"
            if target_request_id:
                self._store.upsert_request_chart_state(request_id=target_request_id, chart_state=chart_state)

        market_source = str(
            metrics.get("market_data_source")
            or metrics.get("data_source")
            or metrics.get("quote_source")
            or "aggregated_market_feed"
        ).strip()
        market_updated_at = str(
            metrics.get("market_data_updated_at")
            or metrics.get("data_updated_at")
            or metrics.get("quote_updated_at")
            or metrics.get("last_updated_at")
            or datetime.now(timezone.utc).isoformat()
        ).strip()
        indicator_spec = "RSI14(1d)、MA20(1d)"
        main_text, supplement_text = self._build_analysis_contract(
            symbol=symbol,
            period=period,
            summary=summary,
            latest_close=latest_close,
            high=high,
            low=low,
            ret30=ret30,
            max_drawdown=max_drawdown,
            rsi=rsi,
            ma=ma,
            market_source=market_source,
            market_updated_at=market_updated_at,
            indicator_spec=indicator_spec,
            chart_note=chart_note,
            news_digest=news_digest,
        )
        evidence_visible = bool(main_text)
        self._store.record_metric(metric_name="evidence_visible_total", metric_value=1.0 if evidence_visible else 0.0)
        schema_version = self._final_schema_version_for_snapshot(
            period=period,
            news_window_days=news_window_days,
            need_chart=need_chart,
        )
        if target_request_id and not self._store.claim_final_message_dispatch(
            request_id=target_request_id,
            final_schema_version=schema_version,
        ):
            self._store.record_metric(metric_name="dedupe_suppressed_count", metric_value=1.0)
            return ActionResult(command="analyze_snapshot", request_id=request_id)

        menu_buttons = self._snapshot_buttons(request_id=target_request_id, chart_state=chart_state)
        payload = await self._send_text(chat_id=chat_id, text=main_text, buttons=menu_buttons)
        if target_request_id:
            self._store.mark_final_message_dispatched(
                request_id=target_request_id,
                final_schema_version=schema_version,
                message_id=self._extract_message_id(payload),
            )
        if supplement_text:
            await self._send_text(chat_id=chat_id, text=supplement_text)
        self._store.record_metric(metric_name="analysis_response_total", metric_value=1.0)
        self._store.record_metric(metric_name="analysis_explainable_total", metric_value=1.0)

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
        strategy_tier: str = DEFAULT_STRATEGY_TIER,
    ) -> ActionResult:
        if not self._store.can_chat_monitor(chat_id=chat_id):
            await self._send_text(chat_id=chat_id, text="无权限：当前会话不允许创建监控任务。")
            return ActionResult(command="monitor")

        active_jobs = self._store.count_active_watch_jobs(chat_id=chat_id)
        if active_jobs >= self._limits.max_watch_jobs_per_chat:
            await self._send_text(chat_id=chat_id, text=f"配额超限：当前会话最多可创建 {self._limits.max_watch_jobs_per_chat} 个监控任务。")
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
            strategy_tier=normalize_strategy_tier(strategy_tier),
            template_id=template,
            interval_sec=interval_sec,
            market="auto",
            threshold=threshold,
            mode=mode,
        )
        await self._send_text(
            chat_id=chat_id,
            text=(
            f"监控已创建：范围={scope} 标的={','.join(symbol_list)} 周期={job.interval_sec}s 模板={template} "
            f"模式={job.mode} 阈值={job.threshold} 路由={route_strategy} 策略层级={job.strategy_tier} "
            f"任务ID={job.job_id} 下次执行={job.next_run_at}"
            ),
        )
        return ActionResult(command="monitor")

    async def handle_report(self, *, chat_id: str, target_id: str, detail: str = "short") -> ActionResult:
        self._store.record_metric(metric_name="report_lookup_total", metric_value=1.0)
        report = self._store.get_analysis_report(report_id=target_id, chat_id=chat_id)
        if report is None:
            await self._send_text(chat_id=chat_id, text=f"未找到报告：{target_id}")
            return ActionResult(command="report")

        safe_metrics = self._clean_key_metrics(report.key_metrics)
        metrics_line, _ = self._render_key_metrics(safe_metrics)
        summary = report.summary[:220] if detail == "short" else report.summary[:1200]
        digest = self._news_digest_from_metrics(safe_metrics)
        if detail == "short":
            message = (
                "报告摘要\n"
                f"标的：{report.symbol}\n"
                f"结论：{summary}\n"
                f"关键指标：{metrics_line or '暂无关键指标'}\n"
                f"新闻概览：{digest.window_label} 共 {digest.total_count} 条，情绪{digest.sentiment_direction}（{digest.sentiment_range}）\n"
                f"如需完整证据，请输入：/report {target_id} full"
            )
            await self._send_text(chat_id=chat_id, text=message)
            self._store.record_metric(metric_name="report_lookup_success", metric_value=1.0)
            return ActionResult(command="report")

        evidence = self._store.list_nl_execution_evidence(request_id=report.request_id, limit=10)
        evidence_count = len(evidence)
        data_window = str(safe_metrics.get("data_window") or "unknown")
        fallback_reason = str(safe_metrics.get("fallback_reason") or "").strip()
        fallback_line = f"\n- 回退原因：{fallback_reason}" if fallback_reason else ""
        source_line = "、".join(digest.source_coverage) if digest.source_coverage else "无"
        cluster_block = "\n".join(format_cluster_lines(digest))
        top5_block = "\n".join(format_top_news_lines(digest))
        full_text = (
            "报告摘要（完整版）\n"
            f"标的：{report.symbol}\n"
            f"结论：{summary}\n"
            f"关键指标：{metrics_line or '暂无关键指标'}\n"
            "\n证据块：\n"
            f"- 数据窗口：{data_window}\n"
            f"- 执行事件数：{evidence_count}"
            f"{fallback_line}\n"
            "\n新闻证据块：\n"
            f"- 时间窗：{digest.window_label}\n"
            f"- 总条数：{digest.total_count}\n"
            f"- 来源覆盖：{source_line}\n"
            f"- 情绪方向：{digest.sentiment_direction}（{digest.sentiment_range}）\n"
            "- 事件分布：\n"
            f"{cluster_block}\n"
            "- Top5 摘要：\n"
            f"{top5_block}"
        )
        await self._send_text(chat_id=chat_id, text=self._clip_line(full_text, limit=3400))
        self._store.record_metric(metric_name="report_lookup_success", metric_value=1.0)
        return ActionResult(command="report")

    async def handle_news_detail(self, *, chat_id: str, target_id: str) -> ActionResult:
        report = self._store.get_analysis_report(report_id=target_id, chat_id=chat_id)
        if report is None:
            await self._send_text(chat_id=chat_id, text="未找到对应分析记录。")
            return ActionResult(command="news_detail")
        digest = self._news_digest_from_metrics(self._clean_key_metrics(report.key_metrics))
        text = (
            f"新闻详单（{digest.window_label}）\n"
            f"总条数：{digest.total_count}\n"
            f"情绪方向：{digest.sentiment_direction}（{digest.sentiment_range}）\n"
            + "\n".join(format_top_news_lines(digest))
        )
        await self._send_text(chat_id=chat_id, text=self._clip_line(text, limit=3400))
        return ActionResult(command="news_detail")

    async def handle_news_cluster(self, *, chat_id: str, target_id: str) -> ActionResult:
        report = self._store.get_analysis_report(report_id=target_id, chat_id=chat_id)
        if report is None:
            await self._send_text(chat_id=chat_id, text="未找到对应分析记录。")
            return ActionResult(command="news_cluster")
        digest = self._news_digest_from_metrics(self._clean_key_metrics(report.key_metrics))
        source_line = "、".join(digest.source_coverage) if digest.source_coverage else "无"
        text = (
            f"事件聚类（{digest.window_label}）\n"
            f"总条数：{digest.total_count}\n"
            f"来源覆盖：{source_line}\n"
            f"情绪方向：{digest.sentiment_direction}（{digest.sentiment_range}）\n"
            "分布：\n"
            + "\n".join(format_cluster_lines(digest))
        )
        await self._send_text(chat_id=chat_id, text=self._clip_line(text, limit=2600))
        return ActionResult(command="news_cluster")

    async def handle_digest(self, *, chat_id: str, period: str) -> ActionResult:
        digest = self._store.build_daily_digest(chat_id=chat_id) if period == "daily" else {}
        if not digest:
            await self._send_text(chat_id=chat_id, text="暂不支持该摘要周期。")
            return ActionResult(command="digest")

        latest = digest.get("latest_reports") or []
        lines = [
            "每日报告（近24小时）",
            f"active_jobs={digest['active_jobs']}",
            f"alerts_triggered={digest['alerts_triggered']}",
            f"delivered_notifications={digest['delivered_notifications']}",
            f"completed_analyses={digest['completed_analyses']}",
        ]
        if latest:
            lines.append("最新报告：")
            for item in latest:
                lines.append(f"- {item['symbol']} run_id={item['run_id']}")
        await self._send_text(chat_id=chat_id, text="\n".join(lines))
        self._store.record_metric(metric_name="digest_generated", metric_value=1.0)
        return ActionResult(command="digest")

    async def handle_list(self, *, chat_id: str) -> ActionResult:
        jobs = self._store.list_watch_jobs(chat_id=chat_id, include_disabled=False)
        if not jobs:
            await self._send_text(chat_id=chat_id, text="当前无活跃监控任务。可用 /monitor <symbol> <interval> 创建。")
            return ActionResult(command="list")

        lines = ["活跃监控任务："]
        for job in jobs:
            last_triggered_at, last_pct_change = self._store.get_recent_watch_event_summary(job_id=job.job_id)
            recent = "无"
            if last_triggered_at is not None and last_pct_change is not None:
                recent = f"{last_triggered_at} ({round(last_pct_change * 100, 2)}%)"
            lines.append(
                f"- {job.job_id} {job.symbol} 每{job.interval_sec}s "
                f"下次={job.next_run_at} 路由={job.route_strategy} 层级={job.strategy_tier} 最近触发={recent}"
            )
        await self._send_text(chat_id=chat_id, text="\n".join(lines))
        return ActionResult(command="list")

    async def handle_alerts(self, *, chat_id: str, view: str, limit: int) -> ActionResult:
        rows = self._store.list_alert_hub(chat_id=chat_id, view=view, limit=limit)
        if not rows:
            await self._send_text(chat_id=chat_id, text=f"视图 {view} 暂无告警记录。")
            return ActionResult(command="alerts")
        lines = [f"告警中心 view={view}（最近 {limit} 条）"]
        for row in rows:
            extra = ""
            if row.suppressed_reason:
                extra = f" 抑制原因={row.suppressed_reason}"
            elif row.last_error:
                extra = f" 错误={row.last_error[:80]}"
            lines.append(
                f"- {row.event_id} {row.symbol} {row.priority} 层级={row.strategy_tier} {row.channel}:{row.status}{extra}"
            )
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

    async def handle_route(
        self,
        *,
        chat_id: str,
        action: str,
        channel: str = "",
        target: str = "",
    ) -> ActionResult:
        normalized_channel = str(channel).strip().lower()
        normalized_target = str(target).strip()
        if action == "set":
            if normalized_channel == "telegram" and normalized_target.lower() in {"self", "chat", "default"}:
                normalized_target = chat_id
            self._store.upsert_notification_route(
                chat_id=chat_id,
                channel=normalized_channel,
                target=normalized_target,
                enabled=True,
            )
            await self._send_text(
                chat_id=chat_id,
                text=f"Route 已启用 (Route enabled): channel={normalized_channel} target={normalized_target}",
            )
            return ActionResult(command="route")

        if action == "disable":
            all_routes = self._store.list_notification_routes(chat_id=chat_id, enabled_only=False)
            matched = [item for item in all_routes if item.channel == normalized_channel and item.target == normalized_target]
            if not matched:
                await self._send_text(chat_id=chat_id, text="未找到 Route (Route not found).")
                return ActionResult(command="route")
            self._store.upsert_notification_route(
                chat_id=chat_id,
                channel=normalized_channel,
                target=normalized_target,
                enabled=False,
            )
            await self._send_text(
                chat_id=chat_id,
                text=f"Route 已禁用 (Route disabled): channel={normalized_channel} target={normalized_target}",
            )
            return ActionResult(command="route")

        routes = self._store.list_notification_routes(chat_id=chat_id, enabled_only=False)
        if not routes:
            await self._send_text(chat_id=chat_id, text="未配置 Route (No routes configured).")
            return ActionResult(command="route")
        lines = ["Route 列表 (Routes):"]
        for item in routes:
            lines.append(f"- {item.channel} target={item.target} enabled={item.enabled}")
        await self._send_text(chat_id=chat_id, text="\n".join(lines))
        return ActionResult(command="route")

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

        await self._send_text(chat_id=chat_id, text=f"已停止监控任务 {disabled} 个，target={target}")
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
        cancel_event: asyncio.Event | None = None,
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
                    result = await self._run_research_with_cancel(
                        cancel_event=cancel_event,
                        timeout_seconds=timeout_seconds,
                        request=request_text,
                        symbol=symbol,
                    )
            run_id = str(result.get("run_id", ""))
            summary = str(((result.get("fused_insights") or {}).get("summary") or "")).strip()
            key_metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
            latest_close = self._metric_float(key_metrics, "latest_close", "data_close")
            rsi = self._metric_float(key_metrics, "technical_rsi_14", "rsi14")
            ma = self._metric_float(key_metrics, "technical_ma_20", "ma20")
            high = self._metric_float(key_metrics, "window_high", "period_high")
            low = self._metric_float(key_metrics, "window_low", "period_low")
            ret30 = self._metric_float(key_metrics, "return_30d", "change_30d")
            max_drawdown = self._metric_float(key_metrics, "max_drawdown_30d", "max_drawdown")
            news_digest = build_news_digest_from_result(result, window_days=7)
            report_metrics = self._clean_key_metrics(key_metrics)
            report_metrics.update(
                {
                    "news_digest": news_digest.to_dict(),
                    "news_total_count": news_digest.total_count,
                    "news_window_label": news_digest.window_label,
                    "news_sentiment_direction": news_digest.sentiment_direction,
                    "news_sentiment_range": news_digest.sentiment_range,
                }
            )
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
                    summary=summary or "暂无摘要",
                    key_metrics=report_metrics,
                )
            self._store.upsert_request_chart_state(request_id=request_id, chart_state="none")
            market_source = str(
                key_metrics.get("market_data_source")
                or key_metrics.get("data_source")
                or key_metrics.get("quote_source")
                or "aggregated_market_feed"
            ).strip()
            market_updated_at = str(
                key_metrics.get("market_data_updated_at")
                or key_metrics.get("data_updated_at")
                or key_metrics.get("quote_updated_at")
                or key_metrics.get("last_updated_at")
                or datetime.now(timezone.utc).isoformat()
            ).strip()
            main_text, supplement_text = self._build_analysis_contract(
                symbol=symbol,
                period="1mo",
                summary=summary,
                latest_close=latest_close,
                high=high,
                low=low,
                ret30=ret30,
                max_drawdown=max_drawdown,
                rsi=rsi,
                ma=ma,
                market_source=market_source,
                market_updated_at=market_updated_at,
                indicator_spec="RSI14(1d)、MA20(1d)",
                chart_note=None,
                news_digest=news_digest,
            )
            schema_version = self._final_schema_version_for_snapshot(
                period="1mo",
                news_window_days=7,
                need_chart=False,
            )
            if self._store.claim_final_message_dispatch(
                request_id=request_id,
                final_schema_version=schema_version,
            ):
                payload = await self._send_text(
                    chat_id=chat_id,
                    text=main_text,
                    buttons=self._snapshot_buttons(request_id=request_id, chart_state="none"),
                )
                self._store.mark_final_message_dispatched(
                    request_id=request_id,
                    final_schema_version=schema_version,
                    message_id=self._extract_message_id(payload),
                )
                if supplement_text:
                    await self._send_text(chat_id=chat_id, text=supplement_text)
            else:
                self._store.record_metric(metric_name="dedupe_suppressed_count", metric_value=1.0)
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
            await self._send_text(chat_id=chat_id, text="分析超时，结果将在后台自动重试。")
        except Exception as exc:
            self._store.transition_analysis_request_status(
                request_id=request_id,
                from_statuses=("running",),
                to_status="failed",
                run_id=None,
                last_error=str(exc),
            )
            await self._send_text(chat_id=chat_id, text="分析失败，请稍后重试。")
        finally:
            latency_ms = (time.perf_counter() - start) * 1000
            self._store.record_metric(metric_name="analysis_latency_ms", metric_value=latency_ms, tags={"chat_id": chat_id})

    async def _run_research_with_cancel(
        self,
        *,
        cancel_event: asyncio.Event | None,
        timeout_seconds: float,
        **runner_kwargs: Any,
    ) -> dict[str, Any]:
        runner_task = asyncio.create_task(self._research_runner(**runner_kwargs))
        cancel_task: asyncio.Task[bool] | None = None
        try:
            if cancel_event is None:
                return await asyncio.wait_for(runner_task, timeout=timeout_seconds)

            cancel_task = asyncio.create_task(cancel_event.wait())
            done, pending = await asyncio.wait(
                {runner_task, cancel_task},
                timeout=timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()

            if runner_task in done:
                return runner_task.result()
            if cancel_task in done and cancel_task.result():
                runner_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await runner_task
                raise RuntimeError("analysis_cancelled")

            runner_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await runner_task
            raise TimeoutError("analysis_timeout")
        finally:
            if cancel_task is not None:
                cancel_task.cancel()
