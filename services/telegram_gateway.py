from __future__ import annotations

import asyncio
import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from uuid import uuid4

import aiohttp

from agents.telegram_command_router import CommandError, parse_telegram_command
from agents.telegram_nlu_planner import (
    NLUPlan,
    detect_prompt_injection_risk,
    extract_clarify_slots,
    hash_text,
    parse_interval_to_seconds,
    plan_from_text,
    sanitize_user_text,
)
from services.runtime_controls import RuntimeLimits
from services.telegram_actions import TelegramActions
from services.telegram_store import TelegramTaskStore
from tools.market_data import normalize_market_symbol


_ALLOWED_ANALYZE_PERIODS = {"5d", "1mo", "3mo", "6mo", "1y"}
_SYMBOL_ALIAS_MAP_VERSION = "v1.0.0"
_SYMBOL_ALIAS_UPDATED_AT = "2026-02-27T00:00:00+00:00"
_SYMBOL_ALIAS_MAP: dict[str, list[str]] = {
    "腾讯": ["0700.HK", "TCEHY"],
    "tencent": ["0700.HK", "TCEHY"],
    "阿里": ["9988.HK", "BABA"],
    "alibaba": ["9988.HK", "BABA"],
    "特斯拉": ["TSLA"],
    "tesla": ["TSLA"],
    "苹果": ["AAPL"],
    "apple": ["AAPL"],
}


def _mask_chat_id(chat_id: str) -> str:
    if len(chat_id) <= 4:
        return "***"
    return f"{chat_id[:2]}***{chat_id[-2:]}"


class TelegramGateway:
    _CLARIFY_SLOT_WHITELIST = {"symbol", "period", "interval", "template", "market"}
    _GENERAL_CONVERSATION_INTENTS = {"greeting", "capability", "help", "how_to_start"}
    _TEMPLATE_MODE_THRESHOLD = {
        "volatility": ("anomaly", 0.03),
        "price": ("price_breakout", 0.02),
        "rsi": ("rsi_extreme", 70.0),
    }

    def __init__(
        self,
        *,
        store: TelegramTaskStore,
        actions: TelegramActions,
        limits: RuntimeLimits | None = None,
        allowed_chat_ids: set[str] | None = None,
        blocked_chat_ids: set[str] | None = None,
        access_mode: str = "blacklist",
        allowed_commands: set[str] | None = None,
        gray_release_enabled: bool = False,
        nlu_parser: Callable[[str], NLUPlan] = plan_from_text,
    ):
        self._store = store
        self._actions = actions
        self._limits = limits or RuntimeLimits()
        self._allowed_chat_ids = allowed_chat_ids or set()
        self._blocked_chat_ids = blocked_chat_ids or set()
        mode = (access_mode or "blacklist").strip().lower()
        self._access_mode = mode if mode in {"allowlist", "blacklist"} else "blacklist"
        self._allowed_commands = allowed_commands or {
            "help",
            "analyze",
            "monitor",
            "list",
            "stop",
            "report",
            "digest",
            "alerts",
            "bulk",
            "webhook",
            "pref",
        }
        self._gray_release_enabled = gray_release_enabled
        self._nlu_parser = nlu_parser
        self._offset = max(0, self._store.get_latest_update_id() + 1)
        self._cancel_events_by_chat: dict[str, asyncio.Event] = {}

    def _is_chat_denied(self, *, chat_id: str) -> bool:
        if self._access_mode == "allowlist":
            return bool(self._allowed_chat_ids) and chat_id not in self._allowed_chat_ids
        return chat_id in self._blocked_chat_ids

    @staticmethod
    def _audit_payload(update: dict[str, Any]) -> dict[str, Any]:
        message = update.get("message") or {}
        callback_query = update.get("callback_query") or {}
        callback_message = callback_query.get("message") or {}
        callback_data = str(callback_query.get("data", "")).strip()
        effective_message = message or callback_message
        effective_chat = effective_message.get("chat") or {}
        effective_text = str(effective_message.get("text", "")).strip()
        chat = message.get("chat") or {}
        text = str(message.get("text", "")).strip()
        return {
            "update_id": int(update.get("update_id", 0)),
            "message": {
                "chat": {"id": str(effective_chat.get("id", "")), "type": str(effective_chat.get("type", ""))},
                "text": effective_text[:256],
                "from": effective_message.get("from") or {},
            },
            "callback_query": {
                "id": str(callback_query.get("id", "")),
                "data": callback_data[:128],
            }
            if callback_query
            else None,
            "raw_message": {"chat": {"id": str(chat.get("id", ""))}, "text": text[:256]},
        }

    @staticmethod
    def _short_request_id(request_id: str) -> str:
        return request_id[-6:] if request_id else "n/a"

    @staticmethod
    def _conversation_scope_key(*, chat_id: str, chat_type: str, user_id: str | None) -> str:
        chat_kind = (chat_type or "").lower()
        if chat_kind in {"group", "supergroup"} and user_id:
            return f"group_user:{chat_id}:{user_id}"
        return f"chat:{chat_id}"

    @staticmethod
    def _extract_period_from_text(text: str) -> str | None:
        lowered = (text or "").lower()
        if any(item in lowered for item in ("近30天", "一个月", "1个月", "一月", "本月", "上月", "30d", "30天", "1mo")):
            return "1mo"
        if any(item in lowered for item in ("近3个月", "三个月", "3个月", "3mo")):
            return "3mo"
        if any(item in lowered for item in ("半年", "6个月", "6mo")):
            return "6mo"
        if any(item in lowered for item in ("一年", "1年", "1y")):
            return "1y"
        if any(item in lowered for item in ("一周", "1周", "week")):
            return "5d"
        return None

    @staticmethod
    def _extract_explicit_symbol_token(text: str) -> str | None:
        for token in re.findall(r"[A-Za-z0-9.\-]{1,16}", text or ""):
            if token.strip().lower() in {"k"}:
                continue
            normalized = normalize_market_symbol(token, market="auto")
            if normalized and re.fullmatch(r"[A-Z0-9.\-]{1,12}", normalized):
                return normalized
        return None

    @staticmethod
    def _extract_alias_candidates(text: str) -> tuple[list[str], str | None]:
        lowered = (text or "").lower()
        matched_alias: str | None = None
        for alias, symbols in _SYMBOL_ALIAS_MAP.items():
            if alias in lowered:
                matched_alias = alias
                return [str(item).upper() for item in symbols], alias
        return [], matched_alias

    @staticmethod
    def _is_explicit_switch_text(text: str) -> bool:
        lowered = (text or "").lower()
        return any(item in lowered for item in ("不是", "换成", "改看", "换标的", "怎么样"))

    def _inject_snapshot_defaults(
        self,
        *,
        plan: NLUPlan,
        normalized_text: str,
        scope_key: str,
    ) -> tuple[NLUPlan, str | None, bool]:
        if plan.intent != "analyze_snapshot":
            return plan, None, False

        context = self._store.get_conversation_context(scope_key=scope_key)
        slots = dict(plan.slots)
        explicit_symbol = self._extract_explicit_symbol_token(normalized_text)
        candidates, alias = self._extract_alias_candidates(normalized_text)
        explicit_switch = self._is_explicit_switch_text(normalized_text)
        carry_hit = False
        carry_symbol: str | None = None

        if explicit_symbol:
            slots["symbol"] = explicit_symbol
        elif len(candidates) == 1:
            slots["symbol"] = candidates[0]
        elif len(candidates) > 1 and not explicit_symbol:
            slots["_candidate_symbols"] = candidates
            slots["_candidate_alias"] = alias
            plan.slots = slots
            plan.reject_reason = "candidate_selection_needed"
            plan.explain = "candidate_selection_needed"
            plan.clarify_slot = "symbol"
            return plan, None, False
        elif context and context.last_symbol_context and (not str(slots.get("symbol", "")).strip()):
            slots["symbol"] = str(context.last_symbol_context)
            carry_hit = True
            carry_symbol = str(context.last_symbol_context)

        infer_period = self._extract_period_from_text(normalized_text)
        if infer_period:
            slots["period"] = infer_period
        elif context and context.last_period_context:
            slots["period"] = str(context.last_period_context)
        else:
            slots["period"] = "1mo"

        if str(slots.get("period", "")).lower() not in _ALLOWED_ANALYZE_PERIODS:
            slots["period"] = "1mo"
        slots["_context_scope_key"] = scope_key
        slots["_alias_map_version"] = _SYMBOL_ALIAS_MAP_VERSION
        slots["_alias_map_updated_at"] = _SYMBOL_ALIAS_UPDATED_AT
        plan.slots = slots

        symbol = str(slots.get("symbol", "")).strip()
        if symbol:
            plan.reject_reason = None
            plan.explain = "snapshot defaults resolved"
            if plan.confidence < 0.75:
                plan.confidence = 0.9
        elif not candidates and explicit_switch:
            plan.reject_reason = "unknown_symbol"
            plan.explain = "alias_not_found"
        return plan, carry_symbol, carry_hit

    async def enqueue_update(self, update: dict[str, Any]) -> int | None:
        update_id = int(update.get("update_id", 0))
        if update_id <= 0:
            return None

        message = update.get("message") or {}
        callback_query = update.get("callback_query") or {}
        callback_message = callback_query.get("message") or {}
        chat = (message or callback_message).get("chat") or {}
        chat_id = str(chat.get("id", ""))
        inserted = self._store.insert_bot_update_if_new(
            update_id=update_id,
            chat_id=chat_id,
            payload=self._audit_payload(update),
        )
        if not inserted:
            self._store.record_metric(metric_name="duplicate_update_dropped", metric_value=1.0)
            return None
        return update_id

    async def process_update(self, update: dict[str, Any]) -> bool:
        update_id = await self.enqueue_update(update)
        if update_id is None:
            return False
        return await self.process_enqueued_update(update_id=update_id)

    @staticmethod
    def _is_high_risk_intent(intent: str) -> bool:
        return intent in {"create_monitor", "stop_job", "bulk_change"}

    @staticmethod
    def _current_ts() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _build_bucket(now: datetime, seconds: int = 30) -> int:
        epoch = int(now.timestamp())
        return epoch - (epoch % max(1, seconds))

    def _build_dedupe_keys(
        self,
        *,
        chat_id: str,
        normalized_text: str,
        intent: str,
        slots: dict[str, Any],
        now: datetime,
    ) -> tuple[str, str]:
        bucket = self._build_bucket(now, seconds=30)
        text_key = f"{chat_id}:{hash_text(normalized_text.lower())}:{bucket}"
        slots_key = json.dumps(slots, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        intent_key = f"{chat_id}:{intent}:{hash_text(slots_key)}:{bucket}"
        return text_key, intent_key

    def _evaluate_llm_degradation(self) -> None:
        now = self._current_ts()
        since = now - timedelta(minutes=max(1, int(self._limits.llm_degrade_window_minutes)))
        total = self._store.count_metric_events(metric_name="llm_parse_total", since=since)
        failed = self._store.count_metric_events(metric_name="llm_parse_failed", since=since)
        fail_rate = (failed / total) if total else 0.0
        min_samples = max(1, int(self._limits.llm_degrade_min_samples))
        active = self._store.is_degradation_active(state_key="nl_command_hint_mode")
        if total >= min_samples and fail_rate >= float(self._limits.llm_degrade_fail_rate_threshold):
            if not active:
                self._store.set_degradation_state(
                    state_key="nl_command_hint_mode",
                    status="active",
                    reason=f"llm_parse_fail_rate={fail_rate:.2%}",
                )
            return
        if active and fail_rate <= float(self._limits.llm_recover_fail_rate_threshold):
            self._store.set_degradation_state(
                state_key="nl_command_hint_mode",
                status="recovered",
                reason=f"llm_parse_fail_rate={fail_rate:.2%}",
            )

    async def _parse_nlu_plan(self, *, text: str) -> tuple[NLUPlan | None, str | None]:
        attempts = max(1, int(self._limits.nl_parse_max_retries) + 1)
        last_error: str | None = None
        timeout_seconds = float(self._limits.nl_parse_timeout_seconds)
        for _ in range(attempts):
            started = time.perf_counter()
            self._store.record_metric(metric_name="llm_parse_total", metric_value=1.0)
            try:
                plan = self._nlu_parser(text)
                latency_ms = (time.perf_counter() - started) * 1000
                if timeout_seconds > 0 and (latency_ms / 1000.0) > timeout_seconds:
                    raise TimeoutError(f"nlu_parse_timeout>{timeout_seconds}s")
                self._store.record_metric(metric_name="llm_parse_latency_ms", metric_value=latency_ms)
                self._evaluate_llm_degradation()
                return plan, None
            except Exception as exc:  # noqa: BLE001
                latency_ms = (time.perf_counter() - started) * 1000
                self._store.record_metric(metric_name="llm_parse_latency_ms", metric_value=latency_ms)
                self._store.record_metric(metric_name="llm_parse_failed", metric_value=1.0, tags={"reason": str(exc)[:48]})
                last_error = str(exc)
        self._evaluate_llm_degradation()
        return None, last_error

    async def _send_fallback_help(self, *, chat_id: str, reason: str) -> None:
        self._store.record_metric(metric_name="nl_intent_fallback_help", metric_value=1.0, tags={"reason": reason})
        await self._actions.send_error_message(
            chat_id=chat_id,
            text=(
                f"已降级为命令提示模式 (Fallback to command hints): {reason}\n"
                "请使用命令：/analyze <symbol> 或 /monitor <symbol> <interval>"
            ),
        )

    async def _send_nl_reject(self, *, chat_id: str, reason: str, template: str) -> None:
        await self._actions.send_error_message(
            chat_id=chat_id,
            text=f"请求已拒绝 (Rejected): {reason}\n可复制命令模板 (Command template): {template}",
        )

    async def _send_pending_confirm_prompt(self, *, chat_id: str, request_id: str) -> None:
        short_id = request_id[-6:]
        await self._actions.send_error_message(
            chat_id=chat_id,
            text=(
                "高风险操作待确认 (Pending confirmation).\n"
                f"request_id={request_id} short={short_id}\n"
                "请优先点击确认按钮 (prefer callback_data): yes|<request_id> / no|<request_id>\n"
                f"降级文本确认: yes {short_id} / no {short_id}\n"
                "取消: /cancel"
            ),
        )

    @staticmethod
    def _default_plan_steps(intent: str) -> list[dict[str, str]]:
        return [
            {"step": "validate_slots", "action": "validate_slots"},
            {"step": "execute_intent", "action": "execute_intent"},
            {"step": "render_response", "action": "render_response"},
        ]

    @classmethod
    def _is_conversation_intent(cls, intent: str) -> bool:
        return intent in cls._GENERAL_CONVERSATION_INTENTS

    def _get_chat_cancel_event(self, *, chat_id: str) -> asyncio.Event:
        event = self._cancel_events_by_chat.get(chat_id)
        if event is None:
            event = asyncio.Event()
            self._cancel_events_by_chat[chat_id] = event
        return event

    async def _cancel_executing_request(self, *, chat_id: str, update_id: int) -> bool:
        running = self._store.get_executing_nl_request(chat_id=chat_id)
        if running is None:
            return False
        event = self._get_chat_cancel_event(chat_id=chat_id)
        event.set()
        await self._actions.send_error_message(
            chat_id=chat_id,
            text=f"已发送取消信号，处理中。request_id(short)={self._short_request_id(running.request_id)}",
        )
        self._store.update_bot_update_status(
            update_id=update_id,
            status="processed",
            command="cancel_execution_requested",
            request_id=running.request_id,
            error=None,
        )
        self._store.record_metric(metric_name="nl_cancel_execution_requested", metric_value=1.0)
        return True

    def _compact_conversation_history_if_needed(
        self,
        *,
        chat_id: str,
        scope_key: str,
        update_id: int,
    ) -> None:
        compacted = self._store.compact_conversation_history(
            chat_id=chat_id,
            scope_key=scope_key,
            keep_recent=int(self._limits.conversation_archive_keep_recent),
            min_batch=int(self._limits.conversation_archive_min_batch),
        )
        if not compacted:
            return
        self._store.record_metric(metric_name="conversation_archive_total", metric_value=1.0)
        self._store.add_audit_event(
            event_type="conversation_archived",
            chat_id=chat_id,
            update_id=update_id,
            action="compact_conversation",
            reason="history_compacted",
            metadata=compacted,
        )

    async def _maybe_handle_snapshot_singleflight(
        self,
        *,
        chat_id: str,
        update_id: int,
        scope_key: str,
        plan: NLUPlan,
    ) -> bool:
        ttl_seconds = int(self._limits.session_singleflight_ttl_seconds)
        if ttl_seconds <= 0:
            return False
        if plan.intent != "analyze_snapshot":
            return False
        symbol = str(plan.slots.get("symbol", "")).strip().upper()
        period = str(plan.slots.get("period", "")).strip().lower()
        interval = str(plan.slots.get("interval", "")).strip().lower()
        if not symbol or not period or not interval:
            return False

        existing = self._store.find_recent_snapshot_singleflight(
            chat_id=chat_id,
            scope_key=scope_key,
            symbol=symbol,
            period=period,
            interval=interval,
            ttl_seconds=ttl_seconds,
        )
        if existing is None:
            return False

        if existing.status in {"queued", "executing"}:
            await self._actions.send_error_message(
                chat_id=chat_id,
                text=(
                    "同会话已有相同分析进行中，已复用该请求。\n"
                    f"request_id(short)={self._short_request_id(existing.request_id)}"
                ),
            )
            self._store.record_metric(metric_name="analysis_singleflight_hit_total", metric_value=1.0, tags={"state": "inflight"})
            self._store.update_bot_update_status(
                update_id=update_id,
                status="processed",
                command="nl_singleflight_inflight",
                request_id=existing.request_id,
                error=None,
            )
            return True

        report = self._store.get_analysis_report(report_id=existing.request_id, chat_id=chat_id)
        run_hint = f"run_id={report.run_id}" if report else "run_id=N/A"
        chart_state_rec = self._store.get_request_chart_state(request_id=existing.request_id)
        chart_state = chart_state_rec.chart_state if chart_state_rec else "none"
        await self._actions.send_inline_buttons(
            chat_id=chat_id,
            text=(
                "同会话短窗复用最近分析结果，避免重复重算。\n"
                f"request_id(short)={self._short_request_id(existing.request_id)} {run_hint}"
            ),
            buttons=self._actions.build_snapshot_buttons(
                request_id=existing.request_id,
                chart_state=chart_state,
            ),
        )
        self._store.record_metric(metric_name="analysis_singleflight_hit_total", metric_value=1.0, tags={"state": "completed"})
        self._store.update_bot_update_status(
            update_id=update_id,
            status="processed",
            command="nl_singleflight_reuse",
            request_id=existing.request_id,
            error=None,
        )
        return True

    @staticmethod
    def _extract_plan_steps(slots: dict[str, Any], intent: str) -> list[dict[str, Any]]:
        raw = slots.get("_plan_steps")
        if isinstance(raw, list):
            steps: list[dict[str, Any]] = []
            for item in raw:
                if not isinstance(item, dict):
                    continue
                action = str(item.get("action", "")).strip()
                step = str(item.get("step", action)).strip()
                if not action:
                    continue
                steps.append({"step": step or action, "action": action})
            if steps:
                return steps
        return TelegramGateway._default_plan_steps(intent)

    def _resolve_clarify_followup(self, *, pending: Any, text: str) -> tuple[dict[str, Any], list[str]]:
        followup_slots = extract_clarify_slots(text)
        merged_slots = dict(pending.slots)
        for key in self._CLARIFY_SLOT_WHITELIST:
            value = followup_slots.get(key)
            if value in (None, ""):
                continue
            merged_slots[key] = value

        if pending.intent == "create_monitor":
            interval = str(merged_slots.get("interval", "")).lower()
            if interval:
                merged_slots["interval"] = interval
                merged_slots["interval_sec"] = parse_interval_to_seconds(interval)
            template = str(merged_slots.get("template", "volatility")).lower()
            mode, threshold = self._TEMPLATE_MODE_THRESHOLD.get(template, ("anomaly", 0.03))
            merged_slots["template"] = template
            merged_slots["mode"] = mode
            merged_slots["threshold"] = threshold
        if pending.intent == "stop_job":
            symbol = str(merged_slots.get("symbol", "")).upper()
            if symbol:
                merged_slots["target"] = symbol
                merged_slots["target_type"] = "symbol"

        unresolved = [slot for slot in pending.missing_slots if not str(merged_slots.get(slot, "")).strip()]
        return merged_slots, unresolved

    def _add_execution_evidence(
        self,
        *,
        record: Any,
        update_id: int,
        result: str,
        run_id: str | None = None,
        error: str | None = None,
    ) -> None:
        schema_version = str(record.slots.get("_schema_version", "telegram_nlu_plan_v1"))
        self._store.add_audit_event(
            event_type="nl_execution_evidence",
            chat_id=record.chat_id,
            update_id=update_id,
            action=record.intent,
            reason=result,
            metadata={
                "request_id": record.request_id,
                "schema_version": schema_version,
                "action_version": record.action_version,
                "intent": record.intent,
                "result": result,
                "run_id": run_id,
                "error": error,
            },
        )

    def _add_plan_step_event(
        self,
        *,
        record: Any,
        update_id: int,
        index: int,
        step: dict[str, Any],
        status: str,
        error: str | None = None,
    ) -> None:
        self._store.add_audit_event(
            event_type="nl_plan_step",
            chat_id=record.chat_id,
            update_id=update_id,
            action=record.intent,
            reason=status,
            metadata={
                "request_id": record.request_id,
                "action_version": record.action_version,
                "schema_version": str(record.slots.get("_schema_version", "telegram_nlu_plan_v1")),
                "step_index": index,
                "step": str(step.get("step", "")),
                "step_action": str(step.get("action", "")),
                "status": status,
                "error": error,
            },
        )

    async def _execute_nl_intent(self, *, record: Any, update_id: int) -> str | None:
        slots = record.slots
        cancel_event = self._get_chat_cancel_event(chat_id=record.chat_id)
        if self._is_conversation_intent(record.intent):
            await self._actions.handle_general_conversation(chat_id=record.chat_id, intent=record.intent)
            return None
        if record.intent == "create_monitor":
            await self._actions.handle_monitor(
                chat_id=record.chat_id,
                symbol=str(slots.get("symbol", "")),
                symbols=[str(slots.get("symbol", ""))],
                interval_sec=int(slots.get("interval_sec", 0)),
                mode=str(slots.get("mode", "anomaly")),
                threshold=float(slots.get("threshold", 0.03)),
                template=str(slots.get("template", "volatility")),
                route_strategy=str(slots.get("route_strategy", "dual_channel")),
            )
            return None
        if record.intent == "analyze_snapshot":
            if self._limits.send_progress_updates:
                await self._actions.send_analysis_progress(
                    chat_id=record.chat_id,
                    request_id=record.request_id,
                    stage="identify_symbol",
                )
                await self._actions.send_analysis_progress(
                    chat_id=record.chat_id,
                    request_id=record.request_id,
                    stage="fetch_market_data",
                )
                await self._actions.send_analysis_progress(
                    chat_id=record.chat_id,
                    request_id=record.request_id,
                    stage="merge_news",
                )
                await self._actions.send_analysis_progress(
                    chat_id=record.chat_id,
                    request_id=record.request_id,
                    stage="process_chart",
                )
            want_chart = bool(slots.get("need_chart", False))
            fail_before = self._store.count_metric_events(metric_name="chart_render_fail_rate") if want_chart else 0
            if want_chart:
                self._store.record_metric(metric_name="chart_render_attempt_total", metric_value=1.0)
                if self._store.is_degradation_active(state_key="chart_text_only"):
                    slots = dict(slots)
                    slots["need_chart"] = False
                    await self._actions.send_error_message(
                        chat_id=record.chat_id,
                        text="图表服务降级中，已返回文本替代 (chart_text_only).",
                    )
                    self._store.add_audit_event(
                        event_type="degrade_skip",
                        chat_id=record.chat_id,
                        update_id=update_id,
                        action="analyze_snapshot_chart",
                        reason="chart_text_only",
                    )
            await self._actions.handle_analyze_snapshot(
                chat_id=record.chat_id,
                symbol=str(slots.get("symbol", "")).upper(),
                period=str(slots.get("period", "1mo")).lower(),
                interval=str(slots.get("interval", "1d")).lower(),
                need_chart=bool(slots.get("need_chart", False)),
                need_news=bool(slots.get("need_news", False)),
                request_id=record.request_id,
                cancel_event=cancel_event,
            )
            if want_chart:
                fail_after = self._store.count_metric_events(metric_name="chart_render_fail_rate")
                if fail_after > fail_before or bool(slots.get("need_chart", False)) is False:
                    self._store.record_metric(metric_name="chart_render_fail_total", metric_value=1.0)
                self._evaluate_chart_degradation()
            report = self._store.get_analysis_report(report_id=record.request_id, chat_id=record.chat_id)
            return report.run_id if report else None
        if record.intent == "list_jobs":
            await self._actions.handle_list(chat_id=record.chat_id)
            return None
        if record.intent == "stop_job":
            await self._actions.handle_stop(
                chat_id=record.chat_id,
                target=str(slots.get("target", "")),
                target_type=str(slots.get("target_type", "symbol")),
            )
            return None
        if record.intent == "daily_digest":
            await self._actions.handle_digest(chat_id=record.chat_id, period=str(slots.get("period", "daily")))
            return None
        raise ValueError(f"unsupported_intent:{record.intent}")

    async def _execute_nl_request(self, *, update_id: int, request_id: str) -> bool:
        record = self._store.get_nl_request(request_id=request_id)
        if record is None:
            return False
        if record.intent not in {
            "create_monitor",
            "analyze_snapshot",
            "list_jobs",
            "stop_job",
            "daily_digest",
            *self._GENERAL_CONVERSATION_INTENTS,
        }:
            self._store.set_nl_request_status(
                request_id=request_id,
                to_status="failed",
                last_error="unsupported_intent",
            )
            return False
        moved = self._store.transition_nl_request_status(
            request_id=request_id,
            from_statuses=("queued", "pending_confirm"),
            to_status="executing",
            reject_reason=None,
            last_error=None,
            confirm_deadline_at=None,
        )
        if not moved:
            return False
        cancel_event = self._get_chat_cancel_event(chat_id=record.chat_id)
        cancel_event.clear()
        if record.intent == "analyze_snapshot":
            await self._actions.send_analysis_ack(chat_id=record.chat_id, request_id=record.request_id)
        plan_steps = self._extract_plan_steps(record.slots, record.intent)
        run_id: str | None = None
        try:
            for idx, step in enumerate(plan_steps, start=1):
                self._add_plan_step_event(record=record, update_id=update_id, index=idx, step=step, status="started")
                action = str(step.get("action", "")).strip().lower()
                try:
                    if action == "execute_intent":
                        run_id = await self._execute_nl_intent(record=record, update_id=update_id)
                    self._add_plan_step_event(record=record, update_id=update_id, index=idx, step=step, status="completed")
                except Exception as exc:
                    self._add_plan_step_event(
                        record=record,
                        update_id=update_id,
                        index=idx,
                        step=step,
                        status="failed",
                        error=str(exc),
                    )
                    raise
            self._store.set_nl_request_status(
                request_id=request_id,
                to_status="completed",
                reject_reason=None,
                last_error=None,
                confirm_deadline_at=None,
            )
            if record.intent == "analyze_snapshot":
                scope_key = str(record.slots.get("_context_scope_key", "")).strip()
                symbol = str(record.slots.get("symbol", "")).strip().upper()
                period = str(record.slots.get("period", "1mo")).strip().lower()
                if scope_key and symbol:
                    self._store.upsert_conversation_context(
                        scope_key=scope_key,
                        last_symbol_context=symbol,
                        last_period_context=period if period in _ALLOWED_ANALYZE_PERIODS else "1mo",
                        ttl_seconds=1800,
                    )
            self._add_execution_evidence(record=record, update_id=update_id, result="completed", run_id=run_id)
            self._store.record_metric(metric_name="nl_intent_success", metric_value=1.0, tags={"intent": record.intent})
            self._store.update_bot_update_status(
                update_id=update_id,
                status="processed",
                command=f"nl_{record.intent}",
                request_id=request_id,
                error=None,
            )
            return True
        except Exception as exc:
            if str(exc) == "analysis_cancelled":
                self._store.set_nl_request_status(
                    request_id=request_id,
                    to_status="rejected",
                    reject_reason="cancelled",
                    last_error=str(exc),
                )
                await self._actions.send_error_message(
                    chat_id=record.chat_id,
                    text=f"任务已取消。request_id(short)={self._short_request_id(record.request_id)}",
                )
                self._add_execution_evidence(
                    record=record,
                    update_id=update_id,
                    result="cancelled",
                    run_id=run_id,
                    error=str(exc),
                )
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="processed",
                    command=f"nl_{record.intent}_cancelled",
                    request_id=request_id,
                    error=None,
                )
                self._store.record_metric(metric_name="nl_cancel_execution_effective", metric_value=1.0)
                return True
            self._store.set_nl_request_status(
                request_id=request_id,
                to_status="failed",
                last_error=str(exc),
            )
            self._add_execution_evidence(record=record, update_id=update_id, result="failed", run_id=run_id, error=str(exc))
            self._store.update_bot_update_status(
                update_id=update_id,
                status="failed",
                command=f"nl_{record.intent}",
                request_id=request_id,
                error=str(exc),
            )
            raise
        finally:
            cancel_event.clear()

    def _evaluate_chart_degradation(self) -> None:
        now = self._current_ts()
        since = now - timedelta(minutes=max(1, int(self._limits.chart_degrade_window_minutes)))
        attempts = self._store.count_metric_events(metric_name="chart_render_attempt_total", since=since)
        fails = self._store.count_metric_events(metric_name="chart_render_fail_total", since=since)
        fail_rate = (fails / attempts) if attempts else 0.0
        active = self._store.is_degradation_active(state_key="chart_text_only")
        if attempts >= max(1, int(self._limits.chart_degrade_min_samples)) and fail_rate >= float(
            self._limits.chart_degrade_fail_rate_threshold
        ):
            if not active:
                self._store.set_degradation_state(
                    state_key="chart_text_only",
                    status="active",
                    reason=f"chart_render_fail_rate={fail_rate:.2%}",
                )
            return
        if active and fail_rate <= float(self._limits.chart_recover_fail_rate_threshold):
            self._store.set_degradation_state(
                state_key="chart_text_only",
                status="recovered",
                reason=f"chart_render_fail_rate={fail_rate:.2%}",
            )

    async def _cancel_pending_confirm(self, *, chat_id: str, update_id: int) -> bool:
        pending = self._store.get_pending_confirm_request(chat_id=chat_id)
        if pending is None:
            await self._actions.send_error_message(chat_id=chat_id, text="当前无待确认请求 (No pending confirmation).")
            self._store.update_bot_update_status(
                update_id=update_id,
                status="processed",
                command="cancel_noop",
                request_id=None,
                error=None,
            )
            return True
        self._store.set_nl_request_status(
            request_id=pending.request_id,
            to_status="rejected",
            reject_reason="cancelled",
            confirm_deadline_at=None,
        )
        await self._actions.send_error_message(chat_id=chat_id, text=f"已取消请求 (Cancelled): {pending.request_id}")
        self._store.update_bot_update_status(
            update_id=update_id,
            status="processed",
            command="cancel_confirm",
            request_id=pending.request_id,
            error=None,
        )
        return True

    @staticmethod
    def _parse_callback_confirm(data: str) -> tuple[str, str] | None:
        raw = (data or "").strip()
        if not raw:
            return None
        if "|" in raw:
            left, right = raw.split("|", 1)
            action = left.strip().lower()
            request_ref = right.strip()
        elif ":" in raw:
            parts = [part.strip() for part in raw.split(":") if part.strip()]
            if len(parts) < 2:
                return None
            action = parts[-2].lower()
            request_ref = parts[-1]
        else:
            return None
        if action not in {"yes", "no", "cancel"}:
            return None
        if not request_ref:
            return None
        return action, request_ref

    @staticmethod
    def _parse_callback_candidate(data: str) -> tuple[str, str] | None:
        raw = (data or "").strip()
        if not raw.startswith("pick|"):
            return None
        parts = raw.split("|", 2)
        if len(parts) != 3:
            return None
        request_ref = parts[1].strip()
        chosen_symbol = parts[2].strip().upper()
        if not request_ref or not chosen_symbol:
            return None
        return request_ref, chosen_symbol

    @staticmethod
    def _parse_callback_action(data: str) -> tuple[str, str] | None:
        raw = (data or "").strip()
        if not raw.startswith("act|"):
            return None
        parts = raw.split("|", 2)
        if len(parts) != 3:
            return None
        request_ref = parts[1].strip()
        action = parts[2].strip().lower()
        if not request_ref or action not in {
            "chart",
            "news7",
            "news30",
            "retry",
            "report",
            "period3mo",
            "news_only",
            "set_monitor",
            "why_no_chart",
            "why_no_rsi",
        }:
            return None
        return request_ref, action

    @staticmethod
    def _parse_callback_guide(data: str) -> str | None:
        raw = (data or "").strip()
        if not raw.startswith("guide|"):
            return None
        parts = raw.split("|", 1)
        if len(parts) != 2:
            return None
        action = parts[1].strip().lower()
        if action not in {"analyze", "monitor", "help", "start"}:
            return None
        return action

    async def _handle_guide_action(self, *, chat_id: str, update_id: int, action: str) -> bool:
        if action == "analyze":
            await self._actions.send_error_message(
                chat_id=chat_id,
                text="示例：分析 TSLA 一个月走势；或 分析 0700.HK 近30天并给我K线。",
            )
        elif action == "monitor":
            await self._actions.send_error_message(
                chat_id=chat_id,
                text="示例：帮我盯 TSLA 每小时；或 /monitor TSLA 1h volatility。",
            )
        elif action == "help":
            await self._actions.handle_help(chat_id=chat_id)
        else:
            await self._actions.handle_general_conversation(chat_id=chat_id, intent="how_to_start")
        self._store.update_bot_update_status(
            update_id=update_id,
            status="processed",
            command=f"guide_{action}",
            request_id=None,
            error=None,
        )
        return True

    @staticmethod
    def _parse_text_confirm(text: str) -> tuple[str, str] | None:
        raw = (text or "").strip()
        match = re.match(r"^(yes|no|cancel|是|否)\s+([A-Za-z0-9\-]+)$", raw, flags=re.IGNORECASE)
        if not match:
            return None
        action = match.group(1).lower()
        if action == "是":
            action = "yes"
        elif action == "否":
            action = "no"
        request_ref = match.group(2).strip()
        if action not in {"yes", "no", "cancel"}:
            return None
        return action, request_ref

    async def _handle_confirmation(
        self,
        *,
        chat_id: str,
        update_id: int,
        action: str,
        request_ref: str,
    ) -> bool:
        pending = self._store.get_pending_confirm_by_ref(chat_id=chat_id, request_ref=request_ref)
        if pending is None:
            await self._actions.send_error_message(
                chat_id=chat_id,
                text="无法绑定确认请求 (Cannot bind confirmation). 请回复带 request_id 的确认消息。",
            )
            self._store.update_bot_update_status(
                update_id=update_id,
                status="failed",
                command="confirm_failed",
                error="request_binding_failed",
            )
            return True

        if pending.confirm_deadline_at:
            deadline = datetime.fromisoformat(pending.confirm_deadline_at)
            if self._current_ts() > deadline:
                self._store.set_nl_request_status(
                    request_id=pending.request_id,
                    to_status="rejected",
                    reject_reason="confirm_timeout",
                    confirm_deadline_at=None,
                )
                await self._actions.send_error_message(chat_id=chat_id, text="请求已过期，请重新发起 (Confirmation expired).")
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="processed",
                    command="confirm_timeout",
                    request_id=pending.request_id,
                    error=None,
                )
                self._store.record_metric(metric_name="nl_confirm_timeout_count", metric_value=1.0)
                return True

        if action in {"no", "cancel"}:
            self._store.set_nl_request_status(
                request_id=pending.request_id,
                to_status="rejected",
                reject_reason="cancelled",
                confirm_deadline_at=None,
            )
            await self._actions.send_error_message(chat_id=chat_id, text=f"请求已取消 (Cancelled): {pending.request_id}")
            self._store.update_bot_update_status(
                update_id=update_id,
                status="processed",
                command="confirm_cancelled",
                request_id=pending.request_id,
                error=None,
            )
            return True

        await self._execute_nl_request(update_id=update_id, request_id=pending.request_id)
        return True

    async def _handle_candidate_selection(
        self,
        *,
        chat_id: str,
        update_id: int,
        request_ref: str,
        chosen_symbol: str,
    ) -> bool:
        pending = self._store.get_pending_candidate_by_ref(chat_id=chat_id, request_ref=request_ref)
        if pending is None:
            await self._actions.send_error_message(
                chat_id=chat_id,
                text=(
                    "候选点选已过期或已完成。请重发请求。\n"
                    "示例：分析 0700.HK 近30天、分析 TSLA 近3个月。"
                ),
            )
            self._store.update_bot_update_status(
                update_id=update_id,
                status="processed",
                command="candidate_selection_expired",
                error=None,
            )
            return True
        if chosen_symbol not in pending.candidates:
            await self._actions.send_error_message(
                chat_id=chat_id,
                text="候选不匹配该请求，请重新点选对应按钮。",
            )
            self._store.update_bot_update_status(
                update_id=update_id,
                status="failed",
                command="candidate_selection_rejected",
                request_id=pending.request_id,
                error="candidate_not_in_request",
            )
            return True
        record = self._store.get_nl_request(request_id=pending.request_id)
        if record is None or record.status not in {"clarify_pending", "queued"}:
            self._store.mark_pending_candidate_selection(request_id=pending.request_id, status="rejected")
            await self._actions.send_error_message(chat_id=chat_id, text="该请求已结束，请重发。")
            return True
        slots = dict(record.slots)
        slots["symbol"] = chosen_symbol
        self._store.update_nl_request_slots(request_id=record.request_id, slots=slots)
        self._store.transition_nl_request_status(
            request_id=record.request_id,
            from_statuses=("clarify_pending", "queued"),
            to_status="queued",
            reject_reason=None,
            last_error=None,
        )
        self._store.mark_pending_candidate_selection(request_id=record.request_id, status="resolved")
        await self._actions.send_error_message(
            chat_id=chat_id,
            text=f"已选择标的 {chosen_symbol}，开始分析。request_id(short)={self._short_request_id(record.request_id)}",
        )
        await self._execute_nl_request(update_id=update_id, request_id=record.request_id)
        return True

    async def _handle_request_action(
        self,
        *,
        chat_id: str,
        update_id: int,
        request_ref: str,
        action: str,
    ) -> bool:
        record = self._store.get_nl_request_by_ref(chat_id=chat_id, request_ref=request_ref)
        if record is None:
            await self._actions.send_error_message(chat_id=chat_id, text="该请求不存在或已过期，请重发分析请求。")
            self._store.update_bot_update_status(
                update_id=update_id,
                status="processed",
                command="request_action_expired",
                error=None,
            )
            return True
        if record.intent != "analyze_snapshot":
            await self._actions.send_error_message(chat_id=chat_id, text="该请求不支持该操作。")
            self._store.update_bot_update_status(
                update_id=update_id,
                status="failed",
                command="request_action_rejected",
                request_id=record.request_id,
                error="unsupported_action_intent",
            )
            return True
        if record.status in {"executing", "pending_confirm", "clarify_pending"}:
            await self._actions.send_error_message(
                chat_id=chat_id,
                text=f"请求正在生成中，请稍候。request_id(short)={self._short_request_id(record.request_id)}",
            )
            self._store.update_bot_update_status(
                update_id=update_id,
                status="processed",
                command="request_action_pending",
                request_id=record.request_id,
                error=None,
            )
            return True
        if action == "report":
            await self._actions.handle_report(chat_id=chat_id, target_id=record.request_id, detail="full")
            self._store.update_bot_update_status(
                update_id=update_id,
                status="processed",
                command="request_action_report",
                request_id=record.request_id,
                error=None,
            )
            return True
        if action == "set_monitor":
            slots = dict(record.slots)
            await self._actions.handle_monitor(
                chat_id=chat_id,
                symbol=str(slots.get("symbol", "")).upper(),
                symbols=[str(slots.get("symbol", "")).upper()],
                interval_sec=3600,
                mode="anomaly",
                threshold=0.03,
                template="volatility",
                route_strategy="dual_channel",
            )
            self._store.update_bot_update_status(
                update_id=update_id,
                status="processed",
                command="request_action_set_monitor",
                request_id=record.request_id,
                error=None,
            )
            return True
        if action in {"why_no_chart", "why_no_rsi"}:
            chart_state_rec = self._store.get_request_chart_state(request_id=record.request_id)
            chart_state = chart_state_rec.chart_state if chart_state_rec else "none"
            metrics = record.slots if isinstance(record.slots, dict) else {}
            if action == "why_no_chart":
                reason = "当前未触发图表生成，你可以点击“📈K线”或“🔁重试”。"
                if chart_state == "failed":
                    reason = "最近一次图表生成失败，系统已保留文本分析与重试入口。"
                elif chart_state == "rendering":
                    reason = "图表仍在生成中，请稍候刷新。"
                await self._actions.send_error_message(
                    chat_id=chat_id,
                    text=f"为什么不给K线：{reason}\n证据: 图表状态={chart_state}",
                )
            else:
                reason = "本次缺少 RSI 所需价格序列，因此未展示 RSI 结论。"
                if str(metrics.get("need_chart", "")).strip().lower() in {"true", "1"}:
                    reason = "当前回复走了轻量模板，RSI 只在有完整指标时展示。"
                await self._actions.send_error_message(
                    chat_id=chat_id,
                    text=f"为什么不给RSI：{reason}\n证据: 图表状态={chart_state}",
                )
            self._store.update_bot_update_status(
                update_id=update_id,
                status="processed",
                command=f"request_action_{action}",
                request_id=record.request_id,
                error=None,
            )
            return True
        slots = dict(record.slots)
        need_chart = action in {"chart", "retry"}
        need_news = action in {"news7", "news30", "retry", "news_only"} or bool(slots.get("need_news", False))
        news_window_days = 30 if action == "news30" else 7
        period = "3mo" if action == "period3mo" else str(slots.get("period", "1mo")).lower()
        await self._actions.handle_analyze_snapshot(
            chat_id=chat_id,
            symbol=str(slots.get("symbol", "")).upper(),
            period=period,
            interval=str(slots.get("interval", "1d")).lower(),
            need_chart=need_chart,
            need_news=need_news,
            news_window_days=news_window_days,
            request_id=record.request_id,
        )
        self._store.update_bot_update_status(
            update_id=update_id,
            status="processed",
            command=f"request_action_{action}",
            request_id=record.request_id,
            error=None,
        )
        return True

    async def process_enqueued_update(self, *, update_id: int) -> bool:
        started_at = time.perf_counter()
        payload = self._store.get_bot_update_payload(update_id=update_id)
        if payload is None:
            return False

        message = payload.get("message") or {}
        callback_query = payload.get("callback_query") or {}
        chat = message.get("chat") or {}
        chat_id = str(chat.get("id", ""))
        chat_type = str(chat.get("type", "private"))
        from_user = message.get("from") or {}
        callback_from = callback_query.get("from") or {}
        effective_from = from_user or callback_from
        user_id = str(effective_from.get("id", "")) if effective_from.get("id") is not None else None
        username = str(from_user.get("username", "")).strip() or None
        text = str(message.get("text", "")).strip()
        callback_data = str(callback_query.get("data", "")).strip()

        print(f"[gateway] handling update={update_id} chat={_mask_chat_id(chat_id)}")
        self._store.record_metric(metric_name="command_total", metric_value=1.0, tags={"chat_id": chat_id})

        try:
            if self._is_chat_denied(chat_id=chat_id):
                denied = "chat is not allowlisted" if self._access_mode == "allowlist" else "chat is blocklisted"
                await self._actions.send_error_message(chat_id=chat_id, text=f"无权限 (Permission denied): {denied}")
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="failed",
                    command="source_denied",
                    error=denied,
                )
                self._store.add_audit_event(
                    event_type="source_denied",
                    chat_id=chat_id,
                    update_id=update_id,
                    action="reject",
                    reason=denied,
                )
                return True

            if self._gray_release_enabled and (not self._store.is_chat_allowlisted(chat_id=chat_id)):
                denied = "gray release active: chat not allowlisted"
                await self._actions.send_error_message(chat_id=chat_id, text=f"无权限 (Permission denied): {denied}")
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="failed",
                    command="gray_release_denied",
                    error=denied,
                )
                self._store.add_audit_event(
                    event_type="gray_release_denied",
                    chat_id=chat_id,
                    update_id=update_id,
                    action="reject",
                    reason=denied,
                )
                return True

            self._store.upsert_telegram_chat(chat_id=chat_id, user_id=user_id, username=username)

            if callback_data:
                guide_action = self._parse_callback_guide(callback_data)
                if guide_action is not None:
                    return await self._handle_guide_action(chat_id=chat_id, update_id=update_id, action=guide_action)
                candidate = self._parse_callback_candidate(callback_data)
                if candidate is not None:
                    request_ref, chosen_symbol = candidate
                    return await self._handle_candidate_selection(
                        chat_id=chat_id,
                        update_id=update_id,
                        request_ref=request_ref,
                        chosen_symbol=chosen_symbol,
                    )
                action_payload = self._parse_callback_action(callback_data)
                if action_payload is not None:
                    request_ref, action = action_payload
                    return await self._handle_request_action(
                        chat_id=chat_id,
                        update_id=update_id,
                        request_ref=request_ref,
                        action=action,
                    )
                confirm = self._parse_callback_confirm(callback_data)
                if confirm is None:
                    await self._actions.send_error_message(chat_id=chat_id, text="确认回调无效 (Invalid confirmation callback).")
                    self._store.update_bot_update_status(
                        update_id=update_id,
                        status="failed",
                        command="invalid_callback",
                        error="invalid_callback_data",
                    )
                    return True
                action, request_ref = confirm
                return await self._handle_confirmation(
                    chat_id=chat_id,
                    update_id=update_id,
                    action=action,
                    request_ref=request_ref,
                )

            within_limit, _ = self._store.check_and_increment_command_rate_limit(
                chat_id=chat_id,
                max_per_minute=self._limits.per_chat_per_minute,
            )
            if not within_limit:
                msg = f"限流触发 (Rate limit exceeded): max {self._limits.per_chat_per_minute} commands/minute."
                await self._actions.send_error_message(chat_id=chat_id, text=msg)
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="failed",
                    command="rate_limited",
                    error=msg,
                )
                self._store.add_audit_event(
                    event_type="rate_limited",
                    chat_id=chat_id,
                    update_id=update_id,
                    action="reject",
                    reason="per_chat_per_minute",
                )
                return True

            if text.startswith("/"):
                if text.split()[0].lower() == "/reset":
                    scope_key = self._conversation_scope_key(chat_id=chat_id, chat_type=chat_type, user_id=user_id)
                    self._store.reset_conversation_runtime_state(chat_id=chat_id, scope_key=scope_key)
                    await self._actions.send_error_message(
                        chat_id=chat_id,
                        text="已清空上下文：last_symbol_context、last_period_context、pending_candidate_selection、pending_confirm。",
                    )
                    self._store.update_bot_update_status(
                        update_id=update_id,
                        status="processed",
                        command="reset",
                        request_id=None,
                        error=None,
                    )
                    return True
                if text.split()[0].lower() == "/cancel":
                    if await self._cancel_executing_request(chat_id=chat_id, update_id=update_id):
                        return True
                    return await self._cancel_pending_confirm(chat_id=chat_id, update_id=update_id)
                parsed = parse_telegram_command(text)
                if isinstance(parsed, CommandError):
                    await self._actions.send_error_message(chat_id=chat_id, text=parsed.message)
                    self._store.update_bot_update_status(
                        update_id=update_id,
                        status="failed",
                        command="invalid_command",
                        error=parsed.message,
                    )
                    return True

                if parsed.name not in self._allowed_commands:
                    error = f"命令不在白名单 (Command not allowlisted): {parsed.name}"
                    await self._actions.send_error_message(chat_id=chat_id, text=error)
                    self._store.update_bot_update_status(
                        update_id=update_id,
                        status="failed",
                        command="command_denied",
                        error=error,
                    )
                    self._store.add_audit_event(
                        event_type="command_denied",
                        chat_id=chat_id,
                        update_id=update_id,
                        action=parsed.name,
                        reason="command_not_allowlisted",
                    )
                    return True

                if parsed.name == "help":
                    result = await self._actions.handle_help(chat_id=chat_id)
                elif parsed.name == "analyze":
                    result = await self._actions.handle_analyze(
                        update_id=update_id,
                        chat_id=chat_id,
                        symbol=parsed.args["symbol"],
                    )
                elif parsed.name == "monitor":
                    if self._store.is_degradation_active(state_key="no_monitor_push"):
                        reason = "monitor push paused due to SLO degradation"
                        await self._actions.send_error_message(chat_id=chat_id, text=reason)
                        self._store.add_audit_event(
                            event_type="degrade_skip",
                            chat_id=chat_id,
                            update_id=update_id,
                            action="monitor",
                            reason=reason,
                        )
                        result = type("ActionResult", (), {"command": "monitor_skipped", "request_id": None})()
                    else:
                        result = await self._actions.handle_monitor(
                            chat_id=chat_id,
                            symbol=parsed.args["symbol"],
                            symbols=[item for item in str(parsed.args.get("symbols_csv", parsed.args["symbol"])).split(",") if item],
                            interval_sec=int(parsed.args["interval_sec"]),
                            mode=str(parsed.args.get("mode", "anomaly")),
                            threshold=float(parsed.args.get("threshold", "0.03")),
                            template=str(parsed.args.get("template", "volatility")),
                            route_strategy=str(parsed.args.get("route_strategy", "dual_channel")),
                        )
                elif parsed.name == "list":
                    result = await self._actions.handle_list(chat_id=chat_id)
                elif parsed.name == "stop":
                    if str(parsed.args.get("target_type", "")) == "execution":
                        if await self._cancel_executing_request(chat_id=chat_id, update_id=update_id):
                            return True
                        await self._actions.send_error_message(chat_id=chat_id, text="当前无可取消的执行任务。")
                        self._store.update_bot_update_status(
                            update_id=update_id,
                            status="processed",
                            command="stop_execution_noop",
                            request_id=None,
                            error=None,
                        )
                        return True
                    result = await self._actions.handle_stop(
                        chat_id=chat_id,
                        target=parsed.args["target"],
                        target_type=parsed.args["target_type"],
                    )
                elif parsed.name == "report":
                    result = await self._actions.handle_report(
                        chat_id=chat_id,
                        target_id=parsed.args["target_id"],
                        detail=str(parsed.args.get("detail", "short")),
                    )
                elif parsed.name == "digest":
                    result = await self._actions.handle_digest(chat_id=chat_id, period=parsed.args["period"])
                elif parsed.name == "alerts":
                    result = await self._actions.handle_alerts(
                        chat_id=chat_id,
                        view=str(parsed.args["view"]),
                        limit=int(parsed.args["limit"]),
                    )
                elif parsed.name == "bulk":
                    result = await self._actions.handle_bulk(
                        chat_id=chat_id,
                        action=str(parsed.args["action"]),
                        target=str(parsed.args["target"]),
                        value=str(parsed.args.get("value", "")),
                    )
                elif parsed.name == "webhook":
                    result = await self._actions.handle_webhook(
                        chat_id=chat_id,
                        action=str(parsed.args["action"]),
                        url=str(parsed.args.get("url", "")),
                        secret=str(parsed.args.get("secret", "")),
                        webhook_id=str(parsed.args.get("webhook_id", "")),
                    )
                elif parsed.name == "pref":
                    result = await self._actions.handle_pref(
                        chat_id=chat_id,
                        setting=str(parsed.args["setting"]),
                        value=str(parsed.args["value"]),
                    )
                else:
                    raise ValueError(f"Unsupported route: {parsed.name}")

                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="processed",
                    command=result.command,
                    request_id=result.request_id,
                    error=None,
                )
                self._store.record_metric(metric_name="command_success", metric_value=1.0, tags={"command": parsed.name})
                return True

            text_confirm = self._parse_text_confirm(text)
            if text_confirm is not None:
                action, request_ref = text_confirm
                return await self._handle_confirmation(
                    chat_id=chat_id,
                    update_id=update_id,
                    action=action,
                    request_ref=request_ref,
                )

            pending = self._store.get_pending_confirm_request(chat_id=chat_id)
            if pending is not None:
                await self._actions.send_error_message(
                    chat_id=chat_id,
                    text=f"当前有待确认请求 {pending.request_id}，请先确认或 /cancel。",
                )
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="failed",
                    command="pending_confirm_conflict",
                    request_id=pending.request_id,
                    error="pending_confirm_exists",
                )
                return True

            within_nl_limit, _ = self._store.check_and_increment_command_rate_limit(
                chat_id=chat_id,
                max_per_minute=self._limits.nl_per_chat_per_minute,
                rate_scope="nl",
            )
            if not within_nl_limit:
                await self._actions.send_error_message(
                    chat_id=chat_id,
                    text=f"NL 限流触发 (NL rate limit exceeded): {self._limits.nl_per_chat_per_minute}/min",
                )
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="failed",
                    command="nl_rate_limited",
                    error="nl_per_chat_per_minute",
                )
                self._store.add_audit_event(
                    event_type="rate_limited",
                    chat_id=chat_id,
                    update_id=update_id,
                    action="nl",
                    reason="nl_per_chat_per_minute",
                )
                return True

            nl_quota_used = self._store.count_recent_nl_requests(chat_id=chat_id, since=self._current_ts() - timedelta(days=1))
            if nl_quota_used >= max(1, int(self._limits.nl_per_chat_per_day)):
                await self._actions.send_error_message(
                    chat_id=chat_id,
                    text=f"NL 配额已用尽 (NL quota exceeded): {self._limits.nl_per_chat_per_day}/24h",
                )
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="failed",
                    command="nl_quota_exceeded",
                    error="nl_per_chat_per_day",
                )
                self._store.add_audit_event(
                    event_type="quota_exceeded",
                    chat_id=chat_id,
                    update_id=update_id,
                    action="nl",
                    reason="nl_per_chat_per_day",
                    metadata={"used_24h": nl_quota_used},
                )
                return True

            normalized = sanitize_user_text(text)
            self._store.record_metric(metric_name="nl_intent_total", metric_value=1.0)
            if self._store.is_degradation_active(state_key="nl_command_hint_mode"):
                await self._send_fallback_help(chat_id=chat_id, reason="llm_degraded")
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="processed",
                    command="nl_fallback_help",
                    error=None,
                )
                self._store.add_audit_event(
                    event_type="degrade_skip",
                    chat_id=chat_id,
                    update_id=update_id,
                    action="nl",
                    reason="nl_command_hint_mode",
                )
                return True

            injection_reason = detect_prompt_injection_risk(normalized)
            if injection_reason is not None:
                self._store.record_metric(metric_name="nl_intent_reject", metric_value=1.0, tags={"reason": injection_reason})
                await self._send_nl_reject(chat_id=chat_id, reason=injection_reason, template="/help")
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="processed",
                    command="nl_rejected",
                    error=None,
                )
                self._store.add_audit_event(
                    event_type="nl_rejected",
                    chat_id=chat_id,
                    update_id=update_id,
                    action="nl",
                    reason=injection_reason,
                    metadata={"raw_text_hash": hash_text(text), "reject_reason": injection_reason, "confidence": 0.0},
                )
                return True

            clarify_pending, clarify_expired = self._store.get_clarify_pending_state(chat_id=chat_id)
            plan: NLUPlan | None = None
            if clarify_pending is not None:
                merged_slots, unresolved = self._resolve_clarify_followup(pending=clarify_pending, text=normalized)
                self._store.clear_clarify_pending(chat_id=chat_id)
                if clarify_expired:
                    self._store.set_nl_request_status(
                        request_id=clarify_pending.request_id,
                        to_status="rejected",
                        reject_reason="clarify_timeout",
                        last_error="clarify_pending_expired",
                    )
                    self._store.record_metric(metric_name="nl_intent_reject", metric_value=1.0, tags={"reason": "clarify_timeout"})
                    await self._actions.send_error_message(
                        chat_id=chat_id,
                        text=(
                            "澄清已超时，请重新发起 (Clarify timeout).\n"
                            f"可复制命令模板 (Command template): {clarify_pending.command_template}"
                        ),
                    )
                    self._store.update_bot_update_status(
                        update_id=update_id,
                        status="processed",
                        command="nl_clarify_timeout",
                        request_id=clarify_pending.request_id,
                        error=None,
                    )
                    return True
                if unresolved:
                    self._store.set_nl_request_status(
                        request_id=clarify_pending.request_id,
                        to_status="rejected",
                        reject_reason="clarify_failed",
                        last_error=f"missing_slots={','.join(unresolved)}",
                    )
                    self._store.record_metric(metric_name="nl_intent_reject", metric_value=1.0, tags={"reason": "clarify_failed"})
                    await self._actions.send_error_message(
                        chat_id=chat_id,
                        text=(
                            "澄清仍不完整，已拒绝执行 (Clarify incomplete).\n"
                            f"可复制命令模板 (Command template): {clarify_pending.command_template}"
                        ),
                    )
                    self._store.update_bot_update_status(
                        update_id=update_id,
                        status="processed",
                        command="nl_clarify_failed",
                        request_id=clarify_pending.request_id,
                        error=None,
                    )
                    return True
                self._store.record_metric(metric_name="nl_clarify_resolved_total", metric_value=1.0)
                plan = NLUPlan(
                    intent=clarify_pending.intent,
                    slots=merged_slots,
                    confidence=0.88,
                    risk_level="high" if self._is_high_risk_intent(clarify_pending.intent) else "low",
                    needs_confirm=self._is_high_risk_intent(clarify_pending.intent),
                    normalized_request=f"clarify_followup:{clarify_pending.intent}",
                    action_version=clarify_pending.action_version,
                    explain="clarify follow-up resolved",
                    command_template=clarify_pending.command_template,
                    schema_version=clarify_pending.schema_version,
                    plan_steps=self._default_plan_steps(clarify_pending.intent),
                )
            else:
                plan, parse_error = await self._parse_nlu_plan(text=normalized)
                if plan is None:
                    await self._send_fallback_help(chat_id=chat_id, reason="llm_parse_unavailable")
                    self._store.update_bot_update_status(
                        update_id=update_id,
                        status="processed",
                        command="nl_fallback_help",
                        error=parse_error,
                    )
                    self._store.add_audit_event(
                        event_type="degrade_skip",
                        chat_id=chat_id,
                        update_id=update_id,
                        action="nl",
                        reason="llm_parse_unavailable",
                        metadata={"raw_text_hash": hash_text(text)},
                    )
                    return True

            now = self._current_ts()
            scope_key = self._conversation_scope_key(chat_id=chat_id, chat_type=chat_type, user_id=user_id)
            plan, carry_symbol, carry_hit = self._inject_snapshot_defaults(
                plan=plan,
                normalized_text=normalized,
                scope_key=scope_key,
            )

            if await self._maybe_handle_snapshot_singleflight(
                chat_id=chat_id,
                update_id=update_id,
                scope_key=scope_key,
                plan=plan,
            ):
                return True

            if self._store.has_executing_nl_request(chat_id=chat_id) and not self._is_conversation_intent(plan.intent):
                await self._actions.send_error_message(
                    chat_id=chat_id,
                    text="正在执行，请稍后或使用 /status 查询 (Executing, please retry later).",
                )
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="failed",
                    command="nl_executing_conflict",
                    error="nl_executing",
                )
                return True

            request_id = f"nlr-{uuid4().hex[:10]}"
            text_key, intent_key = self._build_dedupe_keys(
                chat_id=chat_id,
                normalized_text=normalized,
                intent=plan.intent,
                slots=plan.slots,
                now=now,
            )
            self._store.create_nl_request(
                request_id=request_id,
                update_id=update_id,
                chat_id=chat_id,
                intent=plan.intent,
                slots={
                    **plan.slots,
                    **({"_plan_steps": plan.plan_steps} if plan.plan_steps else {}),
                    "_schema_version": plan.schema_version,
                },
                confidence=plan.confidence,
                needs_confirm=plan.needs_confirm,
                status="queued",
                text_dedupe_key=text_key,
                intent_dedupe_key=intent_key,
                normalized_text=hash_text(normalized),
                normalized_request=plan.normalized_request,
                action_version=plan.action_version,
                risk_level=plan.risk_level,
                raw_text_hash=hash_text(text),
                intent_candidate=plan.intent,
                reject_reason=plan.reject_reason,
            )

            duplicate = False
            duplicate_request_id: str | None = None
            if not self._is_conversation_intent(plan.intent):
                duplicate, duplicate_request_id = self._store.find_recent_nl_duplicates(
                    chat_id=chat_id,
                    text_dedupe_key=text_key,
                    intent_dedupe_key=intent_key,
                    intent=plan.intent,
                    current_request_id=request_id,
                )
            if duplicate:
                self._store.set_nl_request_status(
                    request_id=request_id,
                    to_status="rejected",
                    reject_reason="duplicate_request",
                    last_error=f"duplicate_of={duplicate_request_id}",
                )
                self._store.record_metric(metric_name="nl_dedupe_suppressed_count", metric_value=1.0, tags={"intent": plan.intent})
                duplicate_record = (
                    self._store.get_nl_request(request_id=duplicate_request_id)
                    if duplicate_request_id
                    else None
                )
                if duplicate_record is not None and duplicate_record.status == "completed":
                    report = self._store.get_analysis_report(report_id=duplicate_record.request_id, chat_id=chat_id)
                    run_hint = f"run_id={report.run_id}" if report else "run_id=N/A"
                    chart_state_record = self._store.get_request_chart_state(request_id=duplicate_record.request_id)
                    chart_state = chart_state_record.chart_state if chart_state_record else "none"
                    await self._actions.send_inline_buttons(
                        chat_id=chat_id,
                        text=(
                            "30秒内重复请求已合并：已生成。\n"
                            f"request_id(short)={self._short_request_id(duplicate_record.request_id)} {run_hint}"
                        ),
                        buttons=self._actions.build_snapshot_buttons(
                            request_id=duplicate_record.request_id,
                            chart_state=chart_state,
                        ),
                    )
                else:
                    pending_hint = duplicate_request_id or "n/a"
                    await self._actions.send_error_message(
                        chat_id=chat_id,
                        text=f"30秒内重复请求已合并：生成中。request_id(short)={self._short_request_id(pending_hint)}",
                    )
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="processed",
                    command="nl_deduped",
                    request_id=request_id,
                    error=None,
                )
                return True

            if plan.reject_reason == "clarify_needed" and plan.clarify_slot:
                self._store.record_metric(metric_name="nl_clarify_asked_total", metric_value=1.0, tags={"slot": plan.clarify_slot})
                self._store.set_nl_request_status(
                    request_id=request_id,
                    to_status="clarify_pending",
                    reject_reason="clarify_needed",
                    last_error=None,
                )
                clarify_slots = [slot for slot in (plan.clarify_slots_needed or [plan.clarify_slot]) if slot in self._CLARIFY_SLOT_WHITELIST]
                self._store.upsert_clarify_pending(
                    chat_id=chat_id,
                    request_id=request_id,
                    intent=plan.intent,
                    slots=plan.slots,
                    missing_slots=clarify_slots,
                    command_template=plan.command_template,
                    action_version=plan.action_version,
                    schema_version=plan.schema_version,
                    ttl_seconds=300,
                )
                clarify_question = plan.clarify_question or "请补充必要参数。"
                await self._actions.send_error_message(
                    chat_id=chat_id,
                    text=(
                        f"{clarify_question}\n"
                        "请在 5 分钟内补充一次 (one follow-up within 5 minutes).\n"
                        f"可复制命令模板 (Command template): {plan.command_template}"
                    ),
                )
                self._store.add_audit_event(
                    event_type="nl_clarify_pending",
                    chat_id=chat_id,
                    update_id=update_id,
                    action=plan.intent,
                    reason="clarify_needed",
                    metadata={
                        "raw_text_hash": hash_text(text),
                        "intent_candidate": plan.intent,
                        "reject_reason": "clarify_needed",
                        "clarify_slot": plan.clarify_slot,
                        "confidence": plan.confidence,
                    },
                )
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="processed",
                    command="nl_clarify_pending",
                    request_id=request_id,
                    error=None,
                )
                return True

            if plan.reject_reason == "candidate_selection_needed":
                self._store.set_nl_request_status(
                    request_id=request_id,
                    to_status="clarify_pending",
                    reject_reason="candidate_selection_needed",
                    last_error="candidate_selection_needed",
                )
                candidates = [str(item).upper() for item in plan.slots.get("_candidate_symbols", []) if isinstance(item, str)]
                self._store.upsert_pending_candidate_selection(
                    request_id=request_id,
                    chat_id=chat_id,
                    scope_key=scope_key,
                    candidates=candidates,
                    command_template=plan.command_template,
                    ttl_seconds=300,
                )
                alias = str(plan.slots.get("_candidate_alias", "该标的")).strip()
                buttons = [[(item, f"pick|{request_id[-6:]}|{item}")] for item in candidates[:4]]
                await self._actions.send_inline_buttons(
                    chat_id=chat_id,
                    text=(
                        f"标的 `{alias}` 命中多个候选，请在 5 分钟内点选按钮：\n"
                        f"request_id(short)={self._short_request_id(request_id)}"
                    ),
                    buttons=buttons,
                )
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="processed",
                    command="nl_candidate_selection",
                    request_id=request_id,
                    error=None,
                )
                return True

            if plan.reject_reason == "unknown_symbol":
                self._store.set_nl_request_status(
                    request_id=request_id,
                    to_status="rejected",
                    reject_reason="unknown_symbol",
                    last_error=plan.explain,
                )
                await self._actions.send_error_message(
                    chat_id=chat_id,
                    text=(
                        "未识别该标的，已拒绝执行以避免误分析。\n"
                        "请使用示例：`600519.SH`、`0700.HK`、`TSLA`。"
                    ),
                )
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="processed",
                    command="nl_rejected_unknown_symbol",
                    request_id=request_id,
                    error=None,
                )
                return True

            if plan.confidence < 0.75 or plan.reject_reason is not None:
                reject_reason = plan.reject_reason or "low_confidence"
                self._store.set_nl_request_status(
                    request_id=request_id,
                    to_status="rejected",
                    reject_reason=reject_reason,
                    last_error=plan.explain,
                )
                self._store.add_audit_event(
                    event_type="nl_rejected",
                    chat_id=chat_id,
                    update_id=update_id,
                    action=plan.intent,
                    reason=reject_reason,
                    metadata={
                        "raw_text_hash": hash_text(text),
                        "intent_candidate": plan.intent,
                        "reject_reason": reject_reason,
                        "confidence": plan.confidence,
                    },
                )
                self._store.record_metric(metric_name="nl_intent_reject", metric_value=1.0, tags={"reason": reject_reason})
                await self._send_nl_reject(chat_id=chat_id, reason=reject_reason, template=plan.command_template)
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="processed",
                    command="nl_rejected",
                    request_id=request_id,
                    error=None,
                )
                return True

            needs_confirm = self._is_high_risk_intent(plan.intent) or bool(plan.needs_confirm)
            if needs_confirm:
                deadline = (now + timedelta(minutes=5)).isoformat()
                self._store.transition_nl_request_status(
                    request_id=request_id,
                    from_statuses=("queued",),
                    to_status="pending_confirm",
                    reject_reason=None,
                    last_error=None,
                    confirm_deadline_at=deadline,
                )
                await self._send_pending_confirm_prompt(chat_id=chat_id, request_id=request_id)
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="processed",
                    command="nl_pending_confirm",
                    request_id=request_id,
                    error=None,
                )
                return True

            await self._execute_nl_request(update_id=update_id, request_id=request_id)
            if carry_hit and carry_symbol:
                await self._actions.send_error_message(
                    chat_id=chat_id,
                    text=f"默认沿用标的：{carry_symbol}。如需切换请回复：换标的 TSLA。",
                )
                self._store.record_metric(metric_name="symbol_carry_over_hit_rate", metric_value=1.0)
            return True
        except Exception as exc:
            self._store.update_bot_update_status(
                update_id=update_id,
                status="failed",
                command=None,
                request_id=None,
                error=str(exc),
            )
            raise
        finally:
            latency_ms = (time.perf_counter() - started_at) * 1000
            self._store.record_metric(metric_name="command_latency_ms", metric_value=latency_ms)
            if chat_id:
                scope_key = self._conversation_scope_key(chat_id=chat_id, chat_type=chat_type, user_id=user_id)
                try:
                    self._compact_conversation_history_if_needed(chat_id=chat_id, scope_key=scope_key, update_id=update_id)
                except Exception:
                    self._store.record_metric(metric_name="conversation_archive_failed_total", metric_value=1.0)

    async def process_pending_updates(self, *, limit: int = 100) -> int:
        pending_update_ids = self._store.list_pending_bot_update_ids(limit=limit)
        handled = 0
        for update_id in pending_update_ids:
            if await self.process_enqueued_update(update_id=update_id):
                handled += 1
        handled += await self._actions.process_due_analysis_recovery(limit=limit)
        return handled

    async def process_updates(self, updates: list[dict[str, Any]]) -> int:
        handled = 0
        for update in updates:
            if await self.process_update(update):
                handled += 1
            update_id = int(update.get("update_id", 0))
            if update_id > 0:
                self._offset = max(self._offset, update_id + 1)
        return handled

    async def run_long_polling(
        self,
        *,
        bot_token: str,
        poll_timeout_seconds: int = 20,
        idle_sleep_seconds: float = 0.5,
    ) -> None:
        url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
        async with aiohttp.ClientSession() as session:
            while True:
                payload = {
                    "timeout": poll_timeout_seconds,
                    "offset": self._offset,
                    "allowed_updates": ["message", "callback_query"],
                }
                try:
                    async with session.post(url, json=payload, timeout=poll_timeout_seconds + 5) as response:
                        data = await response.json(content_type=None)
                        if response.status >= 400 or not data.get("ok"):
                            await self.process_pending_updates(limit=100)
                            await asyncio.sleep(idle_sleep_seconds)
                            continue
                        updates = data.get("result", [])
                        if not isinstance(updates, list):
                            await self.process_pending_updates(limit=100)
                            await asyncio.sleep(idle_sleep_seconds)
                            continue
                        await self.process_updates([item for item in updates if isinstance(item, dict)])
                        await self.process_pending_updates(limit=100)
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    await self.process_pending_updates(limit=100)
                await asyncio.sleep(idle_sleep_seconds)
