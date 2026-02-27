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
    hash_text,
    plan_from_text,
    sanitize_user_text,
)
from services.runtime_controls import RuntimeLimits
from services.telegram_actions import TelegramActions
from services.telegram_store import TelegramTaskStore


def _mask_chat_id(chat_id: str) -> str:
    if len(chat_id) <= 4:
        return "***"
    return f"{chat_id[:2]}***{chat_id[-2:]}"


class TelegramGateway:
    def __init__(
        self,
        *,
        store: TelegramTaskStore,
        actions: TelegramActions,
        limits: RuntimeLimits | None = None,
        allowed_chat_ids: set[str] | None = None,
        allowed_commands: set[str] | None = None,
        gray_release_enabled: bool = False,
        nlu_parser: Callable[[str], NLUPlan] = plan_from_text,
    ):
        self._store = store
        self._actions = actions
        self._limits = limits or RuntimeLimits()
        self._allowed_chat_ids = allowed_chat_ids or set()
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
                "chat": {"id": str(effective_chat.get("id", ""))},
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
        for _ in range(attempts):
            started = time.perf_counter()
            self._store.record_metric(metric_name="llm_parse_total", metric_value=1.0)
            try:
                plan = await asyncio.wait_for(
                    asyncio.to_thread(self._nlu_parser, text),
                    timeout=float(self._limits.nl_parse_timeout_seconds),
                )
                latency_ms = (time.perf_counter() - started) * 1000
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
            want_chart = bool(slots.get("need_chart", False))
            fail_before = self._store.count_metric_events(metric_name="chart_render_fail_rate") if want_chart else 0
            if want_chart:
                self._store.record_metric(metric_name="chart_render_attempt_total", metric_value=1.0)
                if self._store.is_degradation_active(state_key="chart_text_only"):
                    slots = dict(slots)
                    slots["need_chart"] = False
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
        if record.intent not in {"create_monitor", "analyze_snapshot", "list_jobs", "stop_job", "daily_digest"}:
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

    async def process_enqueued_update(self, *, update_id: int) -> bool:
        started_at = time.perf_counter()
        payload = self._store.get_bot_update_payload(update_id=update_id)
        if payload is None:
            return False

        message = payload.get("message") or {}
        callback_query = payload.get("callback_query") or {}
        chat = message.get("chat") or {}
        chat_id = str(chat.get("id", ""))
        from_user = message.get("from") or {}
        user_id = str(from_user.get("id", "")) if from_user.get("id") is not None else None
        username = str(from_user.get("username", "")).strip() or None
        text = str(message.get("text", "")).strip()
        callback_data = str(callback_query.get("data", "")).strip()

        print(f"[gateway] handling update={update_id} chat={_mask_chat_id(chat_id)}")
        self._store.record_metric(metric_name="command_total", metric_value=1.0, tags={"chat_id": chat_id})

        try:
            if self._allowed_chat_ids and chat_id not in self._allowed_chat_ids:
                denied = "chat is not allowlisted"
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
                if text.split()[0].lower() == "/cancel":
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

            if self._store.has_executing_nl_request(chat_id=chat_id):
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
                await self._actions.send_error_message(
                    chat_id=chat_id,
                    text=f"重复请求已抑制 (Deduped). existing_request_id={duplicate_request_id}",
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
                    to_status="rejected",
                    reject_reason="clarify_failed",
                    last_error=plan.explain,
                )
                clarify_question = plan.clarify_question or "请补充必要参数。"
                await self._actions.send_error_message(
                    chat_id=chat_id,
                    text=(
                        f"{clarify_question}\n"
                        f"仅支持一次澄清 (single clarify).\n"
                        f"可复制命令模板 (Command template): {plan.command_template}"
                    ),
                )
                self._store.add_audit_event(
                    event_type="nl_rejected",
                    chat_id=chat_id,
                    update_id=update_id,
                    action=plan.intent,
                    reason="clarify_failed",
                    metadata={
                        "raw_text_hash": hash_text(text),
                        "intent_candidate": plan.intent,
                        "reject_reason": "clarify_failed",
                        "clarify_slot": plan.clarify_slot,
                        "confidence": plan.confidence,
                    },
                )
                self._store.record_metric(metric_name="nl_intent_reject", metric_value=1.0, tags={"reason": "clarify_failed"})
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="processed",
                    command="nl_clarify_once",
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
                await asyncio.sleep(idle_sleep_seconds)
