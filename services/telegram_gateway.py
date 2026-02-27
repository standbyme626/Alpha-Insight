from __future__ import annotations

import asyncio
import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import aiohttp

from agents.telegram_command_router import CommandError, parse_telegram_command
from agents.telegram_nlu_planner import NLUPlan, hash_text, normalize_text, plan_from_text
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
        text_key = f"{chat_id}:{normalized_text.lower()}:{bucket}"
        slots_key = json.dumps(slots, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        intent_key = f"{chat_id}:{intent}:{slots_key}:{bucket}"
        return text_key, intent_key

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

    async def _execute_nl_request(self, *, update_id: int, request_id: str) -> bool:
        record = self._store.get_nl_request(request_id=request_id)
        if record is None:
            return False
        if record.intent not in {"create_monitor", "analyze_snapshot"}:
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
        slots = record.slots
        try:
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
            else:
                await self._actions.handle_analyze_snapshot(
                    chat_id=record.chat_id,
                    symbol=str(slots.get("symbol", "")).upper(),
                    period=str(slots.get("period", "1mo")).lower(),
                    interval=str(slots.get("interval", "1d")).lower(),
                    need_chart=bool(slots.get("need_chart", False)),
                    need_news=bool(slots.get("need_news", False)),
                    request_id=record.request_id,
                )
            self._store.set_nl_request_status(
                request_id=request_id,
                to_status="completed",
                reject_reason=None,
                last_error=None,
                confirm_deadline_at=None,
            )
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
            self._store.update_bot_update_status(
                update_id=update_id,
                status="failed",
                command=f"nl_{record.intent}",
                request_id=request_id,
                error=str(exc),
            )
            raise

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

            plan: NLUPlan = plan_from_text(text)
            now = self._current_ts()
            request_id = f"nlr-{uuid4().hex[:10]}"
            normalized = normalize_text(text)
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
                slots=plan.slots,
                confidence=plan.confidence,
                needs_confirm=plan.needs_confirm,
                status="queued",
                text_dedupe_key=text_key,
                intent_dedupe_key=intent_key,
                normalized_text=normalized,
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
