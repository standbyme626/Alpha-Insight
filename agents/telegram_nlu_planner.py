from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

from core.strategy_tier import ALLOWED_STRATEGY_TIERS, DEFAULT_STRATEGY_TIER, normalize_strategy_tier
from tools.market_data import normalize_market_symbol

_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.\-]{1,12}$")
_INTERVAL_PATTERN = re.compile(r"^(\d+)([smhd])$", re.IGNORECASE)
_ALLOWED_INTERVALS = {"1m", "5m", "15m", "30m", "1h", "4h", "1d", "24h"}
_ALLOWED_ANALYZE_PERIODS = {"5d", "1mo", "3mo", "6mo", "1y"}
_ALLOWED_TEMPLATES = {"volatility", "price", "rsi"}
_ALLOWED_ROUTE_STRATEGIES = {"telegram_only", "webhook_only", "dual_channel", "email_only", "wecom_only", "multi_channel"}
_ALLOWED_CLARIFY_SLOTS = {"symbol", "period", "interval", "template", "market"}
_GENERAL_CONVERSATION_INTENTS = {"greeting", "capability", "help", "how_to_start"}
_MAX_NL_TEXT_CHARS = 800
_PROMPT_INJECTION_PATTERNS = [
    re.compile(r"ignore\s+(all|the)\s+(previous|prior|above)\s+(instructions?|prompts?)", re.IGNORECASE),
    re.compile(r"(system|developer)\s*[:：]\s*", re.IGNORECASE),
    re.compile(r"you\s+are\s+chatgpt", re.IGNORECASE),
    re.compile(r"(reveal|show|print).{0,24}(system prompt|hidden prompt|developer message)", re.IGNORECASE),
    re.compile(r"</?system>", re.IGNORECASE),
]

_TEMPLATE_MODE_THRESHOLD = {
    "volatility": ("anomaly", 0.03),
    "price": ("price_breakout", 0.02),
    "rsi": ("rsi_extreme", 70.0),
}


@dataclass
class NLUPlan:
    intent: str
    slots: dict[str, Any]
    confidence: float
    risk_level: str
    needs_confirm: bool
    normalized_request: str
    action_version: str
    explain: str
    reject_reason: str | None = None
    command_template: str = (
        "/monitor <symbol> <interval> [volatility|price|rsi] "
        "[telegram_only|webhook_only|dual_channel|email_only|wecom_only|multi_channel] "
        "[research-only|alert-only|execution-ready]"
    )
    schema_version: str = "telegram_nlu_plan_v2"
    plan_steps: list[dict[str, Any]] | None = None
    clarify_slot: str | None = None
    clarify_question: str | None = None
    clarify_slots_needed: list[str] | None = None


def normalize_text(text: str) -> str:
    return " ".join((text or "").strip().split())


def sanitize_user_text(text: str) -> str:
    compact = "".join(ch for ch in (text or "") if ch.isprintable())
    normalized = normalize_text(compact)
    return normalized[:_MAX_NL_TEXT_CHARS]


def detect_prompt_injection_risk(text: str) -> str | None:
    value = text or ""
    if not value:
        return "empty_input"
    for pattern in _PROMPT_INJECTION_PATTERNS:
        if pattern.search(value):
            return "prompt_injection_pattern"
    return None


def hash_text(text: str) -> str:
    return hashlib.sha256((text or "").strip().encode("utf-8")).hexdigest()


def parse_interval_to_seconds(raw: str) -> int | None:
    value = (raw or "").strip().lower()
    if value == "24h":
        return 86400
    match = _INTERVAL_PATTERN.fullmatch(value)
    if not match:
        return None
    amount = int(match.group(1))
    if amount <= 0:
        return None
    unit = match.group(2).lower()
    factor = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
    seconds = amount * factor
    if seconds < 60 or seconds > 86400:
        return None
    return seconds


def _extract_symbol(text: str) -> str | None:
    lowered = text.lower()
    alias_map = {
        "腾讯": "0700.HK",
        "tencent": "0700.HK",
    }
    for alias, symbol in alias_map.items():
        if alias in lowered:
            return symbol

    tokens = re.findall(r"[A-Za-z0-9.\-]{1,12}", text)
    for token in tokens:
        if token.isdigit():
            continue
        # Avoid treating chart keywords like "k线" as ticker K.
        if token.lower() in {"k"}:
            continue
        normalized = normalize_market_symbol(token, market="auto")
        if normalized and _SYMBOL_PATTERN.fullmatch(normalized):
            return normalized
    return None


def _extract_interval(text: str) -> str | None:
    lowered = text.lower()
    if "每小时" in lowered or "每 小时" in lowered or "hourly" in lowered:
        return "1h"
    if "每天" in lowered or "daily" in lowered:
        return "1d"
    if "24h" in lowered:
        return "24h"
    match = re.search(r"\b(\d+[smhd])\b", lowered)
    if match:
        return match.group(1)
    return None


def _extract_template(text: str) -> str:
    lowered = text.lower()
    if "rsi" in lowered:
        return "rsi"
    if "price" in lowered or "突破" in lowered:
        return "price"
    return "volatility"


def _extract_route_strategy(text: str) -> str:
    lowered = text.lower()
    if "telegram_only" in lowered:
        return "telegram_only"
    if "email_only" in lowered:
        return "email_only"
    if "wecom_only" in lowered:
        return "wecom_only"
    if "multi_channel" in lowered:
        return "multi_channel"
    if "webhook_only" in lowered:
        return "webhook_only"
    return "dual_channel"


def _extract_strategy_tier(text: str) -> str:
    lowered = text.lower()
    if "research-only" in lowered or "research only" in lowered:
        return "research-only"
    if "alert-only" in lowered or "alert only" in lowered:
        return "alert-only"
    if "execution-ready" in lowered or "execution ready" in lowered:
        return "execution-ready"
    return DEFAULT_STRATEGY_TIER


def _intent_from_text(text: str) -> str:
    lowered = text.lower()
    conversation_intent = detect_general_conversation_intent(text)
    if conversation_intent is not None:
        return conversation_intent
    if any(keyword in lowered for keyword in ("digest", "日报", "daily digest", "每日报告", "日报告")):
        return "daily_digest"
    if any(keyword in lowered for keyword in ("list", "列表", "任务列表", "监控列表", "有哪些监控")):
        return "list_jobs"
    if any(keyword in lowered for keyword in ("stop", "停止", "取消监控", "停掉", "关掉", "删除监控")):
        return "stop_job"
    if any(keyword in lowered for keyword in ("monitor", "盯", "监控", "提醒", "watch")):
        return "create_monitor"
    if any(keyword in lowered for keyword in ("analyze", "analysis", "分析", "涨跌", "走势", "看", "snapshot")):
        return "analyze_snapshot"
    if "bulk" in lowered or "批量" in lowered:
        return "bulk_change"
    return "unknown"


def detect_general_conversation_intent(text: str) -> str | None:
    normalized = normalize_text(text)
    lowered = normalized.lower()
    if not lowered:
        return None

    if any(
        keyword in lowered
        for keyword in (
            "你会什么",
            "你可以做什么",
            "可以做什么",
            "都能做什么",
            "都可以做什么",
            "你能帮我什么",
            "你可以帮我什么",
            "有哪些功能",
            "功能有哪些",
            "能做什么",
            "能力",
            "capability",
            "what can you do",
        )
    ):
        return "capability"
    if any(keyword in lowered for keyword in ("怎么开始", "如何开始", "新手", "从哪开始", "how to start", "start guide")):
        return "how_to_start"
    if any(
        keyword in lowered
        for keyword in ("怎么用", "如何用", "help", "帮助", "指令", "命令", "使用说明", "怎么使用", "如何使用")
    ):
        return "help"
    if normalized in {"你好", "您好", "hi", "hello", "嗨", "在吗", "在嗎", "哈喽", "哈囉"}:
        return "greeting"
    if len(normalized) <= 16 and any(token in lowered for token in ("你好", "您好", "hi", "hello", "嗨")):
        return "greeting"
    return None


def _extract_period(text: str) -> str:
    lowered = text.lower()
    if any(item in lowered for item in ("一年", "1年", "一年期", "1y")):
        return "1y"
    if any(item in lowered for item in ("半年", "6个月", "6月", "6mo")):
        return "6mo"
    if any(item in lowered for item in ("三个月", "3个月", "三月", "3mo")):
        return "3mo"
    if any(item in lowered for item in ("一周", "1周", "一星期", "week", "weekly")):
        return "5d"
    return "1mo"


def _extract_need_chart(text: str) -> bool:
    lowered = text.lower()
    return any(item in lowered for item in ("图", "k线", "k 線", "chart", "图片", "image"))


def _extract_need_news(text: str) -> bool:
    lowered = text.lower()
    if any(item in lowered for item in ("不要新闻", "无需新闻", "no news", "without news")):
        return False
    return any(item in lowered for item in ("新闻", "news", "综合分析", "研报", "情绪"))


def _extract_stop_target(text: str) -> tuple[str | None, str]:
    lowered = text.lower()
    job_match = re.search(r"\b(job-[a-z0-9\-]+)\b", lowered)
    if job_match:
        return job_match.group(1), "job_id"
    symbol = _extract_symbol(text)
    if symbol:
        return symbol, "symbol"
    return None, "symbol"


def _clarify(plan: NLUPlan, *, slot: str, question: str) -> NLUPlan:
    if slot not in _ALLOWED_CLARIFY_SLOTS:
        plan.reject_reason = "invalid_slot"
        plan.explain = f"unsupported clarify slot: {slot}"
        return plan
    plan.reject_reason = "clarify_needed"
    plan.clarify_slot = slot
    plan.clarify_question = question
    plan.clarify_slots_needed = [slot]
    plan.explain = f"clarify {slot}"
    return plan


def extract_clarify_slots(text: str) -> dict[str, Any]:
    normalized = normalize_text(text)
    slots: dict[str, Any] = {}

    symbol = _extract_symbol(normalized)
    if symbol:
        slots["symbol"] = symbol

    interval = _extract_interval(normalized)
    if interval:
        slots["interval"] = interval.lower()

    period = _extract_period(normalized)
    if period:
        slots["period"] = period.lower()

    template = _extract_template(normalized)
    if template in _ALLOWED_TEMPLATES:
        slots["template"] = template

    market_match = re.search(r"\b(us|hk|cn|a股|港股|美股)\b", normalized.lower())
    if market_match:
        raw_market = market_match.group(1)
        market = {"a股": "cn", "港股": "hk", "美股": "us"}.get(raw_market, raw_market)
        slots["market"] = market

    return {key: value for key, value in slots.items() if key in _ALLOWED_CLARIFY_SLOTS}


def _build_plan_steps(intent: str, *, need_chart: bool = False) -> list[dict[str, Any]]:
    steps: list[dict[str, Any]] = [
        {"step": "validate_slots", "action": "validate_slots", "status": "pending"},
        {"step": "execute_intent", "action": "execute_intent", "status": "pending"},
    ]
    if intent in _GENERAL_CONVERSATION_INTENTS:
        steps.insert(1, {"step": "render_capability_card", "action": "render_capability_card", "status": "pending"})
    if intent == "daily_digest":
        steps.insert(1, {"step": "build_digest", "action": "build_digest", "status": "pending"})
    if intent == "list_jobs":
        steps.insert(1, {"step": "list_jobs", "action": "list_jobs", "status": "pending"})
    if intent == "analyze_snapshot" and need_chart:
        steps.append({"step": "render_chart", "action": "render_chart", "status": "pending"})
    steps.append({"step": "render_response", "action": "render_response", "status": "pending"})
    return steps


def _validate_slots(plan: NLUPlan) -> NLUPlan:
    if plan.intent not in {
        "create_monitor",
        "stop_job",
        "bulk_change",
        "analyze_snapshot",
        "list_jobs",
        "daily_digest",
        *_GENERAL_CONVERSATION_INTENTS,
    }:
        plan.reject_reason = "low_confidence"
        plan.explain = "intent not supported in v2"
        return plan

    if plan.intent in _GENERAL_CONVERSATION_INTENTS:
        return plan

    if plan.intent == "list_jobs":
        return plan

    if plan.intent == "daily_digest":
        period = str(plan.slots.get("period", "daily")).lower()
        if period != "daily":
            plan.reject_reason = "invalid_slot"
            plan.explain = "invalid digest period"
        return plan

    if plan.intent == "analyze_snapshot":
        symbol = str(plan.slots.get("symbol", "")).upper()
        period = str(plan.slots.get("period", "")).lower()
        interval = str(plan.slots.get("interval", "")).lower()
        if not symbol:
            return _clarify(
                plan,
                slot="symbol",
                question="请补充标的 (symbol)，例如：TSLA。",
            )
        if not symbol or not _SYMBOL_PATTERN.fullmatch(symbol):
            plan.reject_reason = "invalid_slot"
            plan.explain = "invalid symbol"
            return plan
        if period not in _ALLOWED_ANALYZE_PERIODS:
            plan.reject_reason = "invalid_slot"
            plan.explain = "invalid period"
            return plan
        if interval not in _ALLOWED_INTERVALS or parse_interval_to_seconds(interval) is None:
            plan.reject_reason = "invalid_slot"
            plan.explain = "invalid interval"
            return plan
        return plan

    if plan.intent == "stop_job":
        target = str(plan.slots.get("target", "")).strip()
        target_type = str(plan.slots.get("target_type", "symbol")).lower()
        if not target:
            return _clarify(
                plan,
                slot="symbol",
                question="请补充要停止的 symbol 或 job_id，例如：TSLA 或 job-abc123。",
            )
        if target_type == "symbol":
            if not _SYMBOL_PATTERN.fullmatch(target.upper()):
                plan.reject_reason = "invalid_slot"
                plan.explain = "invalid stop target symbol"
                return plan
            plan.slots["target"] = target.upper()
        return plan

    if plan.intent != "create_monitor":
        return plan

    symbol = str(plan.slots.get("symbol", "")).upper()
    interval = str(plan.slots.get("interval", "")).lower()
    template = str(plan.slots.get("template", "")).lower()
    route_strategy = str(plan.slots.get("route_strategy", "")).lower()
    strategy_tier = normalize_strategy_tier(str(plan.slots.get("strategy_tier", DEFAULT_STRATEGY_TIER)))

    if not symbol:
        return _clarify(
            plan,
            slot="symbol",
            question="请补充监控标的 (symbol)，例如：TSLA。",
        )
    if not _SYMBOL_PATTERN.fullmatch(symbol):
        plan.reject_reason = "invalid_slot"
        plan.explain = "invalid symbol"
        return plan
    if not interval:
        return _clarify(
            plan,
            slot="interval",
            question="请补充监控周期 (interval)：5m / 1h / 1d。",
        )
    if interval not in _ALLOWED_INTERVALS or parse_interval_to_seconds(interval) is None:
        plan.reject_reason = "invalid_slot"
        plan.explain = "invalid interval"
        return plan
    if template not in _ALLOWED_TEMPLATES:
        plan.reject_reason = "invalid_slot"
        plan.explain = "invalid template"
        return plan
    if route_strategy not in _ALLOWED_ROUTE_STRATEGIES:
        plan.reject_reason = "unsafe_route"
        plan.explain = "invalid route_strategy"
        return plan
    if strategy_tier not in ALLOWED_STRATEGY_TIERS:
        plan.reject_reason = "invalid_slot"
        plan.explain = "invalid strategy_tier"
        return plan
    plan.slots["strategy_tier"] = strategy_tier

    return plan


def plan_from_text(text: str) -> NLUPlan:
    normalized = normalize_text(text)
    intent = _intent_from_text(normalized)
    action_version = "v2"
    if intent == "analyze_snapshot":
        symbol = _extract_symbol(normalized)
        period = _extract_period(normalized)
        interval = _extract_interval(normalized) or "1d"
        need_chart = _extract_need_chart(normalized)
        need_news = _extract_need_news(normalized)
        confidence = 0.9 if symbol else 0.4
        slots: dict[str, Any] = {
            "symbol": (symbol or ""),
            "period": period,
            "interval": interval,
            "need_chart": bool(need_chart),
            "need_news": bool(need_news),
        }
        plan = NLUPlan(
            intent="analyze_snapshot",
            slots=slots,
            confidence=confidence,
            risk_level="low",
            needs_confirm=False,
            normalized_request=f"analyze_snapshot {json.dumps(slots, sort_keys=True)}",
            action_version=action_version,
            explain="rule-based analyze_snapshot parsing",
            command_template="/analyze <symbol>",
            plan_steps=_build_plan_steps("analyze_snapshot", need_chart=need_chart),
        )
        return _validate_slots(plan)

    if intent == "list_jobs":
        plan = NLUPlan(
            intent="list_jobs",
            slots={},
            confidence=0.95,
            risk_level="low",
            needs_confirm=False,
            normalized_request="list_jobs",
            action_version=action_version,
            explain="rule-based list_jobs parsing",
            command_template="/list",
            plan_steps=_build_plan_steps("list_jobs"),
        )
        return _validate_slots(plan)

    if intent == "daily_digest":
        slots = {"period": "daily"}
        plan = NLUPlan(
            intent="daily_digest",
            slots=slots,
            confidence=0.95,
            risk_level="low",
            needs_confirm=False,
            normalized_request=f"daily_digest {json.dumps(slots, sort_keys=True)}",
            action_version=action_version,
            explain="rule-based daily_digest parsing",
            command_template="/digest daily",
            plan_steps=_build_plan_steps("daily_digest"),
        )
        return _validate_slots(plan)

    if intent == "stop_job":
        target, target_type = _extract_stop_target(normalized)
        confidence = 0.9 if target else 0.5
        slots = {"target": target or "", "target_type": target_type}
        plan = NLUPlan(
            intent="stop_job",
            slots=slots,
            confidence=confidence,
            risk_level="high",
            needs_confirm=True,
            normalized_request=f"stop_job {json.dumps(slots, sort_keys=True)}",
            action_version=action_version,
            explain="rule-based stop_job parsing",
            command_template="/stop <job_id|symbol>",
            plan_steps=_build_plan_steps("stop_job"),
        )
        return _validate_slots(plan)

    if intent in _GENERAL_CONVERSATION_INTENTS:
        slots = {"conversation_intent": intent}
        plan = NLUPlan(
            intent=intent,
            slots=slots,
            confidence=0.98,
            risk_level="low",
            needs_confirm=False,
            normalized_request=f"{intent} {json.dumps(slots, sort_keys=True)}",
            action_version=action_version,
            explain="rule-based general conversation parsing",
            command_template="/help",
            plan_steps=_build_plan_steps(intent),
        )
        return _validate_slots(plan)

    if intent != "create_monitor":
        return NLUPlan(
            intent=intent,
            slots={},
            confidence=0.2,
            risk_level="high" if intent in {"stop_job", "bulk_change"} else "low",
            needs_confirm=intent in {"create_monitor", "stop_job", "bulk_change"},
            normalized_request=normalized,
            action_version=action_version,
            explain="could not map request to supported intents",
            reject_reason="low_confidence",
            plan_steps=[{"step": "fallback_help", "action": "fallback_help", "status": "pending"}],
        )

    symbol = _extract_symbol(normalized)
    interval = _extract_interval(normalized)
    template = _extract_template(normalized)
    route_strategy = _extract_route_strategy(normalized)
    strategy_tier = _extract_strategy_tier(normalized)
    mode, threshold = _TEMPLATE_MODE_THRESHOLD[template]

    confidence = 0.92 if symbol and interval else 0.45
    slots: dict[str, Any] = {
        "symbol": (symbol or ""),
        "interval": (interval or ""),
        "interval_sec": parse_interval_to_seconds(interval or ""),
        "template": template,
        "mode": mode,
        "threshold": threshold,
        "route_strategy": route_strategy,
        "strategy_tier": strategy_tier,
    }
    plan = NLUPlan(
        intent="create_monitor",
        slots=slots,
        confidence=confidence,
        risk_level="high",
        needs_confirm=True,
        normalized_request=f"create_monitor {json.dumps(slots, sort_keys=True)}",
        action_version=action_version,
        explain="rule-based create_monitor parsing",
        plan_steps=_build_plan_steps("create_monitor"),
    )
    return _validate_slots(plan)
