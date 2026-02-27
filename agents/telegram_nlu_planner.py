from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

from tools.market_data import normalize_market_symbol

_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.\-]{1,12}$")
_INTERVAL_PATTERN = re.compile(r"^(\d+)([smhd])$", re.IGNORECASE)
_ALLOWED_INTERVALS = {"1m", "5m", "15m", "30m", "1h", "4h", "1d", "24h"}
_ALLOWED_ANALYZE_PERIODS = {"5d", "1mo", "3mo", "6mo", "1y"}
_ALLOWED_TEMPLATES = {"volatility", "price", "rsi"}
_ALLOWED_ROUTE_STRATEGIES = {"telegram_only", "webhook_only", "dual_channel"}

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
    command_template: str = "/monitor <symbol> <interval> [volatility|price|rsi] [telegram_only|webhook_only|dual_channel]"


def normalize_text(text: str) -> str:
    return " ".join((text or "").strip().split())


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
    if "webhook_only" in lowered:
        return "webhook_only"
    return "dual_channel"


def _intent_from_text(text: str) -> str:
    lowered = text.lower()
    if any(keyword in lowered for keyword in ("monitor", "盯", "监控", "提醒", "watch")):
        return "create_monitor"
    if any(keyword in lowered for keyword in ("analyze", "analysis", "分析", "涨跌", "走势", "看", "snapshot")):
        return "analyze_snapshot"
    if any(keyword in lowered for keyword in ("stop", "停止", "取消监控")):
        return "stop_job"
    if "bulk" in lowered or "批量" in lowered:
        return "bulk_change"
    return "unknown"


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


def _validate_slots(plan: NLUPlan) -> NLUPlan:
    if plan.intent not in {"create_monitor", "stop_job", "bulk_change", "analyze_snapshot"}:
        plan.reject_reason = "low_confidence"
        plan.explain = "intent not supported in v1"
        return plan

    if plan.intent == "analyze_snapshot":
        symbol = str(plan.slots.get("symbol", "")).upper()
        period = str(plan.slots.get("period", "")).lower()
        interval = str(plan.slots.get("interval", "")).lower()
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

    if plan.intent != "create_monitor":
        return plan

    symbol = str(plan.slots.get("symbol", "")).upper()
    interval = str(plan.slots.get("interval", "")).lower()
    template = str(plan.slots.get("template", "")).lower()
    route_strategy = str(plan.slots.get("route_strategy", "")).lower()

    if not symbol or not _SYMBOL_PATTERN.fullmatch(symbol):
        plan.reject_reason = "invalid_slot"
        plan.explain = "invalid symbol"
        return plan
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

    return plan


def plan_from_text(text: str) -> NLUPlan:
    normalized = normalize_text(text)
    intent = _intent_from_text(normalized)
    action_version = "v1"
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
            explain="could not map request to create_monitor",
            reject_reason="low_confidence",
        )

    symbol = _extract_symbol(normalized)
    interval = _extract_interval(normalized)
    template = _extract_template(normalized)
    route_strategy = _extract_route_strategy(normalized)
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
    )
    return _validate_slots(plan)
