from __future__ import annotations

import re
from dataclasses import dataclass

from tools.market_data import normalize_market_symbol

_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.\-]{1,12}$")
_INTERVAL_PATTERN = re.compile(r"^(\d+)([smhd])$", re.IGNORECASE)
_JOB_ID_PATTERN = re.compile(r"^job-[a-f0-9]{8}$")
_QUIET_HOURS_PATTERN = re.compile(r"^(\d{1,2})-(\d{1,2})$")
_ALERT_VIEWS = {"triggered", "failed", "suppressed"}
_ROUTE_STRATEGIES = {"telegram_only", "webhook_only", "dual_channel"}


@dataclass
class CommandRoute:
    name: str
    args: dict[str, str]


@dataclass
class CommandError:
    message: str


def _parse_interval_to_seconds(raw: str) -> int | None:
    match = _INTERVAL_PATTERN.fullmatch((raw or "").strip())
    if not match:
        return None
    value = int(match.group(1))
    unit = match.group(2).lower()
    if value <= 0:
        return None
    factor = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
    seconds = value * factor
    if seconds < 60 or seconds > 86400:
        return None
    return seconds


def _normalize_symbols(raw: str) -> list[str] | None:
    symbols: list[str] = []
    for token in [piece.strip() for piece in (raw or "").split(",")]:
        if not token:
            continue
        symbol = normalize_market_symbol(token, market="auto")
        if not symbol or not _SYMBOL_PATTERN.fullmatch(symbol):
            return None
        symbols.append(symbol)
    if not symbols:
        return None
    deduped = list(dict.fromkeys(symbols))
    return deduped


def parse_telegram_command(text: str) -> CommandRoute | CommandError:
    raw = (text or "").strip()
    if not raw:
        return CommandError("命令为空 (Empty command). 请使用 /help 查看可用命令 (Use /help to see available commands).")
    parts = raw.split()
    command = parts[0].lower()

    if command == "/help":
        if len(parts) > 1:
            return CommandError("`/help` 不接受参数 (`/help` does not take parameters).")
        return CommandRoute(name="help", args={})

    if command == "/analyze":
        if len(parts) != 2:
            return CommandError("用法 (Usage): /analyze <symbol>. 示例 (Example): /analyze AAPL")
        symbol = normalize_market_symbol(parts[1], market="auto")
        if not symbol or not _SYMBOL_PATTERN.fullmatch(symbol):
            return CommandError(f"无效标的 (Invalid symbol): {parts[1]}. 示例 (Example): /analyze AAPL")
        return CommandRoute(name="analyze", args={"symbol": symbol})

    if command == "/monitor":
        if len(parts) not in {3, 4, 5}:
            return CommandError(
                "用法 (Usage): /monitor <symbol|sym1,sym2> <interval> [volatility|price|rsi] "
                "[telegram_only|webhook_only|dual_channel]. 示例 (Example): /monitor TSLA 1h rsi"
            )
        symbols = _normalize_symbols(parts[1])
        if symbols is None:
            return CommandError(f"无效标的列表 (Invalid symbol list): {parts[1]}. 示例 (Example): /monitor TSLA 1h")
        interval_seconds = _parse_interval_to_seconds(parts[2])
        if interval_seconds is None:
            return CommandError("无效周期 (Invalid interval). 请使用 1m-24h，例如 (e.g.) 5m, 1h, 4h.")
        template = "volatility"
        mode = "anomaly"
        threshold = 0.03
        if len(parts) >= 4:
            raw_template = parts[3].lower()
            template_map = {
                "volatility": ("anomaly", 0.03),
                "price": ("price_breakout", 0.02),
                "rsi": ("rsi_extreme", 70.0),
            }
            picked = template_map.get(raw_template)
            if picked is None:
                return CommandError("无效监控模板 (Invalid monitor template). 可用 volatility|price|rsi.")
            mode, threshold = picked
            template = raw_template
        route_strategy = "dual_channel"
        if len(parts) == 5:
            route_strategy = parts[4].lower()
            if route_strategy not in _ROUTE_STRATEGIES:
                return CommandError("无效路由策略 (Invalid route strategy). 可用 telegram_only|webhook_only|dual_channel.")
        scope = "group" if len(symbols) > 1 else "single"
        return CommandRoute(
            name="monitor",
            args={
                "symbol": symbols[0],
                "symbols_csv": ",".join(symbols),
                "symbols_count": str(len(symbols)),
                "scope": scope,
                "interval": parts[2].lower(),
                "interval_sec": str(interval_seconds),
                "template": template,
                "mode": mode,
                "threshold": str(threshold),
                "route_strategy": route_strategy,
            },
        )

    if command == "/list":
        if len(parts) > 1:
            return CommandError("`/list` 不接受参数 (`/list` does not take parameters).")
        return CommandRoute(name="list", args={})

    if command == "/stop":
        if len(parts) != 2:
            return CommandError("用法 (Usage): /stop <job_id|symbol>. 示例 (Example): /stop TSLA")
        target = parts[1].strip()
        if not target:
            return CommandError("用法 (Usage): /stop <job_id|symbol>. 示例 (Example): /stop TSLA")
        if _JOB_ID_PATTERN.fullmatch(target.lower()):
            return CommandRoute(name="stop", args={"target": target.lower(), "target_type": "job_id"})
        symbol = normalize_market_symbol(target, market="auto")
        if not symbol or not _SYMBOL_PATTERN.fullmatch(symbol):
            return CommandError("停止目标无效 (Stop target invalid). 示例 (e.g.) /stop TSLA 或 /stop job-deadbeef")
        return CommandRoute(name="stop", args={"target": symbol, "target_type": "symbol"})

    if command == "/report":
        if len(parts) not in {2, 3}:
            return CommandError("用法 (Usage): /report <run_id|request_id> [short|full]. 示例 (Example): /report run-123abc full")
        target = parts[1].strip()
        if not target:
            return CommandError("用法 (Usage): /report <run_id|request_id>. 示例 (Example): /report run-123abc")
        detail = "short"
        if len(parts) == 3:
            detail = parts[2].strip().lower()
            if detail not in {"short", "full"}:
                return CommandError("报告详情参数必须为 short|full (Report detail must be short|full).")
        return CommandRoute(name="report", args={"target_id": target, "detail": detail})

    if command == "/digest":
        if len(parts) != 2:
            return CommandError("用法 (Usage): /digest daily")
        period = parts[1].strip().lower()
        if period != "daily":
            return CommandError("当前仅支持 `/digest daily` (Only `/digest daily` is supported right now).")
        return CommandRoute(name="digest", args={"period": period})

    if command == "/alerts":
        if len(parts) > 3:
            return CommandError("用法 (Usage): /alerts [triggered|failed|suppressed] [limit]. 示例 (Example): /alerts failed 20")
        view = "triggered"
        if len(parts) >= 2:
            view = parts[1].strip().lower()
            if view not in _ALERT_VIEWS:
                return CommandError("告警视图必须为 triggered|failed|suppressed (Alert view must be triggered|failed|suppressed).")
        limit = 10
        if len(parts) == 3:
            try:
                limit = int(parts[2])
            except ValueError:
                return CommandError("告警数量必须是 1-50 的整数 (Alert limit must be integer in range 1-50).")
            if limit < 1 or limit > 50:
                return CommandError("告警数量必须是 1-50 的整数 (Alert limit must be integer in range 1-50).")
        return CommandRoute(name="alerts", args={"view": view, "limit": str(limit)})

    if command == "/bulk":
        if len(parts) < 3:
            return CommandError("用法 (Usage): /bulk <enable|disable|interval|threshold> <target|all> [value]")
        action = parts[1].strip().lower()
        if action not in {"enable", "disable", "interval", "threshold"}:
            return CommandError("批量动作必须是 enable|disable|interval|threshold (Bulk action must be ...).")
        target = parts[2].strip()
        if not target:
            return CommandError("批量目标必须是 `all` 或逗号分隔的 job_id/symbol 列表 (Bulk target must be `all` or comma-separated job_id/symbol list).")
        selector = target.lower()
        if selector != "all":
            for item in [piece.strip() for piece in target.split(",")]:
                if not item:
                    return CommandError("批量目标包含空项 (Bulk target contains empty item).")
                if _JOB_ID_PATTERN.fullmatch(item.lower()):
                    continue
                symbol = normalize_market_symbol(item, market="auto")
                if not symbol or not _SYMBOL_PATTERN.fullmatch(symbol):
                    return CommandError(f"无效批量目标项 (Invalid bulk target item): {item}")
        value = ""
        if action in {"interval", "threshold"}:
            if len(parts) != 4:
                return CommandError("Bulk interval/threshold 需要参数值 (requires a value).")
            value = parts[3].strip().lower()
            if action == "interval":
                parsed = _parse_interval_to_seconds(value)
                if parsed is None:
                    return CommandError("Bulk interval 必须在 1m-24h，示例 (e.g.) /bulk interval all 30m")
                value = str(parsed)
            else:
                try:
                    threshold = float(value)
                except ValueError:
                    return CommandError("Bulk threshold 必须是数字 (must be numeric).")
                if threshold <= 0:
                    return CommandError("Bulk threshold 必须为正数 (must be positive).")
                value = str(threshold)
        elif len(parts) != 3:
            return CommandError("Bulk enable/disable 不接受额外参数 (does not accept extra value).")
        return CommandRoute(name="bulk", args={"action": action, "target": target, "value": value})

    if command == "/webhook":
        if len(parts) < 2:
            return CommandError("用法 (Usage): /webhook <set|disable|list> ...")
        action = parts[1].strip().lower()
        if action == "set":
            if len(parts) not in {3, 4}:
                return CommandError("用法 (Usage): /webhook set <url> [secret]")
            url = parts[2].strip()
            if not (url.startswith("http://") or url.startswith("https://")):
                return CommandError("Webhook URL 必须以 http:// 或 https:// 开头 (must start with http:// or https://)")
            secret = parts[3].strip() if len(parts) == 4 else ""
            return CommandRoute(name="webhook", args={"action": "set", "url": url, "secret": secret})
        if action == "disable":
            if len(parts) != 3:
                return CommandError("用法 (Usage): /webhook disable <webhook_id>")
            return CommandRoute(name="webhook", args={"action": "disable", "webhook_id": parts[2].strip()})
        if action == "list":
            if len(parts) != 2:
                return CommandError("用法 (Usage): /webhook list")
            return CommandRoute(name="webhook", args={"action": "list"})
        return CommandError("Webhook action 必须是 set|disable|list.")

    if command == "/pref":
        if len(parts) < 3:
            return CommandError("用法 (Usage): /pref <summary|quiet|priority> <value>")
        setting = parts[1].strip().lower()
        value = parts[2].strip().lower()
        if setting == "summary":
            if value not in {"short", "long"}:
                return CommandError("summary 偏好必须是 short|long (Summary preference must be short|long).")
            return CommandRoute(name="pref", args={"setting": "summary", "value": value})
        if setting == "priority":
            if value not in {"critical", "high", "all"}:
                return CommandError("priority 偏好必须是 critical|high|all (Priority preference must be critical|high|all).")
            return CommandRoute(name="pref", args={"setting": "priority", "value": value})
        if setting == "quiet":
            if value == "off":
                return CommandRoute(name="pref", args={"setting": "quiet", "value": "off"})
            match = _QUIET_HOURS_PATTERN.fullmatch(value)
            if not match:
                return CommandError("quiet 时段必须是 `off` 或 HH-HH，示例 (e.g.) 22-07.")
            start = int(match.group(1))
            end = int(match.group(2))
            if not (0 <= start <= 23 and 0 <= end <= 23):
                return CommandError("quiet 时段必须在 00-23 范围 (Quiet hours must use 00-23 range).")
            return CommandRoute(name="pref", args={"setting": "quiet", "value": f"{start:02d}-{end:02d}"})
        return CommandError("偏好键必须是 summary|quiet|priority (Preference key must be ...).")

    return CommandError(f"不支持的命令 (Unsupported command): {parts[0]}. 请使用 /help 查看可用命令 (Use /help to see available commands).")
