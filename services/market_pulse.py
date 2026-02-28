from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from services.telegram_store import PulseSubscriptionRecord, TelegramTaskStore


class PulseSender(Protocol):
    async def send_text(self, chat_id: str, text: str) -> dict[str, Any]:
        ...


@dataclass
class PulseWindow:
    start: datetime
    end: datetime


class MarketPulsePublisher:
    def __init__(self, *, store: TelegramTaskStore, sender: PulseSender):
        self._store = store
        self._sender = sender

    async def publish_due(self, *, now: datetime) -> int:
        ts = now.astimezone(timezone.utc)
        sent = 0
        for sub in self._store.list_pulse_subscriptions():
            window = self._pulse_window(now=ts, interval_hours=sub.interval_hours)
            pulse_key = f"{sub.schedule_token}:{window.end.isoformat()}"
            claimed = self._store.claim_market_pulse_dispatch(
                chat_id=sub.chat_id,
                pulse_key=pulse_key,
                schedule_token=sub.schedule_token,
                window_start=window.start,
                window_end=window.end,
            )
            if not claimed:
                continue
            text = self._build_market_pulse_text(sub=sub, window=window)
            try:
                await self._sender.send_text(sub.chat_id, text)
                self._store.record_metric(
                    metric_name="market_pulse_sent_total",
                    metric_value=1.0,
                    tags={"chat_id": sub.chat_id, "interval": f"{sub.interval_hours}h"},
                )
                sent += 1
            except Exception as exc:  # pragma: no cover - runtime guard
                self._store.record_metric(
                    metric_name="market_pulse_failed_total",
                    metric_value=1.0,
                    tags={"chat_id": sub.chat_id, "error": str(exc)[:60]},
                )
        return sent

    @staticmethod
    def _pulse_window(*, now: datetime, interval_hours: int) -> PulseWindow:
        safe_interval = max(1, int(interval_hours))
        normalized_now = now.astimezone(timezone.utc)
        hour_bucket = (normalized_now.hour // safe_interval) * safe_interval
        end = normalized_now.replace(hour=hour_bucket, minute=0, second=0, microsecond=0)
        if end > normalized_now:
            end = end - timedelta(hours=safe_interval)
        return PulseWindow(start=end - timedelta(hours=safe_interval), end=end)

    def _build_market_pulse_text(self, *, sub: PulseSubscriptionRecord, window: PulseWindow) -> str:
        movers_lines = self._top_mover_lines(chat_id=sub.chat_id, window=window)
        theme_lines = self._theme_delta_lines(chat_id=sub.chat_id, window=window, interval_hours=sub.interval_hours)
        risk_lines = self._risk_lines()
        return (
            f"市场脉冲（{sub.interval_hours}h）\n"
            f"窗口：{window.start.strftime('%Y-%m-%d %H:%M')} ~ {window.end.strftime('%Y-%m-%d %H:%M')} UTC\n"
            "\nTop movers：\n"
            + "\n".join(movers_lines)
            + "\n\n新闻主题变化：\n"
            + "\n".join(theme_lines)
            + "\n\n风险提示：\n"
            + "\n".join(risk_lines)
            + "\n\n可用：/status 查看运行状态，/pref pulse off 关闭主动推送。"
        )

    def _top_mover_lines(self, *, chat_id: str, window: PulseWindow) -> list[str]:
        events = self._store.list_watch_events_for_chat(
            chat_id=chat_id,
            since=window.start,
            until=window.end,
            limit=300,
        )
        if not events:
            return ["- 暂无显著异动。"]
        best_by_symbol: dict[str, tuple[float, str]] = {}
        for item in events:
            pct = float(item.pct_change)
            previous = best_by_symbol.get(item.symbol)
            if previous is None or abs(pct) > abs(previous[0]):
                best_by_symbol[item.symbol] = (pct, item.priority)
        ranked = sorted(best_by_symbol.items(), key=lambda kv: (-abs(kv[1][0]), kv[0]))
        out: list[str] = []
        for symbol, (pct, priority) in ranked[:3]:
            out.append(f"- {symbol} {pct * 100:+.2f}%（优先级={priority}）")
        return out

    def _theme_delta_lines(self, *, chat_id: str, window: PulseWindow, interval_hours: int) -> list[str]:
        current = self._store.list_analysis_report_metrics(chat_id=chat_id, since=window.start, until=window.end, limit=120)
        prev_start = window.start - timedelta(hours=max(1, int(interval_hours)))
        previous = self._store.list_analysis_report_metrics(chat_id=chat_id, since=prev_start, until=window.start, limit=120)
        current_counts = self._aggregate_theme_counts(current)
        previous_counts = self._aggregate_theme_counts(previous)
        keys = sorted(set(current_counts.keys()) | set(previous_counts.keys()))
        if not keys:
            return ["- 主题覆盖不足，变化不明显。"]
        deltas: list[tuple[str, int]] = []
        for key in keys:
            delta = int(current_counts.get(key, 0)) - int(previous_counts.get(key, 0))
            if delta != 0:
                deltas.append((key, delta))
        if not deltas:
            ranked = sorted(current_counts.items(), key=lambda item: (-item[1], item[0]))
            if not ranked:
                return ["- 主题覆盖不足，变化不明显。"]
            return [f"- {name}：热度持平（当前 {count}）" for name, count in ranked[:3]]
        deltas.sort(key=lambda item: (-abs(item[1]), item[0]))
        lines: list[str] = []
        for name, delta in deltas[:3]:
            direction = "上升" if delta > 0 else "下降"
            lines.append(f"- {name}：热度{direction}（{delta:+d}）")
        return lines

    @staticmethod
    def _aggregate_theme_counts(metrics_list: list[dict[str, Any]]) -> dict[str, int]:
        out: dict[str, int] = {}
        for metrics in metrics_list:
            digest = metrics.get("news_digest")
            if not isinstance(digest, dict):
                continue
            top_themes = digest.get("top_themes")
            if isinstance(top_themes, list):
                for item in top_themes:
                    if not isinstance(item, dict):
                        continue
                    name = str(item.get("category", "")).strip()
                    if not name:
                        continue
                    count = int(item.get("count", 0))
                    out[name] = out.get(name, 0) + max(0, count)
                continue
            distribution = digest.get("event_distribution")
            if not isinstance(distribution, dict):
                continue
            for name, raw_count in distribution.items():
                key = str(name).strip()
                if not key:
                    continue
                out[key] = out.get(key, 0) + max(0, int(raw_count))
        return out

    def _risk_lines(self) -> list[str]:
        states = [item for item in self._store.list_degradation_states() if item.status == "active"]
        if states:
            lines: list[str] = []
            for item in states[:3]:
                reason = str(item.reason or "无").strip()
                lines.append(f"- {item.state_key} 仍在降级（原因={reason}）")
            return lines
        retry_depth = self._store.count_retry_queue_depth()
        dlq = self._store.count_dlq()
        if retry_depth > 0 or dlq > 0:
            return [f"- 通知队列存在积压（retry={retry_depth}，dlq={dlq}），请关注投递质量。"]
        return ["- 当前未检测到显著运行风险。"]

