from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from services.telegram_store import TelegramTaskStore


@dataclass
class GovernorConfig:
    push_success_threshold: float = 0.99
    analysis_p95_threshold_ms: float = 90_000.0
    trigger_window_minutes: int = 10
    recovery_window_minutes: int = 30
    dlq_rise_points: int = 5


class ReliabilityGovernor:
    """Evaluates SLO signals and toggles degradation states in persistent storage."""

    def __init__(self, *, store: TelegramTaskStore, config: GovernorConfig | None = None):
        self._store = store
        self._config = config or GovernorConfig()

    def evaluate(self, *, now: datetime | None = None) -> None:
        current = now or datetime.now(timezone.utc)
        trigger_since = current - timedelta(minutes=self._config.trigger_window_minutes)
        recovery_since = current - timedelta(minutes=self._config.recovery_window_minutes)

        # Persist DLQ trend samples for monotonic trend checks.
        self._store.record_metric(
            metric_name="dlq_count_snapshot",
            metric_value=float(self._store.count_dlq()),
            created_at=current,
        )

        push_rate_trigger = self._push_success_rate_since(trigger_since) < self._config.push_success_threshold
        push_rate_recovery = self._push_success_rate_since(recovery_since) >= self._config.push_success_threshold

        dlq_rising_trigger = self._dlq_is_rising(self._config.dlq_rise_points)
        dlq_recovery = not self._dlq_is_rising(max(self._config.dlq_rise_points, 10))

        analysis_p95_trigger = self._analysis_p95_since(trigger_since) > self._config.analysis_p95_threshold_ms
        analysis_p95_recovery = self._analysis_p95_since(recovery_since) <= self._config.analysis_p95_threshold_ms

        self._reconcile_state(
            state_key="no_monitor_push",
            trigger=push_rate_trigger,
            recover=push_rate_recovery,
            trigger_reason=f"push_success_rate < {self._config.push_success_threshold:.2%}",
            recover_reason=f"push_success_rate >= {self._config.push_success_threshold:.2%}",
        )
        self._reconcile_state(
            state_key="summary_mode",
            trigger=dlq_rising_trigger,
            recover=dlq_recovery,
            trigger_reason="dlq_count keeps rising",
            recover_reason="dlq trend stabilized",
        )
        self._reconcile_state(
            state_key="disable_critical_research",
            trigger=analysis_p95_trigger,
            recover=analysis_p95_recovery,
            trigger_reason=f"p95_analysis_latency > {self._config.analysis_p95_threshold_ms:.0f}ms",
            recover_reason=f"p95_analysis_latency <= {self._config.analysis_p95_threshold_ms:.0f}ms",
        )

    def _reconcile_state(
        self,
        *,
        state_key: str,
        trigger: bool,
        recover: bool,
        trigger_reason: str,
        recover_reason: str,
    ) -> None:
        active = self._store.is_degradation_active(state_key=state_key)
        if trigger and not active:
            self._store.set_degradation_state(state_key=state_key, status="active", reason=trigger_reason)
            return
        if active and recover:
            self._store.set_degradation_state(state_key=state_key, status="recovered", reason=recover_reason)

    def _push_success_rate_since(self, since: datetime) -> float:
        attempts = self._store.count_metric_events(metric_name="push_attempt", since=since)
        success = self._store.count_metric_events(metric_name="push_success", since=since)
        if attempts <= 0:
            return 1.0
        return success / attempts

    def _analysis_p95_since(self, since: datetime) -> float:
        values = self._store.metric_values(metric_name="analysis_latency_ms", since=since)
        if not values:
            return 0.0
        values.sort()
        idx = max(0, int(round(0.95 * (len(values) - 1))))
        return float(values[idx])

    def _dlq_is_rising(self, points: int) -> bool:
        if points <= 1:
            return False
        values = self._store.metric_values(metric_name="dlq_count_snapshot")
        if len(values) < points:
            return False
        latest = values[-points:]
        for i in range(1, len(latest)):
            if latest[i] <= latest[i - 1]:
                return False
        return True
