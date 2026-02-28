"""Strategy tier normalization and execution guards for Upgrade7 P2-C."""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_STRATEGY_TIER = "execution-ready"
ALLOWED_STRATEGY_TIERS = ("research-only", "alert-only", "execution-ready")


@dataclass(frozen=True)
class StrategyTierDecision:
    tier: str
    requested_enable_triggered_research: bool
    allow_triggered_research: bool
    allow_notification_dispatch: bool
    guard_reason: str = ""

    @property
    def guarded(self) -> bool:
        return bool(self.guard_reason)

    def to_dict(self) -> dict[str, object]:
        return {
            "tier": self.tier,
            "requested_enable_triggered_research": self.requested_enable_triggered_research,
            "allow_triggered_research": self.allow_triggered_research,
            "allow_notification_dispatch": self.allow_notification_dispatch,
            "guard_reason": self.guard_reason,
        }


def normalize_strategy_tier(raw: str | None, *, default: str = DEFAULT_STRATEGY_TIER) -> str:
    value = str(raw or "").strip().lower()
    if value in ALLOWED_STRATEGY_TIERS:
        return value
    return default


def resolve_strategy_tier(
    tier: str | None,
    *,
    requested_enable_triggered_research: bool,
) -> StrategyTierDecision:
    normalized = normalize_strategy_tier(tier)
    requested = bool(requested_enable_triggered_research)
    if normalized == "research-only":
        return StrategyTierDecision(
            tier=normalized,
            requested_enable_triggered_research=requested,
            allow_triggered_research=requested,
            allow_notification_dispatch=False,
            guard_reason="strategy_tier_guard_research_only",
        )
    if normalized == "alert-only":
        return StrategyTierDecision(
            tier=normalized,
            requested_enable_triggered_research=requested,
            allow_triggered_research=False,
            allow_notification_dispatch=True,
            guard_reason="strategy_tier_guard_alert_only_research",
        )
    return StrategyTierDecision(
        tier=normalized,
        requested_enable_triggered_research=requested,
        allow_triggered_research=requested,
        allow_notification_dispatch=True,
        guard_reason="",
    )
