from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from agents.scanner_engine import ScanConfig, build_scan_trigger, run_watchlist_cycle
from core.runtime_config import resolve_runtime_config
from core.strategy_plugins import PluginContext, PluginKind, StrategyPluginManager
from tools.market_data import MarketDataResult


@pytest.mark.asyncio
async def test_plugin_isolation_shell_timeout_and_exception_degrade() -> None:
    resolved = resolve_runtime_config(
        runtime_flags_layer={
            "strategy_plugins": {
                "policies": {
                    "research_guard": {"enabled": False},
                    "timeout_probe": {
                        "enabled": True,
                        "module": "policies.diagnostic_probes",
                        "class_name": "TimeoutProbePolicyPlugin",
                        "timeout_ms": 60,
                        "params": {"sleep_ms": 120},
                    },
                    "error_probe": {
                        "enabled": True,
                        "module": "policies.diagnostic_probes",
                        "class_name": "ErrorProbePolicyPlugin",
                        "timeout_ms": 200,
                        "params": {"message": "probe boom"},
                    },
                }
            }
        }
    )
    manager = StrategyPluginManager.from_runtime_config(resolved.config)
    payload, audits = await manager.apply(
        kind=PluginKind.POLICIES,
        payload={"allow_triggered_research": True, "selected_alerts": []},
        context=PluginContext(request_id="rid", trigger_id="tid"),
    )

    assert payload["allow_triggered_research"] is True
    statuses = {item.plugin_id: item.status for item in audits}
    assert statuses["timeout_probe"] == "timeout"
    assert statuses["error_probe"] == "error"


@pytest.mark.asyncio
async def test_strategy_plugin_loading_matrix_respects_enable_disable() -> None:
    resolved = resolve_runtime_config(
        runtime_flags_layer={
            "strategy_plugins": {
                "alerts": {
                    "alert_priority": {"enabled": False},
                }
            }
        }
    )
    manager = StrategyPluginManager.from_runtime_config(resolved.config)
    matrix = manager.loading_matrix()
    loaded_ids = {item["plugin_id"] for item in matrix}

    assert "signal_sanity" in loaded_ids
    assert "research_guard" in loaded_ids
    assert "alert_priority" not in loaded_ids


@pytest.mark.asyncio
async def test_strategy_plugin_params_injected_into_policy_path() -> None:
    async def fake_fetch(symbol: str, period: str, interval: str = "1d") -> MarketDataResult:
        frame = pd.DataFrame(
            {
                "Date": ["2026-01-01", "2026-01-02"],
                "Open": [100, 100],
                "High": [101, 107],
                "Low": [99, 99],
                "Close": [100, 108],  # critical move against threshold=3%
                "Volume": [1, 1],
            }
        )
        return MarketDataResult(ok=True, symbol=symbol, message="ok", records=frame.to_dict(orient="records"))

    cfg = ScanConfig(watchlist=["AAPL"], pct_alert_threshold=0.03)
    trigger = build_scan_trigger(trigger_type="event", trigger_id="plugin-param-test", trigger_time=datetime.now(timezone.utc))
    result = await run_watchlist_cycle(
        cfg,
        trigger=trigger,
        mode="anomaly",
        fetcher=fake_fetch,
        enable_triggered_research=True,
        runtime_flags={
            "strategy_plugins": {
                "policies": {
                    "research_guard": {
                        "params": {
                            "max_critical_per_cycle": 0,
                        }
                    }
                }
            }
        },
    )

    assert result.runtime_metrics["allow_triggered_research"] is False
    assert any(item["kind"] == "policies" for item in result.runtime_metrics["plugin_audit"])
