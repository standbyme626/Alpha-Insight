#!/usr/bin/env python3
"""Generate Upgrade7 P1 evidence artifacts for C1/C2/(optional D1)."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from core.connectors import BaseConnector, ConnectorError, ConnectorErrorCode, RetryPolicy, Throttler
from core.runtime_config import RuntimeConfigValidationError, resolve_runtime_config
from core.strategy_plugins import PluginContext, PluginKind, StrategyPluginManager
from tools.news_data import RSSNewsConnector


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


async def _build_plugin_loading_matrix() -> dict[str, Any]:
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
                        "timeout_ms": 120,
                        "params": {"message": "probe boom"},
                    },
                }
            }
        }
    )
    manager = StrategyPluginManager.from_runtime_config(resolved.config)
    output, audit = await manager.apply(
        kind=PluginKind.POLICIES,
        payload={"allow_triggered_research": True, "selected_alerts": []},
        context=PluginContext(request_id="evidence", trigger_id="upgrade7-p1"),
    )
    return {
        "generated_at": _now(),
        "runtime_flags_applied": True,
        "loaded_plugins": manager.loading_matrix(),
        "policy_output": output,
        "audit": [item.to_dict() for item in audit],
    }


def _build_config_layering_validation() -> dict[str, Any]:
    resolved = resolve_runtime_config(
        env_override_layer={
            "connectors": {
                "news_rss": {
                    "timeout_seconds": 12.0,
                }
            }
        },
        runtime_flags_layer={
            "connectors": {
                "news_rss": {
                    "timeout_seconds": 6.0,
                    "retry": {"max_attempts": 3},
                }
            },
            "strategy_plugins": {
                "signals": {
                    "signal_sanity": {"timeout_ms": 1200},
                }
            },
        },
    )
    validation_probe: dict[str, Any] = {"ok": True, "issues": []}
    try:
        resolve_runtime_config(
            runtime_flags_layer={
                "connectors": {
                    "news_rss": {
                        "retry": {"max_attempts": "bad"},
                    }
                }
            }
        )
    except RuntimeConfigValidationError as exc:
        validation_probe = {"ok": False, "issues": [item.to_dict() for item in exc.issues]}

    return {
        "generated_at": _now(),
        "merge_priority": list(resolved.merge_priority),
        "final_config": resolved.merged,
        "diff_summary": resolved.diff_summary,
        "source_trace": resolved.source_trace,
        "validation_probe": validation_probe,
    }


class _FakeResponse:
    def __init__(self, status: int, text: str = ""):
        self.status = status
        self._text = text

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None

    async def text(self) -> str:
        return self._text


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]):
        self._responses = responses
        self.calls = 0

    def get(self, _url: str) -> _FakeResponse:
        idx = min(self.calls, len(self._responses) - 1)
        self.calls += 1
        return self._responses[idx]


async def _build_connector_reliability() -> dict[str, Any]:
    base_connector = BaseConnector(
        name="evidence_connector",
        timeout_seconds=1.0,
        retry_policy=RetryPolicy(max_attempts=3, base_backoff_seconds=0.0, max_backoff_seconds=0.0, jitter_seconds=0.0),
        throttler=Throttler(rate_per_sec=1000),
    )
    attempts = {"count": 0}

    async def flaky_call() -> str:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ConnectorError(code=ConnectorErrorCode.RATE_LIMIT, message="429", retriable=True, status_code=429)
        return "ok"

    result = await base_connector.call("flaky", flaky_call)

    rss_connector = RSSNewsConnector(
        timeout_seconds=1.0,
        throttle_rate_per_sec=1000,
        retry_policy=RetryPolicy(max_attempts=2, base_backoff_seconds=0.0, max_backoff_seconds=0.0, jitter_seconds=0.0),
    )
    fake_session = _FakeSession([_FakeResponse(503), _FakeResponse(200, "<rss></rss>")])
    rss_payload = await rss_connector.fetch_feed(session=fake_session, url="https://example.com/rss")

    return {
        "generated_at": _now(),
        "base_connector_retry": {
            "result": result,
            "attempts": attempts["count"],
            "error_semantic": ConnectorErrorCode.RATE_LIMIT.value,
        },
        "rss_connector_retry": {
            "result": rss_payload,
            "attempts": fake_session.calls,
            "error_semantic": ConnectorErrorCode.UPSTREAM_5XX.value,
        },
    }


async def _main(include_d1: bool) -> None:
    plugin_payload = await _build_plugin_loading_matrix()
    _write_json(Path("docs/evidence/upgrade7_plugin_loading_matrix.json"), plugin_payload)
    print("[OK] docs/evidence/upgrade7_plugin_loading_matrix.json")

    config_payload = _build_config_layering_validation()
    _write_json(Path("docs/evidence/upgrade7_config_layering_validation.json"), config_payload)
    print("[OK] docs/evidence/upgrade7_config_layering_validation.json")

    if include_d1:
        connector_payload = await _build_connector_reliability()
        _write_json(Path("docs/evidence/upgrade7_connector_reliability.json"), connector_payload)
        print("[OK] docs/evidence/upgrade7_connector_reliability.json")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Upgrade7 P1 evidence artifacts.")
    parser.add_argument("--skip-d1", action="store_true", help="Skip connector evidence generation")
    args = parser.parse_args()
    asyncio.run(_main(include_d1=not args.skip_d1))


if __name__ == "__main__":
    main()
