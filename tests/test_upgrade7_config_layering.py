from __future__ import annotations

import pytest

from core.runtime_config import RuntimeConfigValidationError, resolve_runtime_config


def test_config_layering_runtime_flags_override_env_override() -> None:
    resolved = resolve_runtime_config(
        env_override_layer={
            "strategy_plugins": {
                "signals": {
                    "signal_sanity": {
                        "timeout_ms": 900,
                    }
                }
            }
        },
        runtime_flags_layer={
            "strategy_plugins": {
                "signals": {
                    "signal_sanity": {
                        "timeout_ms": 1200,
                    }
                }
            }
        },
    )

    timeout_value = resolved.config.strategy_plugins.signals["signal_sanity"].timeout_ms
    assert timeout_value == 1200
    assert (
        resolved.source_trace["strategy_plugins.signals.signal_sanity.timeout_ms"]
        == "runtime_flags"
    )
    changed_paths = {item["path"] for item in resolved.diff_summary}
    assert "strategy_plugins.signals.signal_sanity.timeout_ms" in changed_paths


def test_config_validation_fail_fast_includes_path_and_suggestion() -> None:
    with pytest.raises(RuntimeConfigValidationError) as exc_info:
        resolve_runtime_config(
            runtime_flags_layer={
                "connectors": {
                    "news_rss": {
                        "retry": {
                            "max_attempts": "bad-type",
                        }
                    }
                }
            }
        )

    issues = exc_info.value.issues
    assert issues
    issue = issues[0]
    assert issue.path == "connectors.news_rss.retry.max_attempts"
    assert "integer" in issue.suggestion.lower()


def test_config_validation_rejects_unknown_field_with_fix_hint() -> None:
    with pytest.raises(RuntimeConfigValidationError) as exc_info:
        resolve_runtime_config(
            runtime_flags_layer={
                "strategy_plugins": {
                    "policies": {
                        "research_guard": {
                            "unknown_key": 1,
                        }
                    }
                }
            }
        )

    issue = exc_info.value.issues[0]
    assert issue.path.endswith("unknown_key")
    assert "remove unknown field" in issue.suggestion.lower()
