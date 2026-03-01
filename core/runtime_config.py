"""Runtime configuration layering, validation, and diff summary for Upgrade7."""

from __future__ import annotations

import copy
import json
import os
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

LAYER_PRIORITY: tuple[str, str, str] = ("base", "env_override", "runtime_flags")
ENV_OVERRIDE_KEY = "ALPHA_INSIGHT_CONFIG_OVERRIDE"


class RetryPolicyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_attempts: int = Field(default=2, ge=1, le=8)
    base_backoff_seconds: float = Field(default=0.2, ge=0.0, le=30.0)
    max_backoff_seconds: float = Field(default=1.0, ge=0.0, le=120.0)
    jitter_seconds: float = Field(default=0.0, ge=0.0, le=5.0)


class ConnectorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    timeout_seconds: float = Field(default=8.0, gt=0.0, le=180.0)
    throttle_rate_per_sec: float = Field(default=4.0, gt=0.0, le=200.0)
    retry: RetryPolicyConfig = Field(default_factory=RetryPolicyConfig)


class ConnectorsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    news_rss: ConnectorConfig = Field(default_factory=ConnectorConfig)


class StrategyPluginConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    module: str
    class_name: str
    timeout_ms: int = Field(default=800, ge=50, le=10000)
    params: dict[str, Any] = Field(default_factory=dict)


class StrategyPluginsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    signals: dict[str, StrategyPluginConfig] = Field(default_factory=dict)
    alerts: dict[str, StrategyPluginConfig] = Field(default_factory=dict)
    policies: dict[str, StrategyPluginConfig] = Field(default_factory=dict)


class RuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategy_plugins: StrategyPluginsConfig = Field(default_factory=StrategyPluginsConfig)
    connectors: ConnectorsConfig = Field(default_factory=ConnectorsConfig)


@dataclass(frozen=True)
class ConfigValidationIssue:
    path: str
    message: str
    suggestion: str

    def to_dict(self) -> dict[str, str]:
        return {"path": self.path, "message": self.message, "suggestion": self.suggestion}


class RuntimeConfigValidationError(ValueError):
    def __init__(self, issues: list[ConfigValidationIssue]):
        self.issues = issues
        joined = "; ".join(
            f"path={item.path} message={item.message} suggestion={item.suggestion}" for item in issues
        )
        super().__init__(joined)


@dataclass(frozen=True)
class ResolvedRuntimeConfig:
    config: RuntimeConfig
    merged: dict[str, Any]
    source_trace: dict[str, str]
    diff_summary: list[dict[str, Any]]
    merge_priority: tuple[str, str, str] = LAYER_PRIORITY

    def to_diff_payload(self) -> dict[str, Any]:
        return {
            "merge_priority": list(self.merge_priority),
            "changed_count": len(self.diff_summary),
            "changed_fields": self.diff_summary,
            "source_trace": self.source_trace,
        }


def _default_base_config() -> dict[str, Any]:
    return {
        "strategy_plugins": {
            "signals": {
                "signal_sanity": {
                    "enabled": True,
                    "module": "signals.signal_sanity",
                    "class_name": "SignalSanityPlugin",
                    "timeout_ms": 800,
                    "params": {},
                }
            },
            "alerts": {
                "alert_priority": {
                    "enabled": True,
                    "module": "alerts.alert_priority",
                    "class_name": "AlertPriorityPlugin",
                    "timeout_ms": 800,
                    "params": {},
                }
            },
            "policies": {
                "research_guard": {
                    "enabled": True,
                    "module": "policies.research_guard",
                    "class_name": "ResearchGuardPolicyPlugin",
                    "timeout_ms": 800,
                    "params": {
                        "max_critical_per_cycle": 1000,
                    },
                }
            },
        },
        "connectors": {
            "news_rss": {
                "enabled": True,
                "timeout_seconds": 8.0,
                "throttle_rate_per_sec": 4.0,
                "retry": {
                    "max_attempts": 2,
                    "base_backoff_seconds": 0.2,
                    "max_backoff_seconds": 1.0,
                    "jitter_seconds": 0.0,
                },
            }
        },
    }


def _suggest_fix(error_type: str, path: str) -> str:
    if error_type == "missing":
        return f"Add required field `{path}` in base/env override/runtime flags."
    if error_type == "extra_forbidden":
        return f"Remove unknown field `{path}` or update RuntimeConfig schema."
    if "bool" in error_type:
        return f"Set `{path}` to a boolean (`true`/`false`)."
    if "int" in error_type:
        return f"Set `{path}` to an integer."
    if "float" in error_type:
        return f"Set `{path}` to a number."
    if "dict" in error_type or "mapping" in error_type:
        return f"Set `{path}` to an object/dict."
    return f"Review `{path}` and provide a schema-compatible value."


def _flatten_values(value: Any, prefix: str = "") -> dict[str, Any]:
    if not isinstance(value, dict):
        return {prefix or "<root>": value}
    out: dict[str, Any] = {}
    for key, item in value.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(item, dict):
            out.update(_flatten_values(item, prefix=path))
        else:
            out[path] = item
    return out


def _deep_merge_with_trace(
    base: Any,
    override: Any,
    *,
    layer_name: str,
    source_trace: dict[str, str],
    path: str = "",
) -> Any:
    if not isinstance(override, dict):
        source_trace[path or "<root>"] = layer_name
        return copy.deepcopy(override)

    baseline: dict[str, Any] = copy.deepcopy(base) if isinstance(base, dict) else {}
    for key, override_value in override.items():
        child_path = f"{path}.{key}" if path else str(key)
        base_value = baseline.get(key)
        if isinstance(override_value, dict):
            baseline[key] = _deep_merge_with_trace(
                base_value if isinstance(base_value, dict) else {},
                override_value,
                layer_name=layer_name,
                source_trace=source_trace,
                path=child_path,
            )
        else:
            baseline[key] = copy.deepcopy(override_value)
            source_trace[child_path] = layer_name
    return baseline


def _ensure_mapping(layer_name: str, value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return copy.deepcopy(value)
    issue = ConfigValidationIssue(
        path=layer_name,
        message=f"{layer_name} must be a dict/object, got {type(value).__name__}",
        suggestion=f"Set `{layer_name}` to a JSON object.",
    )
    raise RuntimeConfigValidationError([issue])


def _parse_env_override(env_var_name: str) -> dict[str, Any]:
    raw = os.getenv(env_var_name, "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeConfigValidationError(
            [
                ConfigValidationIssue(
                    path=env_var_name,
                    message=f"invalid JSON override: {exc.msg}",
                    suggestion=f"Set `{env_var_name}` to valid JSON object string.",
                )
            ]
        ) from exc
    return _ensure_mapping("env_override", parsed)


def _raise_validation_error(exc: ValidationError) -> None:
    issues: list[ConfigValidationIssue] = []
    for error in exc.errors():
        loc = ".".join(str(item) for item in error.get("loc", [])) or "<root>"
        error_type = str(error.get("type", "value_error"))
        issues.append(
            ConfigValidationIssue(
                path=loc,
                message=str(error.get("msg", "invalid value")),
                suggestion=_suggest_fix(error_type, loc),
            )
        )
    raise RuntimeConfigValidationError(issues) from exc


def _build_diff_summary(
    *,
    base_layer: dict[str, Any],
    merged: dict[str, Any],
    source_trace: dict[str, str],
) -> list[dict[str, Any]]:
    baseline = _flatten_values(base_layer)
    final = _flatten_values(merged)
    rows: list[dict[str, Any]] = []
    for path, new_value in final.items():
        old_value = baseline.get(path, "<missing>")
        if old_value == new_value:
            continue
        rows.append(
            {
                "path": path,
                "base_value": old_value,
                "final_value": new_value,
                "source": source_trace.get(path, "base"),
            }
        )
    rows.sort(key=lambda item: str(item["path"]))
    return rows


def resolve_runtime_config(
    *,
    base_layer: dict[str, Any] | None = None,
    env_override_layer: dict[str, Any] | None = None,
    runtime_flags_layer: dict[str, Any] | None = None,
    env_var_name: str = ENV_OVERRIDE_KEY,
) -> ResolvedRuntimeConfig:
    base = _ensure_mapping("base", base_layer if base_layer is not None else _default_base_config())
    env_override = (
        _ensure_mapping("env_override", env_override_layer)
        if env_override_layer is not None
        else _parse_env_override(env_var_name)
    )
    runtime_flags = _ensure_mapping("runtime_flags", runtime_flags_layer)

    source_trace: dict[str, str] = {}
    merged = _deep_merge_with_trace({}, base, layer_name="base", source_trace=source_trace)
    merged = _deep_merge_with_trace(merged, env_override, layer_name="env_override", source_trace=source_trace)
    merged = _deep_merge_with_trace(merged, runtime_flags, layer_name="runtime_flags", source_trace=source_trace)

    try:
        parsed = RuntimeConfig.model_validate(merged)
    except ValidationError as exc:
        _raise_validation_error(exc)

    diff_summary = _build_diff_summary(base_layer=base, merged=merged, source_trace=source_trace)
    return ResolvedRuntimeConfig(
        config=parsed,
        merged=merged,
        source_trace=source_trace,
        diff_summary=diff_summary,
    )
