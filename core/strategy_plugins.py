"""Strategy plugin layer with isolation shell for Upgrade7 P1-C1."""

from __future__ import annotations

import asyncio
import copy
import importlib
import inspect
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

from core.runtime_config import (
    ConfigValidationIssue,
    RuntimeConfig,
    RuntimeConfigValidationError,
    StrategyPluginConfig,
)


class PluginKind(str, Enum):
    SIGNALS = "signals"
    ALERTS = "alerts"
    POLICIES = "policies"


@dataclass(frozen=True)
class PluginContext:
    request_id: str = ""
    run_id: str = ""
    trigger_id: str = ""
    observability_tags: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class PluginAuditRecord:
    plugin_id: str
    plugin_name: str
    kind: str
    status: str
    duration_ms: float
    degraded: bool
    error: str = ""
    output_keys: list[str] = field(default_factory=list)
    observability_tags: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "plugin_id": self.plugin_id,
            "plugin_name": self.plugin_name,
            "kind": self.kind,
            "status": self.status,
            "duration_ms": round(self.duration_ms, 3),
            "degraded": self.degraded,
            "error": self.error,
            "output_keys": self.output_keys,
            "observability_tags": self.observability_tags,
        }


class StrategyPlugin(Protocol):
    name: str
    kind: str

    async def execute(
        self,
        payload: dict[str, Any],
        *,
        params: dict[str, Any],
        context: PluginContext,
    ) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class _LoadedPlugin:
    plugin_id: str
    kind: PluginKind
    timeout_ms: int
    params: dict[str, Any]
    instance: StrategyPlugin


class StrategyPluginManager:
    def __init__(self, runtime_config: RuntimeConfig):
        self._runtime_config = runtime_config
        self._plugins: dict[PluginKind, list[_LoadedPlugin]] = {
            PluginKind.SIGNALS: [],
            PluginKind.ALERTS: [],
            PluginKind.POLICIES: [],
        }
        self._load_plugins()

    @classmethod
    def from_runtime_config(cls, runtime_config: RuntimeConfig) -> "StrategyPluginManager":
        return cls(runtime_config)

    def _load_plugins(self) -> None:
        plugin_layers: dict[PluginKind, dict[str, StrategyPluginConfig]] = {
            PluginKind.SIGNALS: self._runtime_config.strategy_plugins.signals,
            PluginKind.ALERTS: self._runtime_config.strategy_plugins.alerts,
            PluginKind.POLICIES: self._runtime_config.strategy_plugins.policies,
        }
        issues: list[ConfigValidationIssue] = []
        for kind, mapping in plugin_layers.items():
            for plugin_id, config in mapping.items():
                if not config.enabled:
                    continue
                try:
                    module = importlib.import_module(config.module)
                    plugin_cls = getattr(module, config.class_name)
                    instance = plugin_cls()
                except Exception as exc:
                    issues.append(
                        ConfigValidationIssue(
                            path=f"strategy_plugins.{kind.value}.{plugin_id}",
                            message=f"failed to import plugin `{config.module}.{config.class_name}`: {exc}",
                            suggestion="Fix module/class path or disable this plugin.",
                        )
                    )
                    continue
                self._plugins[kind].append(
                    _LoadedPlugin(
                        plugin_id=plugin_id,
                        kind=kind,
                        timeout_ms=int(config.timeout_ms),
                        params=copy.deepcopy(config.params),
                        instance=instance,
                    )
                )
        if issues:
            raise RuntimeConfigValidationError(issues)

    @staticmethod
    async def _run_plugin(
        plugin: StrategyPlugin,
        *,
        payload: dict[str, Any],
        params: dict[str, Any],
        context: PluginContext,
    ) -> dict[str, Any]:
        result = plugin.execute(payload, params=params, context=context)
        if inspect.isawaitable(result):
            result = await result
        if not isinstance(result, dict):
            raise TypeError(f"plugin `{plugin.name}` must return dict, got {type(result).__name__}")
        return result

    async def apply(
        self,
        *,
        kind: PluginKind,
        payload: dict[str, Any],
        context: PluginContext,
    ) -> tuple[dict[str, Any], list[PluginAuditRecord]]:
        current_payload = copy.deepcopy(payload)
        audits: list[PluginAuditRecord] = []
        for loaded in self._plugins.get(kind, []):
            started = time.perf_counter()
            base_tags = {
                **context.observability_tags,
                "plugin_id": loaded.plugin_id,
                "plugin_kind": kind.value,
            }
            try:
                output = await asyncio.wait_for(
                    self._run_plugin(
                        loaded.instance,
                        payload=current_payload,
                        params=copy.deepcopy(loaded.params),
                        context=context,
                    ),
                    timeout=max(0.05, loaded.timeout_ms / 1000.0),
                )
                plugin_tags_raw = output.pop("observability_tags", {})
                plugin_tags = plugin_tags_raw if isinstance(plugin_tags_raw, dict) else {}
                current_payload.update(output)
                audits.append(
                    PluginAuditRecord(
                        plugin_id=loaded.plugin_id,
                        plugin_name=str(getattr(loaded.instance, "name", loaded.plugin_id)),
                        kind=kind.value,
                        status="ok",
                        duration_ms=(time.perf_counter() - started) * 1000,
                        degraded=False,
                        output_keys=sorted(output.keys()),
                        observability_tags={**base_tags, **{str(k): str(v) for k, v in plugin_tags.items()}},
                    )
                )
            except asyncio.TimeoutError:
                audits.append(
                    PluginAuditRecord(
                        plugin_id=loaded.plugin_id,
                        plugin_name=str(getattr(loaded.instance, "name", loaded.plugin_id)),
                        kind=kind.value,
                        status="timeout",
                        duration_ms=(time.perf_counter() - started) * 1000,
                        degraded=True,
                        error=f"plugin timeout after {loaded.timeout_ms}ms",
                        observability_tags={**base_tags, "plugin_status": "timeout"},
                    )
                )
            except Exception as exc:  # pragma: no cover - defensive shell.
                audits.append(
                    PluginAuditRecord(
                        plugin_id=loaded.plugin_id,
                        plugin_name=str(getattr(loaded.instance, "name", loaded.plugin_id)),
                        kind=kind.value,
                        status="error",
                        duration_ms=(time.perf_counter() - started) * 1000,
                        degraded=True,
                        error=str(exc),
                        observability_tags={**base_tags, "plugin_status": "error"},
                    )
                )
        return current_payload, audits

    def loading_matrix(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for kind in (PluginKind.SIGNALS, PluginKind.ALERTS, PluginKind.POLICIES):
            for item in self._plugins[kind]:
                rows.append(
                    {
                        "kind": kind.value,
                        "plugin_id": item.plugin_id,
                        "plugin_name": str(getattr(item.instance, "name", item.plugin_id)),
                        "timeout_ms": item.timeout_ms,
                        "params": copy.deepcopy(item.params),
                    }
                )
        rows.sort(key=lambda row: (str(row["kind"]), str(row["plugin_id"])))
        return rows
