"""Week2 LangGraph workflow: Planner -> Coder -> Executor -> Debugger(loop)."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, TypedDict
from uuid import uuid4

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, StateGraph

from agents.coder_engine import generate_code
from agents.debugger_engine import build_debug_advice
from agents.planner_engine import plan_tasks
from agents.report_reviewer import extract_metrics_from_stdout
from core.fault_injection import FaultInjectionEvent, FaultInjector, fault_semantic
from core.models import DataBundleRef, FusedInsights, ProvenanceEntry, ResearchPlan, ResearchResult, SandboxArtifacts
from core.node_contracts import (
    NodeErrorCode,
    RetryDecision,
    classify_node_error_code,
    resolve_retry_decision,
    validate_coder_contract,
    validate_debugger_contract,
    validate_executor_contract,
    validate_planner_contract,
)
from core.observability import FailureEvent, aggregate_failure_clusters, aggregate_failure_tags, classify_failure
from core.reliability_budget import evaluate_latency_error_budget
from core.sandbox_manager import ExecutionResult, SandboxManager, StructuredTraceback
from core.tool_result import build_tool_result
from tools.market_data import fetch_market_data, market_data_result_to_tool_result


class Week2GraphState(TypedDict, total=False):
    request: str
    symbol: str
    period: str
    interval: str
    need_chart: bool

    plan_steps: list[str]
    data_source: str
    planner_reason: str
    planner_provider: str
    market_data_bundle: dict[str, Any]
    market_data_tool_result: dict[str, Any] | None
    data_fetch_message: str

    sandbox_code: str
    sandbox_stdout: str
    sandbox_stderr: str
    sandbox_backend: str
    sandbox_duration_ms: float
    sandbox_resource_usage: dict[str, Any] | None
    sandbox_images: list[str]
    sandbox_output_files: list[str]
    traceback: dict[str, Any] | None

    debug_advice: str
    retry_count: int
    max_retries: int
    success: bool
    inject_failure: bool
    market_data_latency_ms: float
    executor_latency_ms: float
    fallback_used: bool
    failure_events: list[dict[str, Any]]
    fault_injection: dict[str, Any]
    fault_injection_enabled: bool
    fault_injection_events: list[dict[str, Any]]


def _fault_injector_from_state(state: Week2GraphState) -> FaultInjector:
    payload = state.get("fault_injection")
    if not isinstance(payload, dict):
        return FaultInjector.disabled()
    return FaultInjector.from_payload(payload)


def _fault_error_to_code(fault: str) -> NodeErrorCode:
    mapping = {
        "timeout": NodeErrorCode.TIMEOUT,
        "rate_limit": NodeErrorCode.RATE_LIMIT,
        "upstream_5xx": NodeErrorCode.NETWORK,
        "parse": NodeErrorCode.DATA,
        "sandbox_failure": NodeErrorCode.SANDBOX,
    }
    return mapping.get(fault, NodeErrorCode.UNKNOWN)


def _fault_failure_message(event: FaultInjectionEvent, *, node: str) -> str:
    return fault_semantic(event.fault, node=node).message


def _fault_traceback_payload(event: FaultInjectionEvent, *, node: str) -> dict[str, Any]:
    semantic = fault_semantic(event.fault, node=node)
    return {
        "error_type": semantic.error_type,
        "message": semantic.message,
        "frames": [],
        "raw": semantic.message,
        "error_code": _fault_error_to_code(event.fault).value,
    }


async def planner_node(state: Week2GraphState) -> Week2GraphState:
    print("[DEBUG] QuantNode week2.planner_node Start")
    plan = await plan_tasks(state.get("request", ""))
    payload = {
        "plan_steps": plan.steps,
        "data_source": plan.data_source,
        "planner_reason": plan.reason,
        "planner_provider": plan.provider,
        "interval": str(state.get("interval", "1d")),
        "need_chart": bool(state.get("need_chart", False)),
        "retry_count": int(state.get("retry_count", 0)),
        "max_retries": int(state.get("max_retries", 2)),
    }
    return validate_planner_contract(dict(state), payload)


async def market_data_node(state: Week2GraphState) -> Week2GraphState:
    print("[DEBUG] QuantNode week2.market_data_node Start")
    started_at = time.perf_counter()
    symbol = str(state.get("symbol", "AAPL"))
    period = str(state.get("period", "1mo"))
    interval = str(state.get("interval", "1d"))
    fault_events = list(state.get("fault_injection_events", []))
    injector = _fault_injector_from_state(state)
    injected = injector.maybe_inject(
        node="workflow.market_data",
        allowed_faults=("timeout", "upstream_5xx", "parse", "rate_limit"),
    )
    if injected is not None:
        fault_events.append(injected.to_dict())
        semantic = fault_semantic(injected.fault, node="workflow.market_data")
        message = semantic.message
        failure = classify_failure(
            source="workflow.market_data",
            error_type=semantic.error_type,
            message=message,
            backend="fault_injection",
        )
        latency_ms = (time.perf_counter() - started_at) * 1000
        tool_result = build_tool_result(
            source="market_data:fault_injection",
            confidence=0.0,
            raw={"symbol": symbol, "period": period, "interval": interval},
            error=message,
            meta={"fault_injection": injected.to_dict()},
        )
        return {
            "data_fetch_message": message,
            "market_data_tool_result": tool_result.to_dict(),
            "traceback": _fault_traceback_payload(injected, node="workflow.market_data"),
            "success": False,
            "sandbox_stdout": "",
            "sandbox_stderr": message,
            "market_data_latency_ms": latency_ms,
            "failure_events": [*list(state.get("failure_events", [])), failure.to_dict()],
            "fault_injection_enabled": True,
            "fault_injection_events": fault_events,
        }
    result = await fetch_market_data(symbol, period=period, interval=interval)
    latency_ms = (time.perf_counter() - started_at) * 1000

    if not result.ok or not result.records:
        tool_result = market_data_result_to_tool_result(result, period=period, interval=interval)
        failure = classify_failure(
            source="workflow.market_data",
            error_type="DataFetchError",
            message=tool_result.error or result.message,
        )
        return {
            "data_fetch_message": result.message,
            "market_data_tool_result": tool_result.to_dict(),
            "traceback": {
                "error_type": "DataFetchError",
                "message": result.message,
                "frames": [],
                "raw": result.message,
                "error_code": NodeErrorCode.DATA.value,
            },
            "success": False,
            "sandbox_stdout": "",
            "sandbox_stderr": result.message,
            "market_data_latency_ms": latency_ms,
            "failure_events": [failure.to_dict()],
            "fault_injection_enabled": bool(injector.enabled),
            "fault_injection_events": fault_events,
        }

    bundle_payload = result.bundle.to_serializable_dict() if result.bundle else {
        "records": result.records,
        "metadata": {"period": period, "record_count": len(result.records)},
        "data_source": "api",
        "symbol": result.symbol or symbol,
        "market": "auto",
        "interval": interval,
    }
    return {
        "market_data_bundle": bundle_payload,
        "market_data_tool_result": market_data_result_to_tool_result(result, period=period, interval=interval).to_dict(),
        "data_fetch_message": result.message,
        "traceback": None,
        "market_data_latency_ms": latency_ms,
        "failure_events": list(state.get("failure_events", [])),
        "fault_injection_enabled": bool(injector.enabled),
        "fault_injection_events": fault_events,
    }


async def coder_node(state: Week2GraphState) -> Week2GraphState:
    print("[DEBUG] QuantNode week2.coder_node Start")
    if state.get("traceback"):
        return validate_coder_contract(dict(state), {})
    code = generate_code(state)
    return validate_coder_contract(dict(state), {"sandbox_code": code})


async def executor_node(state: Week2GraphState) -> Week2GraphState:
    print("[DEBUG] QuantNode week2.executor_node Start")
    code = state.get("sandbox_code", "")
    injector = _fault_injector_from_state(state)
    fault_events = list(state.get("fault_injection_events", []))
    manager = SandboxManager(fault_injector=injector)
    await manager.create_session()
    try:
        injected = injector.maybe_inject(
            node="workflow.executor",
            allowed_faults=("timeout", "sandbox_failure"),
        )
        if injected is not None:
            fault_events.append(injected.to_dict())
            semantic = fault_semantic(injected.fault, node="workflow.executor")
            traceback = StructuredTraceback(
                error_type=semantic.error_type,
                message=semantic.message,
                frames=[],
                raw=semantic.message,
            )
            result = ExecutionResult(
                stdout="",
                stderr=traceback.raw,
                exit_code=124 if injected.fault == "timeout" else 1,
                traceback=traceback,
                backend="workflow-executor:fault-injected",
                duration_ms=0.0,
                resource_usage={"fault_injection": injected.to_dict()},
            )
        else:
            result = await manager.execute(code)
    finally:
        await manager.destroy_session()
    latency_ms = float(getattr(result, "duration_ms", 0.0))

    tb = None
    if result.traceback:
        error_code = classify_node_error_code(
            error_type=result.traceback.error_type,
            message=result.traceback.message,
            backend=str(getattr(result, "backend", getattr(result, "execution_backend", "unknown"))),
        )
        tb = {
            "error_type": result.traceback.error_type,
            "message": result.traceback.message,
            "frames": result.traceback.frames,
            "raw": result.traceback.raw,
            "error_code": error_code.value,
        }

    backend = str(getattr(result, "backend", getattr(result, "execution_backend", "unknown")))
    backend_lower = backend.lower()
    used_fallback = "fallback" in backend_lower or "local-process" in backend_lower
    failure_events = list(state.get("failure_events", []))
    resource_usage = getattr(result, "resource_usage", None)
    if isinstance(resource_usage, dict):
        raw_event = resource_usage.get("fault_injection")
        if isinstance(raw_event, dict):
            if raw_event not in fault_events:
                fault_events.append(raw_event)
    if tb:
        failure = classify_failure(
            source="workflow.executor",
            error_type=str(tb.get("error_type", "RuntimeError")),
            message=str(tb.get("message", "")),
            backend=backend,
        )
        failure_events.append(failure.to_dict())

    retry_count = int(state.get("retry_count", 0))
    if tb:
        retry_count += 1

    payload = {
        "sandbox_stdout": result.stdout,
        "sandbox_stderr": result.stderr,
        "sandbox_backend": backend,
        "sandbox_duration_ms": float(getattr(result, "duration_ms", latency_ms)),
        "sandbox_resource_usage": resource_usage if isinstance(resource_usage, dict) else None,
        "sandbox_images": list(getattr(result, "images", []) or []),
        "sandbox_output_files": list(getattr(result, "output_files", []) or []),
        "traceback": tb,
        "retry_count": retry_count,
        "success": tb is None,
        "executor_latency_ms": latency_ms,
        "fallback_used": used_fallback,
        "failure_events": failure_events,
        "fault_injection_enabled": bool(injector.enabled),
        "fault_injection_events": fault_events,
    }
    validated = validate_executor_contract(dict(state), payload)
    validated["fault_injection_enabled"] = bool(injector.enabled)
    validated["fault_injection_events"] = fault_events
    return validated


async def debugger_node(state: Week2GraphState) -> Week2GraphState:
    print("[DEBUG] QuantNode week2.debugger_node Start")
    advice = build_debug_advice(state.get("traceback"))
    return validate_debugger_contract(dict(state), {"debug_advice": advice})


def _after_market_data(state: Week2GraphState) -> str:
    if state.get("traceback") is not None:
        return "done"
    return "coder"


def _after_executor(state: Week2GraphState) -> str:
    decision = resolve_retry_decision(
        traceback=state.get("traceback"),
        retry_count=int(state.get("retry_count", 0)),
        max_retries=int(state.get("max_retries", 2)),
    )
    if decision == RetryDecision.SUCCESS:
        return "done"
    if decision == RetryDecision.RETRY:
        return "debugger"
    return "done"


def build_repair_graph(*, checkpointer: InMemorySaver | None = None):
    print("[DEBUG] QuantNode week2.build_week2_graph Start")
    graph = StateGraph(Week2GraphState)
    graph.add_node("planner", planner_node)
    graph.add_node("market_data", market_data_node)
    graph.add_node("coder", coder_node)
    graph.add_node("executor", executor_node)
    graph.add_node("debugger", debugger_node)

    graph.set_entry_point("planner")
    graph.add_edge("planner", "market_data")
    graph.add_conditional_edges(
        "market_data",
        _after_market_data,
        {
            "coder": "coder",
            "done": END,
        },
    )
    graph.add_edge("coder", "executor")
    graph.add_conditional_edges(
        "executor",
        _after_executor,
        {
            "debugger": "debugger",
            "done": END,
        },
    )
    graph.add_edge("debugger", "coder")

    cp = checkpointer if checkpointer is not None else InMemorySaver()
    return graph.compile(checkpointer=cp)


# Backward-compatible alias.
build_week2_graph = build_repair_graph


def _build_data_bundle_ref(bundle: dict[str, Any] | None, *, symbol: str, interval: str) -> DataBundleRef:
    payload = bundle or {}
    metadata = payload.get("metadata")
    metadata_dict = metadata if isinstance(metadata, dict) else {}
    records = payload.get("records")
    record_count = int(metadata_dict.get("record_count") or (len(records) if isinstance(records, list) else 0))
    return DataBundleRef(
        data_source=str(payload.get("data_source", "unknown")),
        asof=str(payload.get("asof", "")),
        symbol=str(payload.get("symbol", symbol)),
        market=str(payload.get("market", "auto")),
        interval=str(payload.get("interval", interval)),
        record_count=record_count,
    )


def _build_metrics(
    *,
    sandbox: SandboxArtifacts,
    fused_raw: dict[str, Any],
    sandbox_metrics: dict[str, Any],
    data_bundle_ref: DataBundleRef,
) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "full_success": bool(sandbox.success),
        "retry_count": int(sandbox.retry_count),
        "data_record_count": int(data_bundle_ref.record_count),
        "latest_close": float(fused_raw.get("latest_close", 0.0)),
        "period_change_pct": float(fused_raw.get("period_change_pct", 0.0)),
        "ma20": float(fused_raw.get("ma20", 0.0)),
        "rsi14": float(fused_raw.get("rsi14", 0.0)),
        "volatility_pct": float(fused_raw.get("volatility_pct", 0.0)),
        "volume_ratio": float(fused_raw.get("volume_ratio", 0.0)),
        "sentiment_score": float(fused_raw.get("sentiment_score", 0.0)),
    }
    for key, value in sandbox_metrics.items():
        metrics[f"sandbox_{key}"] = value
    return metrics


def _build_provenance(
    *,
    sandbox: SandboxArtifacts,
    data_bundle_ref: DataBundleRef,
    fused_raw: dict[str, Any],
    sandbox_metrics: dict[str, Any],
) -> list[ProvenanceEntry]:
    entries: list[ProvenanceEntry] = [
        ProvenanceEntry(
            metric="data_record_count",
            value=data_bundle_ref.record_count,
            source="data_bundle",
            pointer="data_bundle_ref.record_count",
            note="source market rows used by full + fused compute",
        ),
        ProvenanceEntry(
            metric="retry_count",
            value=sandbox.retry_count,
            source="sandbox_metrics",
            pointer="sandbox_artifacts.retry_count",
        ),
        ProvenanceEntry(
            metric="sandbox_success",
            value=sandbox.success,
            source="sandbox_metrics",
            pointer="sandbox_artifacts.success",
        ),
    ]

    fused_metric_keys = [
        "latest_close",
        "period_change_pct",
        "ma20",
        "rsi14",
        "volatility_pct",
        "volume_ratio",
        "sentiment_score",
    ]
    for key in fused_metric_keys:
        if key in fused_raw:
            entries.append(
                ProvenanceEntry(
                    metric=key,
                    value=fused_raw.get(key),
                    source="fused_metrics",
                    pointer=f"fused_insights.raw.{key}",
                )
            )

    for key, value in sandbox_metrics.items():
        entries.append(
            ProvenanceEntry(
                metric=f"sandbox_{key}",
                value=value,
                source="sandbox_stdout",
                pointer=f"sandbox_artifacts.stdout::METRICS_JSON.{key}",
            )
        )
    return entries


async def run_unified_research(
    *,
    request: str,
    symbol: str,
    period: str = "1mo",
    interval: str = "1d",
    max_retries: int = 2,
    news_limit: int = 8,
    need_chart: bool = False,
    fault_injection: dict[str, Any] | None = None,
) -> dict[str, Any]:
    run_id = f"run-{uuid4().hex[:12]}"
    app = build_week2_graph()
    fault_payload = dict(fault_injection or {}) if isinstance(fault_injection, dict) else {}
    fault_enabled = bool(FaultInjector.from_payload(fault_payload).enabled)
    full_output = await app.ainvoke(
        {
            "request": request,
            "symbol": symbol.strip().upper(),
            "period": period.strip(),
            "interval": interval.strip(),
            "need_chart": bool(need_chart),
            "max_retries": max_retries,
            "fault_injection": fault_payload,
            "fault_injection_enabled": fault_enabled,
            "fault_injection_events": [],
        },
        config={"configurable": {"thread_id": run_id}},
    )

    plan = ResearchPlan(
        provider=str(full_output.get("planner_provider", "unknown")),
        data_source=str(full_output.get("data_source", "unknown")),
        steps=[str(step) for step in full_output.get("plan_steps", [])],
        reason=str(full_output.get("planner_reason", "")),
    )
    bundle_payload = full_output.get("market_data_bundle")
    if not isinstance(bundle_payload, dict):
        bundle_payload = {}
    data_bundle_ref = _build_data_bundle_ref(bundle_payload, symbol=symbol, interval=interval)

    sandbox = SandboxArtifacts(
        code=str(full_output.get("sandbox_code", "")),
        stdout=str(full_output.get("sandbox_stdout", "")),
        stderr=str(full_output.get("sandbox_stderr", "")),
        backend=str(full_output.get("sandbox_backend", "unknown")),
        duration_ms=float(full_output.get("sandbox_duration_ms", full_output.get("executor_latency_ms", 0.0))),
        resource_usage=full_output.get("sandbox_resource_usage")
        if isinstance(full_output.get("sandbox_resource_usage"), dict)
        else None,
        retry_count=int(full_output.get("retry_count", 0)),
        success=bool(full_output.get("success", False)),
        traceback=full_output.get("traceback"),
    )

    from agents.market_news_engine import run_market_news_analysis

    fused_raw = await run_market_news_analysis(
        request=request,
        symbol=symbol,
        period=period,
        interval=interval,
        news_limit=news_limit,
        market_data_bundle=bundle_payload,
    )

    fused = FusedInsights(
        summary=str(fused_raw.get("final_assessment", "")),
        analysis_steps=[str(step) for step in fused_raw.get("analysis_steps", [])],
        raw=fused_raw,
    )
    sandbox_metrics = extract_metrics_from_stdout(sandbox.stdout) or {}
    failure_events_raw = full_output.get("failure_events", [])
    failure_events: list[FailureEvent] = []
    if isinstance(failure_events_raw, list):
        for item in failure_events_raw:
            if isinstance(item, dict):
                failure_events.append(FailureEvent.from_dict(item))
    metrics = _build_metrics(
        sandbox=sandbox,
        fused_raw=fused_raw,
        sandbox_metrics=sandbox_metrics,
        data_bundle_ref=data_bundle_ref,
    )
    metrics["runtime_market_data_latency_ms"] = round(float(full_output.get("market_data_latency_ms", 0.0)), 3)
    metrics["runtime_executor_latency_ms"] = round(float(full_output.get("executor_latency_ms", 0.0)), 3)
    metrics["runtime_fallback_used"] = bool(full_output.get("fallback_used", False))
    metrics["runtime_retry_count"] = int(sandbox.retry_count)
    metrics["runtime_success"] = bool(sandbox.success)
    metrics["runtime_failure_count"] = len(failure_events)
    metrics["runtime_failure_clusters"] = aggregate_failure_clusters(failure_events)
    metrics["runtime_failure_tags"] = aggregate_failure_tags(failure_events)
    fault_events_raw = full_output.get("fault_injection_events", [])
    fault_events = [item for item in fault_events_raw if isinstance(item, dict)]
    metrics["runtime_fault_injection_enabled"] = bool(full_output.get("fault_injection_enabled", fault_enabled))
    metrics["runtime_fault_injection_count"] = len(fault_events)
    if fault_events:
        metrics["runtime_fault_injection_events"] = fault_events
    budget = evaluate_latency_error_budget(
        latency_samples_ms=[metrics["runtime_market_data_latency_ms"], metrics["runtime_executor_latency_ms"]],
        error_count=metrics["runtime_failure_count"],
        total_count=2,
        fallback_count=1 if metrics["runtime_fallback_used"] else 0,
        retry_count=metrics["runtime_retry_count"],
    )
    metrics["runtime_budget_verdict"] = budget.status
    metrics["runtime_budget_reasons"] = [item.to_dict() for item in budget.reasons]
    metrics["runtime_latency_p50_ms"] = budget.metrics["p50_latency_ms"]
    metrics["runtime_latency_p95_ms"] = budget.metrics["p95_latency_ms"]
    metrics["runtime_error_rate"] = budget.metrics["error_rate"]
    metrics["runtime_fallback_rate"] = budget.metrics["fallback_rate"]
    metrics["runtime_retry_pressure"] = budget.metrics["retry_pressure"]
    provenance = _build_provenance(
        sandbox=sandbox,
        data_bundle_ref=data_bundle_ref,
        fused_raw=fused_raw,
        sandbox_metrics=sandbox_metrics,
    )

    result = ResearchResult(
        run_id=run_id,
        request=request,
        symbol=symbol.strip().upper(),
        period=period.strip(),
        created_at=datetime.now(timezone.utc),
        plan=plan,
        data_bundle_ref=data_bundle_ref,
        sandbox_artifacts=sandbox,
        fused_insights=fused,
        metrics=metrics,
        provenance=provenance,
    )
    payload = result.model_dump(mode="json")
    tool_results: dict[str, Any] = {}
    market_tool_result = full_output.get("market_data_tool_result")
    if isinstance(market_tool_result, dict):
        tool_results["market_data"] = market_tool_result
    fused_market_tool_result = fused_raw.get("market_tool_result")
    if isinstance(fused_market_tool_result, dict):
        tool_results["market_analysis_market_data"] = fused_market_tool_result
    fused_news_tool_result = fused_raw.get("news_tool_result")
    if isinstance(fused_news_tool_result, dict):
        tool_results["news"] = fused_news_tool_result
    if tool_results:
        payload["tool_results"] = tool_results
    sandbox_output_files = [str(item) for item in full_output.get("sandbox_output_files", []) if str(item).strip()]
    sandbox_images = [str(item) for item in full_output.get("sandbox_images", []) if str(item).strip()]
    if sandbox_output_files:
        payload["sandbox_output_files"] = sandbox_output_files
    if sandbox_images:
        payload["sandbox_images"] = sandbox_images
    artifact_png = next(
        (
            item
            for item in [*sandbox_images, *sandbox_output_files]
            if str(item).strip().lower().endswith(".png")
        ),
        "",
    )
    if artifact_png:
        payload["artifact_png"] = artifact_png
    return payload
