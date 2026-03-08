"""Result assembly helpers extracted from workflow_engine."""

from __future__ import annotations

from typing import Any

from core.models import DataBundleRef, ProvenanceEntry, SandboxArtifacts


def build_data_bundle_ref(bundle: dict[str, Any] | None, *, symbol: str, interval: str) -> DataBundleRef:
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


def build_metrics(
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


def build_provenance(
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
