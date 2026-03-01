from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field


class StockData(BaseModel):
    symbol: str = Field(..., description="代码")
    price: float = Field(..., description="价格")
    change: float = Field(..., description="涨跌幅")
    volume: int = Field(..., description="成交量")
    timestamp: datetime = Field(..., description="时间戳")


class AnalysisTask(BaseModel):
    task_id: str
    instruction: str = Field(..., description="用户原始指令")
    code_generated: str = Field(..., description="生成的代码")
    status: str = Field(..., description="状态")


class TraceFrame(BaseModel):
    file: str
    line: int
    function: str


class TracebackInfo(BaseModel):
    error_type: str
    message: str
    frames: list[TraceFrame] = Field(default_factory=list)
    raw: str


class DataBundle(BaseModel):
    records: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    data_source: str = "api"
    asof: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    symbol: str = ""
    market: str = "auto"
    interval: str = "1d"

    def to_serializable_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class ResearchPlan(BaseModel):
    provider: str
    data_source: str
    steps: list[str]
    reason: str


class DataBundleRef(BaseModel):
    data_source: str
    asof: str
    symbol: str
    market: str
    interval: str
    record_count: int


class SandboxArtifacts(BaseModel):
    code: str
    stdout: str
    stderr: str
    backend: str
    duration_ms: float = 0.0
    resource_usage: dict[str, Any] | None = None
    retry_count: int
    success: bool
    traceback: dict[str, Any] | None


class FusedInsights(BaseModel):
    summary: str
    analysis_steps: list[str]
    raw: dict[str, Any]


class ProvenanceEntry(BaseModel):
    metric: str
    value: Any
    source: Literal["sandbox_stdout", "sandbox_metrics", "fused_metrics", "data_bundle"]
    pointer: str
    note: str = ""


class ResearchResult(BaseModel):
    run_id: str
    request: str
    symbol: str
    period: str
    created_at: datetime
    plan: ResearchPlan
    data_bundle_ref: DataBundleRef
    sandbox_artifacts: SandboxArtifacts
    fused_insights: FusedInsights
    metrics: dict[str, Any]
    provenance: list[ProvenanceEntry]


class AlertSignalSnapshot(BaseModel):
    symbol: str
    company_name: str = ""
    timestamp: datetime
    price: float
    pct_change: float
    rsi: float
    priority: Literal["critical", "high", "normal"]
    reason: str


class AlertSnapshot(BaseModel):
    snapshot_id: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    trigger_type: Literal["scheduled", "event"]
    trigger_id: str
    trigger_time: datetime
    trigger_metadata: dict[str, Any] = Field(default_factory=dict)
    mode: Literal["anomaly", "digest"] = "anomaly"
    signal: AlertSignalSnapshot
    notification_channels: list[str] = Field(default_factory=list)
    notification_dispatched: bool = False
    research_status: Literal["skipped", "triggered", "failed"] = "skipped"
    research_run_id: str | None = None
    research_result: ResearchResult | None = None
    research_error: str = ""


class AgentState(BaseModel):
    request: str
    symbol: str = "AAPL"
    period: str = "1mo"
    fallback_url: str | None = None

    route: Literal["api", "scraper", "done"] = "api"
    market_data: list[dict] = Field(default_factory=list)
    scraped_data: str | None = None

    sandbox_code: str | None = None
    sandbox_stdout: str | None = None
    sandbox_stderr: str | None = None
    traceback: TracebackInfo | None = None

    telegram_message_sent: bool = False
    telegram_image_sent: bool = False


class Week2AgentState(BaseModel):
    request: str
    symbol: str = "AAPL"
    period: str = "1mo"

    plan_steps: list[str] = Field(default_factory=list)
    data_source: Literal["api", "scraper"] = "api"
    planner_reason: str = ""
    planner_provider: str = "fallback"
    market_data_bundle: DataBundle | None = None

    sandbox_code: str | None = None
    sandbox_stdout: str | None = None
    sandbox_stderr: str | None = None
    traceback: TracebackInfo | None = None

    debug_advice: str = ""
    retry_count: int = 0
    max_retries: int = 2
    success: bool = False


class Week3AgentState(BaseModel):
    request: str
    symbol: str = "AAPL"
    period: str = "6mo"

    sentiment_score: float = 50.0
    sentiment_text: str = ""

    sandbox_code: str | None = None
    sandbox_stdout: str | None = None
    sandbox_stderr: str | None = None
    traceback: TracebackInfo | None = None
    output_files: list[str] = Field(default_factory=list)

    metrics: dict = Field(default_factory=dict)
    metrics_history: list[dict] = Field(default_factory=list)
    report_markdown: str = ""
    transfer_payload: list[dict] = Field(default_factory=list)

    recommendation: str = "HOLD"
    hitl_status: str = "not_required"
    human_approved: bool = False
