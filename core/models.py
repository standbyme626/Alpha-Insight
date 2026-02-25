from __future__ import annotations

from datetime import datetime
from typing import Literal

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
