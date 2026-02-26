"""Week2 planner: task decomposition + DeepSeek-R1 prompt contract."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

import aiohttp


PLANNER_SYSTEM_PROMPT = """
You are DeepSeek-R1 acting as the Planner in a quant multi-agent system.
Think step-by-step internally, but output only strict JSON.

Return schema:
{
  "steps": ["Data Fetch", "Logic Calc", "Plotting"],
  "data_source": "api" | "scraper",
  "reason": "short explanation"
}

Rules:
- Always keep execution-safe, deterministic tasks.
- Prefer API for market OHLCV unless request explicitly asks for web/news/sentiment pages.
- Ensure the pipeline always includes Data Fetch -> Logic Calc -> Plotting.
- Never output markdown.
""".strip()


SCRAPER_KEYWORDS = (
    "news",
    "reuters",
    "twitter",
    "x.com",
    "公告",
    "新闻",
    "网页",
    "舆情",
)


@dataclass
class PlanResult:
    steps: list[str]
    data_source: str
    reason: str
    provider: str


@dataclass
class PlannerLLMConfig:
    api_key: str
    base_url: str
    model: str
    temperature: float
    enable_local_fallback: bool
    enable_thinking: bool | None


def route_data_source(request: str) -> str:
    lower = request.lower()
    if any(keyword in lower for keyword in SCRAPER_KEYWORDS):
        return "scraper"
    return "api"


def build_fallback_plan(request: str) -> PlanResult:
    source = route_data_source(request)
    return PlanResult(
        steps=["Data Fetch", "Logic Calc", "Plotting"],
        data_source=source,
        reason="fallback planner used local heuristic routing",
        provider="fallback",
    )


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _load_config() -> PlannerLLMConfig | None:
    api_key = os.getenv("OPENAI_API_KEY") or os.getenv("DEEPSEEK_API_KEY")
    enable_local_fallback = _env_bool("ENABLE_LOCAL_FALLBACK", True)
    if not api_key:
        return None

    base_url = os.getenv("OPENAI_API_BASE", "https://api.deepseek.com/v1").strip()
    model = os.getenv("OPENAI_MODEL_NAME", "deepseek-reasoner").strip()
    temperature_raw = os.getenv("TEMPERATURE", "0.2").strip()
    try:
        temperature = float(temperature_raw)
    except ValueError:
        temperature = 0.2

    thinking_raw = os.getenv("OPENAI_ENABLE_THINKING")
    enable_thinking: bool | None
    if thinking_raw is None:
        enable_thinking = None
    else:
        enable_thinking = thinking_raw.strip().lower() in {"1", "true", "yes", "on"}

    return PlannerLLMConfig(
        api_key=api_key,
        base_url=base_url.rstrip("/"),
        model=model,
        temperature=temperature,
        enable_local_fallback=enable_local_fallback,
        enable_thinking=enable_thinking,
    )


def _extract_json_object(text: str) -> dict | None:
    content = text.strip()
    if not content:
        return None
    try:
        parsed = json.loads(content)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass

    start = content.find("{")
    end = content.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    candidate = content[start : end + 1]
    try:
        parsed = json.loads(candidate)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


async def _call_remote_planner(request: str, cfg: PlannerLLMConfig, *, timeout_seconds: int = 30) -> PlanResult | None:

    payload = {
        "model": cfg.model,
        "messages": [
            {"role": "system", "content": PLANNER_SYSTEM_PROMPT},
            {"role": "user", "content": request},
        ],
        "temperature": cfg.temperature,
    }
    # DashScope Qwen3 compatible endpoint requires explicit non-thinking mode for non-streaming calls.
    if cfg.enable_thinking is not None:
        payload["enable_thinking"] = cfg.enable_thinking
    elif "dashscope.aliyuncs.com" in cfg.base_url or cfg.model.lower().startswith("qwen3"):
        payload["enable_thinking"] = False

    headers = {
        "Authorization": f"Bearer {cfg.api_key}",
        "Content-Type": "application/json",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{cfg.base_url}/chat/completions",
                headers=headers,
                json=payload,
                timeout=timeout_seconds,
            ) as response:
                if response.status >= 400:
                    return None
                body = await response.json()
    except Exception:
        return None

    try:
        content = body["choices"][0]["message"]["content"]
        parsed = _extract_json_object(content)
        if parsed is None:
            return None
        steps = parsed.get("steps") or []
        source = parsed.get("data_source") or route_data_source(request)
        reason = parsed.get("reason") or "deepseek-r1 planner output"
        if not isinstance(steps, list) or len(steps) < 3:
            return None
        return PlanResult(
            steps=[str(step) for step in steps],
            data_source=str(source),
            reason=str(reason),
            provider=cfg.model,
        )
    except Exception:
        return None


async def plan_tasks(request: str) -> PlanResult:
    cfg = _load_config()
    remote = None
    if cfg is not None:
        remote = await _call_remote_planner(request, cfg)
    if remote is not None:
        return remote
    if cfg is not None and not cfg.enable_local_fallback:
        raise RuntimeError("Remote planner failed and ENABLE_LOCAL_FALLBACK=false.")
    return build_fallback_plan(request)
