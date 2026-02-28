"""Frontend UI: planner console with market-aware examples."""

from __future__ import annotations

import asyncio
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st
from matplotlib.patches import Rectangle

# Ensure imports work even when streamlit is launched outside repo root.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.planner_engine import plan_tasks
from agents.workflow_engine import run_unified_research
from tools.market_data import (
    fetch_market_data,
    get_cn_top100_watchlist,
    get_company_names_batch,
    get_market_top100_watchlist,
    normalize_market_symbol,
)


def _load_local_env() -> None:
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"").strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _mask_key(key: str) -> str:
    if len(key) < 8:
        return "***"
    return key[:4] + "***" + key[-4:]


def _env_quote(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _write_env_updates(env_path: Path, updates: dict[str, str]) -> list[str]:
    managed = {str(key).strip(): str(value) for key, value in updates.items() if str(key).strip()}
    if not managed:
        return []

    existing_lines: list[str] = []
    if env_path.exists():
        existing_lines = env_path.read_text(encoding="utf-8").splitlines()

    written_keys: set[str] = set()
    output_lines: list[str] = []
    for raw in existing_lines:
        line = raw.rstrip("\n")
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            output_lines.append(line)
            continue
        key, _ = line.split("=", 1)
        env_key = key.strip()
        if env_key in managed:
            output_lines.append(f"{env_key}={_env_quote(managed[env_key])}")
            written_keys.add(env_key)
        else:
            output_lines.append(line)

    for key in managed:
        if key not in written_keys:
            output_lines.append(f"{key}={_env_quote(managed[key])}")
            written_keys.add(key)

    payload = "\n".join(output_lines).rstrip() + "\n"
    env_path.write_text(payload, encoding="utf-8")
    return [key for key in managed if key in written_keys]


def _runtime_env_updates(
    *,
    model: str,
    api_base: str,
    api_key_override: str,
    temperature: float,
    fallback_enabled: bool,
) -> dict[str, str]:
    updates: dict[str, str] = {
        "TEMPERATURE": str(float(temperature)),
        "ENABLE_LOCAL_FALLBACK": "true" if fallback_enabled else "false",
    }
    if model.strip():
        updates["OPENAI_MODEL_NAME"] = model.strip()
    if api_base.strip():
        updates["OPENAI_API_BASE"] = api_base.strip().rstrip("/")
    if api_key_override.strip():
        updates["OPENAI_API_KEY"] = api_key_override.strip()
    return updates


def _run_planner(request: str) -> dict:
    result = asyncio.run(plan_tasks(request))
    return {
        "provider": result.provider,
        "data_source": result.data_source,
        "steps": result.steps,
        "reason": result.reason,
    }


def _run_unified_analysis(request: str, symbol: str, period: str) -> dict:
    return asyncio.run(
        run_unified_research(
            request=request,
            symbol=symbol.strip().upper(),
            period=period.strip(),
            interval="1d",
            max_retries=2,
            news_limit=8,
        )
    )


def _to_bilingual_step(step: str) -> str:
    mapping = {
        "Data Fetch": "Data Fetch / 数据获取",
        "Logic Calc": "Logic Calc / 逻辑计算",
        "Plotting": "Plotting / 绘图展示",
    }
    return mapping.get(step, step)


def _apply_runtime_config(
    *,
    model: str,
    api_base: str,
    api_key_override: str,
    temperature: float,
    fallback_enabled: bool,
) -> None:
    if model.strip():
        os.environ["OPENAI_MODEL_NAME"] = model.strip()
    if api_base.strip():
        os.environ["OPENAI_API_BASE"] = api_base.strip().rstrip("/")
    if api_key_override.strip():
        os.environ["OPENAI_API_KEY"] = api_key_override.strip()
    os.environ["TEMPERATURE"] = str(float(temperature))
    os.environ["ENABLE_LOCAL_FALLBACK"] = "true" if fallback_enabled else "false"


def _apply_and_maybe_persist_runtime_config(
    *,
    model: str,
    api_base: str,
    api_key_override: str,
    temperature: float,
    fallback_enabled: bool,
    persist_to_env: bool,
    env_path: Path | None = None,
) -> list[str]:
    _apply_runtime_config(
        model=model,
        api_base=api_base,
        api_key_override=api_key_override,
        temperature=temperature,
        fallback_enabled=fallback_enabled,
    )
    if not persist_to_env:
        return []

    updates = _runtime_env_updates(
        model=model,
        api_base=api_base,
        api_key_override=api_key_override,
        temperature=temperature,
        fallback_enabled=fallback_enabled,
    )
    target = env_path or (PROJECT_ROOT / ".env")
    return _write_env_updates(target, updates)


SEARCH_MARKET_OPTIONS: dict[str, str] = {
    "全市场 / All": "all",
    "A股 / CN": "cn",
    "港股 / HK": "hk",
    "美股 / US": "us",
}

MARKET_NAME_MAP: dict[str, str] = {
    "cn": "A股 / CN",
    "hk": "港股 / HK",
    "us": "美股 / US",
}

SYMBOL_NAME_ZH_HINTS: dict[str, str] = {
    "AAPL": "苹果",
    "MSFT": "微软",
    "NVDA": "英伟达",
    "AMZN": "亚马逊",
    "META": "Meta",
    "GOOGL": "谷歌",
    "TSLA": "特斯拉",
    "BABA": "阿里巴巴",
    "0700.HK": "腾讯控股",
    "9988.HK": "阿里巴巴",
    "3690.HK": "美团",
    "1810.HK": "小米集团",
    "600519.SS": "贵州茅台",
    "300750.SZ": "宁德时代",
    "601318.SS": "中国平安",
    "600036.SS": "招商银行",
    "000858.SZ": "五粮液",
    "002594.SZ": "比亚迪",
    "688981.SS": "中芯国际",
    "000333.SZ": "美的集团",
    "600276.SS": "恒瑞医药",
    "600900.SS": "长江电力",
    "601012.SS": "隆基绿能",
    "601899.SS": "紫金矿业",
    "9618.HK": "京东集团",
    "9888.HK": "百度集团",
    "9999.HK": "网易",
    "2015.HK": "理想汽车",
    "9868.HK": "小鹏汽车",
    "1211.HK": "比亚迪股份",
    "1299.HK": "友邦保险",
    "0941.HK": "中国移动",
    "0388.HK": "香港交易所",
    "0005.HK": "汇丰控股",
    "PDD": "拼多多",
    "JD": "京东",
    "BIDU": "百度",
    "NTES": "网易",
    "TCEHY": "腾讯ADR",
    "NIO": "蔚来",
    "LI": "理想汽车",
    "XPEV": "小鹏汽车",
    "JPM": "摩根大通",
    "BRK-B": "伯克希尔哈撒韦",
    "NFLX": "奈飞",
    "AMD": "超威半导体",
    "INTC": "英特尔",
}

SYMBOL_SEARCH_SEEDS: list[dict[str, str | list[str]]] = [
    {
        "symbol": "0700.HK",
        "market": "hk",
        "name_zh": "腾讯控股",
        "name_en": "Tencent Holdings",
        "aliases": ["腾讯", "腾讯控股", "tencent", "0700", "700"],
    },
    {
        "symbol": "9988.HK",
        "market": "hk",
        "name_zh": "阿里巴巴",
        "name_en": "Alibaba Group",
        "aliases": ["阿里", "阿里巴巴", "alibaba", "9988"],
    },
    {
        "symbol": "BABA",
        "market": "us",
        "name_zh": "阿里巴巴",
        "name_en": "Alibaba Group",
        "aliases": ["阿里", "阿里巴巴", "alibaba", "baba"],
    },
    {
        "symbol": "AAPL",
        "market": "us",
        "name_zh": "苹果",
        "name_en": "Apple",
        "aliases": ["苹果", "apple", "aapl"],
    },
    {
        "symbol": "MSFT",
        "market": "us",
        "name_zh": "微软",
        "name_en": "Microsoft",
        "aliases": ["微软", "microsoft", "msft"],
    },
    {
        "symbol": "NVDA",
        "market": "us",
        "name_zh": "英伟达",
        "name_en": "NVIDIA",
        "aliases": ["英伟达", "nvidia", "nvda"],
    },
    {
        "symbol": "600519.SS",
        "market": "cn",
        "name_zh": "贵州茅台",
        "name_en": "Kweichow Moutai",
        "aliases": ["茅台", "贵州茅台", "600519"],
    },
    {
        "symbol": "300750.SZ",
        "market": "cn",
        "name_zh": "宁德时代",
        "name_en": "CATL",
        "aliases": ["宁德", "宁王", "catl", "300750"],
    },
    {
        "symbol": "601318.SS",
        "market": "cn",
        "name_zh": "中国平安",
        "name_en": "Ping An",
        "aliases": ["平安", "pingan", "601318"],
    },
    {
        "symbol": "600036.SS",
        "market": "cn",
        "name_zh": "招商银行",
        "name_en": "China Merchants Bank",
        "aliases": ["招行", "cmb", "600036"],
    },
    {
        "symbol": "000858.SZ",
        "market": "cn",
        "name_zh": "五粮液",
        "name_en": "Wuliangye",
        "aliases": ["五粮液", "000858"],
    },
    {
        "symbol": "002594.SZ",
        "market": "cn",
        "name_zh": "比亚迪",
        "name_en": "BYD",
        "aliases": ["比亚迪", "002594", "byd"],
    },
    {
        "symbol": "688981.SS",
        "market": "cn",
        "name_zh": "中芯国际",
        "name_en": "SMIC",
        "aliases": ["中芯", "smic", "688981"],
    },
    {
        "symbol": "600276.SS",
        "market": "cn",
        "name_zh": "恒瑞医药",
        "name_en": "Jiangsu Hengrui",
        "aliases": ["恒瑞", "600276"],
    },
    {
        "symbol": "000333.SZ",
        "market": "cn",
        "name_zh": "美的集团",
        "name_en": "Midea",
        "aliases": ["美的", "midea", "000333"],
    },
    {
        "symbol": "600900.SS",
        "market": "cn",
        "name_zh": "长江电力",
        "name_en": "China Yangtze Power",
        "aliases": ["长江电力", "600900"],
    },
    {
        "symbol": "601012.SS",
        "market": "cn",
        "name_zh": "隆基绿能",
        "name_en": "LONGi",
        "aliases": ["隆基", "隆基绿能", "longi", "601012"],
    },
    {
        "symbol": "601899.SS",
        "market": "cn",
        "name_zh": "紫金矿业",
        "name_en": "Zijin Mining",
        "aliases": ["紫金", "601899"],
    },
    {
        "symbol": "9618.HK",
        "market": "hk",
        "name_zh": "京东集团",
        "name_en": "JD.com",
        "aliases": ["京东", "jd", "9618"],
    },
    {
        "symbol": "9888.HK",
        "market": "hk",
        "name_zh": "百度集团",
        "name_en": "Baidu",
        "aliases": ["百度", "baidu", "9888"],
    },
    {
        "symbol": "9999.HK",
        "market": "hk",
        "name_zh": "网易",
        "name_en": "NetEase",
        "aliases": ["网易", "netease", "9999"],
    },
    {
        "symbol": "3690.HK",
        "market": "hk",
        "name_zh": "美团",
        "name_en": "Meituan",
        "aliases": ["美团", "美团-w", "3690"],
    },
    {
        "symbol": "1810.HK",
        "market": "hk",
        "name_zh": "小米集团",
        "name_en": "Xiaomi",
        "aliases": ["小米", "小米集团", "1810", "xiaomi"],
    },
    {
        "symbol": "2015.HK",
        "market": "hk",
        "name_zh": "理想汽车",
        "name_en": "Li Auto",
        "aliases": ["理想", "理想汽车", "2015"],
    },
    {
        "symbol": "9868.HK",
        "market": "hk",
        "name_zh": "小鹏汽车",
        "name_en": "XPeng",
        "aliases": ["小鹏", "小鹏汽车", "9868", "xpeng"],
    },
    {
        "symbol": "1211.HK",
        "market": "hk",
        "name_zh": "比亚迪股份",
        "name_en": "BYD Co.",
        "aliases": ["比亚迪", "1211", "byd"],
    },
    {
        "symbol": "1299.HK",
        "market": "hk",
        "name_zh": "友邦保险",
        "name_en": "AIA",
        "aliases": ["友邦", "aia", "1299"],
    },
    {
        "symbol": "0941.HK",
        "market": "hk",
        "name_zh": "中国移动",
        "name_en": "China Mobile",
        "aliases": ["中移动", "中国移动", "0941", "941"],
    },
    {
        "symbol": "0388.HK",
        "market": "hk",
        "name_zh": "香港交易所",
        "name_en": "HKEX",
        "aliases": ["港交所", "hkex", "0388", "388"],
    },
    {
        "symbol": "0005.HK",
        "market": "hk",
        "name_zh": "汇丰控股",
        "name_en": "HSBC",
        "aliases": ["汇丰", "hsbc", "0005", "5"],
    },
    {
        "symbol": "PDD",
        "market": "us",
        "name_zh": "拼多多",
        "name_en": "PDD Holdings",
        "aliases": ["拼多多", "pdd"],
    },
    {
        "symbol": "JD",
        "market": "us",
        "name_zh": "京东",
        "name_en": "JD.com",
        "aliases": ["京东", "jd"],
    },
    {
        "symbol": "BIDU",
        "market": "us",
        "name_zh": "百度",
        "name_en": "Baidu",
        "aliases": ["百度", "baidu", "bidu"],
    },
    {
        "symbol": "NTES",
        "market": "us",
        "name_zh": "网易",
        "name_en": "NetEase",
        "aliases": ["网易", "netease", "ntes"],
    },
    {
        "symbol": "TCEHY",
        "market": "us",
        "name_zh": "腾讯ADR",
        "name_en": "Tencent ADR",
        "aliases": ["腾讯adr", "tencent", "tcehy"],
    },
    {
        "symbol": "NIO",
        "market": "us",
        "name_zh": "蔚来",
        "name_en": "NIO",
        "aliases": ["蔚来", "nio"],
    },
    {
        "symbol": "LI",
        "market": "us",
        "name_zh": "理想汽车",
        "name_en": "Li Auto",
        "aliases": ["理想", "理想汽车", "li auto", "li"],
    },
    {
        "symbol": "XPEV",
        "market": "us",
        "name_zh": "小鹏汽车",
        "name_en": "XPeng",
        "aliases": ["小鹏", "小鹏汽车", "xpeng", "xpev"],
    },
    {
        "symbol": "JPM",
        "market": "us",
        "name_zh": "摩根大通",
        "name_en": "JPMorgan Chase",
        "aliases": ["摩根大通", "jpmorgan", "jpm"],
    },
    {
        "symbol": "BRK-B",
        "market": "us",
        "name_zh": "伯克希尔哈撒韦",
        "name_en": "Berkshire Hathaway",
        "aliases": ["伯克希尔", "巴菲特", "brkb", "brk-b"],
    },
    {
        "symbol": "NFLX",
        "market": "us",
        "name_zh": "奈飞",
        "name_en": "Netflix",
        "aliases": ["奈飞", "netflix", "nflx"],
    },
    {
        "symbol": "AMD",
        "market": "us",
        "name_zh": "超威半导体",
        "name_en": "AMD",
        "aliases": ["amd", "超威", "苏妈", "advanced micro devices"],
    },
    {
        "symbol": "INTC",
        "market": "us",
        "name_zh": "英特尔",
        "name_en": "Intel",
        "aliases": ["英特尔", "intel", "intc"],
    },
]


def _contains_cjk(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", text))


def _normalize_search_text(text: str) -> str:
    return re.sub(r"\s+", "", text.strip().lower())


def _display_company_name(symbol: str, name: str) -> str:
    raw = str(name or "").strip()
    zh_hint = SYMBOL_NAME_ZH_HINTS.get(symbol.strip().upper(), "")
    if zh_hint:
        if not raw:
            return zh_hint
        if _contains_cjk(raw):
            return raw
        if raw.lower() == zh_hint.lower():
            return raw
        return f"{zh_hint} ({raw})"
    return raw or symbol


def _build_symbol_search_rows(market: str) -> list[dict[str, str | list[str]]]:
    market_lower = market.strip().lower()
    target_markets = ["cn", "hk", "us"] if market_lower == "all" else [market_lower]
    dynamic_markets = ["cn"] if market_lower == "all" else target_markets
    rows_by_symbol: dict[str, dict[str, str | list[str]]] = {}

    def upsert_row(
        *,
        raw_symbol: str,
        market_code: str,
        name: str,
        aliases: list[str] | None = None,
    ) -> None:
        symbol = normalize_market_symbol(raw_symbol, market=market_code).strip().upper()
        if not symbol:
            return
        company_name = _display_company_name(symbol, name)
        alias_pool = [symbol, company_name, name]
        if symbol.endswith(".HK"):
            code = symbol.split(".", 1)[0]
            alias_pool.extend([code, code.lstrip("0") or code])
        if symbol.endswith((".SS", ".SZ")):
            alias_pool.append(symbol.split(".", 1)[0])
        if aliases:
            alias_pool.extend(aliases)
        normalized_aliases = [str(item).strip() for item in alias_pool if str(item).strip()]

        if symbol not in rows_by_symbol:
            rows_by_symbol[symbol] = {
                "symbol": symbol,
                "name": company_name,
                "market": market_code,
                "aliases": normalized_aliases,
            }
            return

        existing = rows_by_symbol[symbol]
        existing_name = str(existing.get("name", "")).strip()
        if company_name and (not existing_name or (_contains_cjk(company_name) and not _contains_cjk(existing_name))):
            existing["name"] = company_name
        merged_aliases = [str(item) for item in existing.get("aliases", []) if str(item).strip()]
        merged_aliases.extend(normalized_aliases)
        deduped: list[str] = []
        seen_aliases: set[str] = set()
        for item in merged_aliases:
            key = item.strip().lower()
            if not key or key in seen_aliases:
                continue
            seen_aliases.add(key)
            deduped.append(item.strip())
        existing["aliases"] = deduped

    for seed in SYMBOL_SEARCH_SEEDS:
        market_code = str(seed.get("market", "")).strip().lower()
        if market_code not in target_markets:
            continue
        name_zh = str(seed.get("name_zh", "")).strip()
        name_en = str(seed.get("name_en", "")).strip()
        display_name = f"{name_zh} ({name_en})" if name_zh and name_en else (name_zh or name_en)
        seed_aliases = [str(item) for item in seed.get("aliases", []) if str(item).strip()]
        upsert_row(
            raw_symbol=str(seed.get("symbol", "")),
            market_code=market_code,
            name=display_name,
            aliases=seed_aliases,
        )

    for market_code in dynamic_markets:
        for row in _cached_constituents(market_code):
            upsert_row(
                raw_symbol=str(row.get("symbol", "")),
                market_code=market_code,
                name=str(row.get("name", "")),
            )

    return list(rows_by_symbol.values())


@st.cache_data(ttl=1800, show_spinner=False)
def _cached_constituents(market: str) -> list[dict[str, str]]:
    market_lower = market.lower()
    try:
        if market_lower == "cn":
            symbols = get_cn_top100_watchlist(use_live_market_cap=False)
        else:
            symbols = get_market_top100_watchlist(market_lower)
    except Exception:
        symbols = []

    symbols = [str(symbol).upper().strip() for symbol in symbols if str(symbol).strip()]
    if not symbols:
        return []

    name_map = get_company_names_batch(symbols, market=market_lower, resolve_remote=False)
    return [{"symbol": symbol, "name": str(name_map.get(symbol, symbol)).strip()} for symbol in symbols]


def _search_symbol_candidates(query: str, market: str, limit: int = 20) -> list[dict[str, str | int | list[str]]]:
    raw_q = query.strip()
    if not raw_q:
        return []
    q = _normalize_search_text(raw_q)
    if not q:
        return []

    rows = _build_symbol_search_rows(market)
    selected_market = market.strip().lower()

    scored: list[tuple[int, str, str, str, list[str]]] = []
    seen: set[str] = set()
    for row in rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        name = str(row.get("name", "")).strip()
        market_code = str(row.get("market", "")).strip().lower()
        aliases = [str(item).strip() for item in row.get("aliases", []) if str(item).strip()]
        if not symbol or symbol in seen:
            continue

        symbol_norm = _normalize_search_text(symbol)
        name_norm = _normalize_search_text(name)
        alias_norm = [_normalize_search_text(item) for item in aliases]

        best_score = 0
        reasons: list[str] = []

        def apply(points: int, reason: str) -> None:
            nonlocal best_score
            best_score += points
            reasons.append(f"{reason} (+{points})")

        if symbol_norm == q:
            apply(100, "代码完全匹配 / Exact symbol match")
        elif symbol_norm.startswith(q):
            apply(80, "代码前缀匹配 / Symbol prefix match")
        elif q in symbol_norm:
            apply(50, "代码包含关键词 / Symbol contains query")

        if name_norm == q:
            apply(70, "公司名完全匹配 / Exact name match")
        elif name_norm.startswith(q):
            apply(55, "公司名前缀匹配 / Name prefix match")
        elif q in name_norm:
            apply(35, "公司名包含关键词 / Name contains query")

        alias_best_points = 0
        alias_best: str | None = None
        for alias_raw, alias in zip(aliases, alias_norm):
            if not alias:
                continue
            if alias in {symbol_norm, name_norm}:
                continue
            if alias == q:
                if 75 > alias_best_points:
                    alias_best_points = 75
                    alias_best = alias_raw
            elif alias.startswith(q):
                if 52 > alias_best_points:
                    alias_best_points = 52
                    alias_best = alias_raw
            elif q in alias:
                if 30 > alias_best_points:
                    alias_best_points = 30
                    alias_best = alias_raw

        if alias_best_points > 0:
            apply(alias_best_points, f"别名匹配 / Alias match: {alias_best}")

        if best_score <= 0:
            continue

        if selected_market != "all" and market_code == selected_market:
            apply(8, "命中所选市场 / Matches selected market")
        if _contains_cjk(raw_q) and _contains_cjk(name):
            apply(5, "中文关键词命中 / Chinese-name hit")

        scored.append((best_score, symbol, name or symbol, market_code, reasons))
        seen.add(symbol)

    scored.sort(key=lambda x: (-x[0], x[3], x[1]))
    return [
        {
            "symbol": symbol,
            "name": name,
            "market": market_code.upper(),
            "ranking_score": score,
            "ranking_reasons": reasons,
        }
        for score, symbol, name, market_code, reasons in scored[:limit]
    ]


def _normalize_symbol_for_run(raw_symbol: str, market_hint: str) -> str:
    text = raw_symbol.strip()
    if not text:
        return ""
    market_lower = market_hint.lower()
    if market_lower not in {"cn", "hk", "us", "auto"}:
        market_lower = "auto"
    normalized = normalize_market_symbol(text, market=market_lower)
    return normalized.strip().upper()


REQUEST_SYMBOL_PATTERN = re.compile(
    r"\b\d{6}\.(?:SS|SZ)\b|\b\d{4,5}\.HK\b|\b\d{6}\b|\b[A-Z]{1,6}(?:-[A-Z])?\b",
    re.IGNORECASE,
)

NON_SYMBOL_TOKENS = {
    "API",
    "OHLCV",
    "RSI",
    "MA",
    "MACD",
    "KDJ",
    "USD",
    "CNY",
    "HKD",
    "CN",
    "US",
    "HK",
}


def _extract_request_symbols(request_text: str) -> list[str]:
    text = str(request_text or "")
    out: list[str] = []
    seen: set[str] = set()

    for match in REQUEST_SYMBOL_PATTERN.finditer(text):
        token = match.group(0).strip()
        normalized = normalize_market_symbol(token, market="auto").strip().upper()
        if normalized in NON_SYMBOL_TOKENS:
            continue
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)

    for symbol, zh_name in SYMBOL_NAME_ZH_HINTS.items():
        if not zh_name:
            continue
        if zh_name in text and symbol not in seen:
            seen.add(symbol)
            out.append(symbol)
    return out


def _symbol_forms(symbol: str) -> set[str]:
    normalized = normalize_market_symbol(symbol, market="auto").strip().upper()
    if not normalized:
        return set()
    forms = {normalized}
    if "." in normalized:
        code = normalized.split(".", 1)[0]
        forms.add(code)
        forms.add(code.lstrip("0") or "0")
    if "-" in normalized:
        forms.add(normalized.replace("-", ""))
    return forms


def _symbols_equivalent(left: str, right: str) -> bool:
    left_forms = _symbol_forms(left)
    right_forms = _symbol_forms(right)
    return bool(left_forms and right_forms and left_forms.intersection(right_forms))


def _validate_request_symbol_consistency(request_text: str, active_symbol: str) -> list[str]:
    normalized_active_symbol = normalize_market_symbol(active_symbol, market="auto").strip().upper()
    request_symbols = _extract_request_symbols(request_text)
    if not normalized_active_symbol or not request_symbols:
        return request_symbols

    conflicts = [item for item in request_symbols if not _symbols_equivalent(item, normalized_active_symbol)]
    if conflicts:
        conflict_text = ", ".join(conflicts)
        raise ValueError(
            "请求标的与当前标的冲突："
            f"active={normalized_active_symbol}, request={conflict_text}. "
            "请先对齐后再执行。 / Request symbol conflicts with active symbol; align before running."
        )
    return request_symbols


def _build_synced_request_text(current_request: str, new_symbol: str) -> str:
    normalized_symbol = normalize_market_symbol(new_symbol, market="auto").strip().upper()
    text = str(current_request or "").strip()
    if not normalized_symbol:
        return text
    if not text:
        return f"分析 {normalized_symbol} 最近一个月走势，给出规划步骤"

    raw_matches = [m.group(0) for m in REQUEST_SYMBOL_PATTERN.finditer(text)]
    if raw_matches:
        old = raw_matches[0]
        lower_text = text.lower()
        lower_old = old.lower()
        idx = lower_text.find(lower_old)
        if idx >= 0:
            return f"{text[:idx]}{normalized_symbol}{text[idx + len(old):]}"

    zh_names = [name for name in SYMBOL_NAME_ZH_HINTS.values() if name and name in text]
    if zh_names:
        new_zh = SYMBOL_NAME_ZH_HINTS.get(normalized_symbol, "")
        if new_zh:
            return text.replace(zh_names[0], new_zh, 1)

    return f"分析 {normalized_symbol}：{text}"


@st.cache_data(ttl=180, show_spinner=False)
def _cached_ohlcv_for_chart(symbol: str, period: str) -> pd.DataFrame:
    normalized_symbol = normalize_market_symbol(symbol, market="auto").strip().upper()
    if not normalized_symbol:
        return pd.DataFrame()
    try:
        result = asyncio.run(fetch_market_data(normalized_symbol, period=period, interval="1d"))
    except Exception:
        return pd.DataFrame()
    if not result.ok or not result.records:
        return pd.DataFrame()
    df = pd.DataFrame(result.records).copy()
    required = {"Date", "Open", "High", "Low", "Close"}
    if not required.issubset(df.columns):
        return pd.DataFrame()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["Date", "Open", "High", "Low", "Close"]).sort_values("Date")
    return df.tail(240).reset_index(drop=True)


def _render_candlestick(df: pd.DataFrame, *, title: str) -> None:
    if df.empty:
        st.info("暂无可绘制K线数据。 / No OHLC data available for candlestick chart.")
        return
    plot_df = df.copy()
    plot_df["MA5"] = plot_df["Close"].rolling(5).mean()
    plot_df["MA10"] = plot_df["Close"].rolling(10).mean()
    plot_df["MA20"] = plot_df["Close"].rolling(20).mean()

    fig, (ax_price, ax_volume) = plt.subplots(
        2,
        1,
        figsize=(10, 6.2),
        dpi=120,
        sharex=True,
        gridspec_kw={"height_ratios": [3.4, 1], "hspace": 0.06},
    )
    x_values = mdates.date2num(plot_df["Date"].dt.tz_convert(None))
    body_width = 0.62
    up_color = "#e53935"
    down_color = "#00a67e"

    for x, o, h, l, c, v in zip(
        x_values,
        plot_df["Open"],
        plot_df["High"],
        plot_df["Low"],
        plot_df["Close"],
        plot_df.get("Volume", pd.Series([0.0] * len(plot_df))),
    ):
        color = up_color if c >= o else down_color
        ax_price.vlines(x, l, h, color=color, linewidth=1.0, alpha=0.95)
        lower = min(o, c)
        height = max(abs(c - o), 0.02)
        rect = Rectangle((x - body_width / 2, lower), body_width, height, facecolor=color, edgecolor=color, linewidth=0.8, alpha=0.92)
        ax_price.add_patch(rect)
        ax_volume.bar(x, float(v), width=body_width, color=color, alpha=0.65)

    ax_price.plot(x_values, plot_df["MA5"], color="#f6c344", linewidth=1.1, label="MA5")
    ax_price.plot(x_values, plot_df["MA10"], color="#5e9bff", linewidth=1.1, label="MA10")
    ax_price.plot(x_values, plot_df["MA20"], color="#ab47bc", linewidth=1.1, label="MA20")

    latest_close = float(plot_df["Close"].iloc[-1])
    latest_open = float(plot_df["Open"].iloc[-1])
    latest_high = float(plot_df["High"].iloc[-1])
    latest_low = float(plot_df["Low"].iloc[-1])
    latest_color = up_color if latest_close >= latest_open else down_color
    ax_price.axhline(latest_close, color=latest_color, linewidth=0.9, linestyle="--", alpha=0.7)
    ax_price.text(
        x_values[-1] + 0.6,
        latest_close,
        f"{latest_close:.2f}",
        color=latest_color,
        va="center",
        fontsize=9,
    )

    ax_price.set_title(title, fontsize=11)
    ax_price.grid(alpha=0.18, linestyle="--", linewidth=0.7)
    ax_price.set_ylabel("Price")
    ax_price.legend(loc="upper left", ncol=3, frameon=False, fontsize=8)

    ax_volume.set_ylabel("Vol")
    ax_volume.grid(alpha=0.12, linestyle="--", linewidth=0.7)
    ax_volume.set_xlabel("Date")
    ax_volume.xaxis_date()
    ax_volume.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    fig.autofmt_xdate(rotation=0)

    st.pyplot(fig, clear_figure=True, use_container_width=True)
    st.caption(
        "当日OHLC / Latest OHLC: "
        f"O {latest_open:.2f}  H {latest_high:.2f}  L {latest_low:.2f}  C {latest_close:.2f}"
    )


def main() -> None:
    _load_local_env()
    st.set_page_config(page_title="Alpha-Insight Planner Console / 规划控制台", layout="wide")
    st.title("Alpha-Insight Planner Console / 规划控制台")
    st.caption("Use this page to inspect planner decomposition and data-source routing / 查看任务拆解和数据源路由。")

    with st.sidebar:
        st.header("Runtime Config / 运行配置")
        current_api_base = os.getenv("OPENAI_API_BASE", "")
        current_model = os.getenv("OPENAI_MODEL_NAME", "")
        current_key = os.getenv("OPENAI_API_KEY", "")
        current_fallback = os.getenv("ENABLE_LOCAL_FALLBACK", "true").strip().lower() in {"1", "true", "yes", "on"}
        current_temp = os.getenv("TEMPERATURE", "0.0")
        try:
            current_temp_value = float(current_temp)
        except Exception:
            current_temp_value = 0.0

        runtime_api_base = st.text_input("API Base / 接口地址", value=current_api_base)
        runtime_model = st.text_input("Model / 模型", value=current_model)
        runtime_temp = st.number_input(
            "Temperature / 温度",
            min_value=0.0,
            max_value=2.0,
            value=float(current_temp_value),
            step=0.1,
            format="%.2f",
        )
        runtime_fallback = st.checkbox("Fallback / 本地回退", value=current_fallback)
        runtime_key_override = st.text_input(
            "API Key Override / 覆盖密钥(可选)",
            value="",
            type="password",
            placeholder="留空则继续使用当前 OPENAI_API_KEY",
        )

        persist_runtime_env = st.checkbox("Persist to .env / 写入 .env", value=False)
        if st.button("Apply Runtime Config / 应用运行配置"):
            try:
                saved_keys = _apply_and_maybe_persist_runtime_config(
                    model=runtime_model,
                    api_base=runtime_api_base,
                    api_key_override=runtime_key_override,
                    temperature=float(runtime_temp),
                    fallback_enabled=bool(runtime_fallback),
                    persist_to_env=bool(persist_runtime_env),
                )
            except Exception as exc:
                st.error(f"Runtime config apply failed / 配置应用失败: {exc}")
            else:
                if persist_runtime_env:
                    if saved_keys:
                        st.success(
                            "Runtime config applied and saved to .env / 配置已应用并写入 .env: "
                            + ", ".join(saved_keys)
                        )
                    else:
                        st.success("Runtime config applied / 已应用运行配置")
                else:
                    st.success("Runtime config applied / 已应用运行配置")
        st.text(f"Active Key / 当前密钥: {_mask_key(os.getenv('OPENAI_API_KEY', '')) if os.getenv('OPENAI_API_KEY') else '(missing/缺失)'}")
        st.markdown("---")
        st.markdown("**快速开始 / Quick Start**")
        st.markdown("1. 选模板或搜索标的 / Select template or search symbol")
        st.markdown("2. 点击 `运行规划 / Run Planner`")
        st.markdown("3. 查看数据源与步骤 / Review data source + steps")

    st.subheader("规划输入 / Planner Input")
    templates = {
        "US Tech (AAPL)": "分析 AAPL 最近一个月走势，给出规划步骤",
        "CN A-Share (贵州茅台)": "分析 贵州茅台 最近三个月走势并给出执行计划",
        "CN Numeric (600519)": "分析 600519 最近三个月走势并说明应使用 API 还是网页抓取",
    }
    template_symbol = {
        "US Tech (AAPL)": "AAPL",
        "CN A-Share (贵州茅台)": "600519.SS",
        "CN Numeric (600519)": "600519.SS",
    }
    default_template = "US Tech (AAPL)"
    if "request_input" not in st.session_state:
        st.session_state.request_input = templates[default_template]
    if "symbol_input" not in st.session_state:
        st.session_state.symbol_input = template_symbol[default_template]
    if "selected_symbol" not in st.session_state:
        st.session_state.selected_symbol = template_symbol[default_template]
    if "pending_symbol_input" not in st.session_state:
        st.session_state.pending_symbol_input = ""
    if "pending_request_input" not in st.session_state:
        st.session_state.pending_request_input = ""
    if "period_input" not in st.session_state:
        st.session_state.period_input = "1mo"
    if "auto_sync_request" not in st.session_state:
        st.session_state.auto_sync_request = True
    if "search_requested" not in st.session_state:
        st.session_state.search_requested = False

    selected_template = st.selectbox("请求模板 / Request Template", list(templates.keys()), index=0)
    if st.button("应用模板 / Apply Template"):
        st.session_state.request_input = templates[selected_template]
        st.session_state.pending_symbol_input = template_symbol[selected_template]
        st.session_state.selected_symbol = template_symbol[selected_template]
        st.rerun()

    # Apply deferred symbol updates before the widget is instantiated.
    pending_symbol = str(st.session_state.get("pending_symbol_input", "")).strip()
    if pending_symbol:
        st.session_state.symbol_input = pending_symbol
        st.session_state.selected_symbol = pending_symbol
        st.session_state.pending_symbol_input = ""

    pending_request = str(st.session_state.get("pending_request_input", "")).strip()
    if pending_request:
        st.session_state.request_input = pending_request
        st.session_state.pending_request_input = ""

    request = st.text_area("请求 / Request", key="request_input", height=120)

    st.markdown("**标的搜索 / Symbol Search**")
    st.checkbox(
        "选中标的后自动同步请求 / Auto-sync request after selecting symbol",
        key="auto_sync_request",
    )
    if "search_market_label" not in st.session_state:
        st.session_state.search_market_label = list(SEARCH_MARKET_OPTIONS.keys())[0]
    if "search_query_input" not in st.session_state:
        st.session_state.search_query_input = ""
    if "symbol_search_last_query" not in st.session_state:
        st.session_state.symbol_search_last_query = ""

    search_market_label = st.selectbox(
        "搜索市场 / Search Market",
        list(SEARCH_MARKET_OPTIONS.keys()),
        key="search_market_label",
    )
    search_market = SEARCH_MARKET_OPTIONS[search_market_label]
    def _queue_symbol_search() -> None:
        st.session_state.search_requested = True

    search_query = st.text_input(
        "按公司名或代码搜索 / Search by Company or Symbol",
        key="search_query_input",
        on_change=_queue_symbol_search,
    )
    st.caption("提示：搜索用于挑选候选；点击“使用所选标的”后才会写入当前分析标的。")
    if "symbol_search_results" not in st.session_state:
        st.session_state.symbol_search_results = []
    if st.button("搜索 / Search"):
        st.session_state.search_requested = True

    if st.session_state.search_requested:
        st.session_state.search_requested = False
        if not search_query.strip():
            st.session_state.symbol_search_results = []
            st.session_state.symbol_search_last_query = ""
        else:
            with st.spinner("正在搜索标的... / Searching symbols..."):
                st.session_state.symbol_search_results = _search_symbol_candidates(search_query, search_market)
                st.session_state.symbol_search_last_query = search_query.strip()

    active_query = str(st.session_state.get("search_query_input", "")).strip()
    last_query = str(st.session_state.get("symbol_search_last_query", "")).strip()
    candidates = st.session_state.symbol_search_results if active_query and active_query == last_query else []
    if active_query:
        if active_query != last_query:
            st.caption("输入已变更，请点击“搜索 / Search”更新候选。 / Query changed, click Search to refresh candidates.")
        elif candidates:
            label_to_candidate: dict[str, dict[str, str | int | list[str]]] = {}
            candidate_labels = [
                (
                    f"{item['symbol']} | {item['name']} | "
                    f"{MARKET_NAME_MAP.get(str(item.get('market', '')).strip().lower(), str(item.get('market', '')).strip().upper())}"
                )
                for item in candidates
            ]
            for label, item in zip(candidate_labels, candidates):
                label_to_candidate[label] = item
            chosen_label = st.selectbox("候选列表 / Candidates", candidate_labels)
            chosen_candidate = label_to_candidate.get(chosen_label)
            if chosen_candidate:
                score = int(chosen_candidate.get("ranking_score", 0))
                reasons = [str(item) for item in chosen_candidate.get("ranking_reasons", []) if str(item).strip()]
                if reasons:
                    st.caption(f"排序分 / Score: {score}；排序依据 / Why ranked: {'；'.join(reasons)}")
            if st.button("使用所选标的 / Use Selected Symbol"):
                chosen_symbol = (
                    str(chosen_candidate.get("symbol", "")).strip().upper()
                    if chosen_candidate
                    else chosen_label.split(" | ", 1)[0].strip().upper()
                )
                st.session_state.pending_symbol_input = chosen_symbol
                st.session_state.selected_symbol = chosen_symbol
                if st.session_state.get("auto_sync_request", True):
                    st.session_state.pending_request_input = _build_synced_request_text(
                        str(st.session_state.get("request_input", "")),
                        chosen_symbol,
                    )
                st.rerun()

            with st.expander("查看候选排序解释 / View Ranking Explanation", expanded=False):
                for idx, item in enumerate(candidates, start=1):
                    market_label = MARKET_NAME_MAP.get(
                        str(item.get("market", "")).strip().lower(),
                        str(item.get("market", "")).strip().upper(),
                    )
                    score = int(item.get("ranking_score", 0))
                    reasons = [str(text) for text in item.get("ranking_reasons", []) if str(text).strip()]
                    reason_text = "；".join(reasons) if reasons else "规则命中 / Rule hit"
                    st.write(f"{idx}. {item['symbol']} | {item['name']} | {market_label} | score={score}")
                    st.caption(reason_text)
        else:
            st.info("当前条件未命中候选，请尝试全市场或更换关键词。 / No candidate found; try All market or another keyword.")

    symbol = st.text_input("标的代码 / Symbol", key="symbol_input")
    if symbol.strip():
        st.session_state.selected_symbol = symbol.strip().upper()
    period = st.selectbox("时间区间 / Period", ["1mo", "3mo", "6mo"], key="period_input")

    normalized_symbol_preview = _normalize_symbol_for_run(symbol, search_market)
    request_symbols = _extract_request_symbols(request)
    request_symbol_conflicts = [
        item
        for item in request_symbols
        if normalized_symbol_preview and not _symbols_equivalent(item, normalized_symbol_preview)
    ]
    if normalized_symbol_preview:
        st.caption(f"当前分析标的 / Active Symbol: `{normalized_symbol_preview}`")
    if request_symbols:
        st.caption(f"请求中识别到的标的 / Symbols found in request: `{', '.join(request_symbols)}`")
    request_symbol_mismatch = bool(normalized_symbol_preview and request_symbol_conflicts)
    if request_symbol_mismatch:
        st.warning(
            "检测到请求标的冲突，执行会被阻断（fail-fast）。"
            f" 冲突标的: {', '.join(request_symbol_conflicts)}"
            " / Request symbol conflicts detected; execution will fail-fast."
        )
        if st.button("一键对齐请求到当前标的 / Align Request to Active Symbol"):
            st.session_state.pending_request_input = _build_synced_request_text(request, normalized_symbol_preview)
            st.rerun()

    st.caption(
        "按钮说明："
        "运行规划=仅任务拆解；运行完整分析/运行行情+新闻融合分析=统一产出同一 run_id 报告对象。"
    )
    run_btn = st.button("运行规划(仅拆解) / Run Planner (Plan Only)")
    run_full_btn = st.button("运行完整分析(含执行) / Run Full Analysis (Plan+Execute)")
    run_fused_btn = st.button("运行行情+新闻融合分析(含图表) / Run Market+News Analysis (Charts)")

    if "history" not in st.session_state:
        st.session_state.history = []

    if run_btn:
        _apply_runtime_config(
            model=runtime_model,
            api_base=runtime_api_base,
            api_key_override=runtime_key_override,
            temperature=float(runtime_temp),
            fallback_enabled=bool(runtime_fallback),
        )
        if not request.strip():
            st.error("请求不能为空。 / Request is empty.")
        else:
            with st.spinner("正在调用远程模型... / Calling remote LLM..."):
                try:
                    output = _run_planner(request)
                    record = {
                        "time": datetime.now(timezone.utc).isoformat(),
                        "request": request,
                        "run_mode": "planner_only",
                        **output,
                    }
                    st.session_state.history.insert(0, record)
                    st.success("规划完成。 / Planner completed.")
                except Exception as exc:
                    st.error(f"规划失败 / Planner failed: {exc}")

    if run_full_btn:
        _apply_runtime_config(
            model=runtime_model,
            api_base=runtime_api_base,
            api_key_override=runtime_key_override,
            temperature=float(runtime_temp),
            fallback_enabled=bool(runtime_fallback),
        )
        normalized_symbol = _normalize_symbol_for_run(symbol, search_market)
        if not request.strip() or not normalized_symbol:
            st.error("请求或代码不能为空。 / Request/Symbol is empty.")
        else:
            with st.spinner("正在执行统一研究工作流... / Running unified full+fused research..."):
                try:
                    _validate_request_symbol_consistency(request, normalized_symbol)
                    result = _run_unified_analysis(request, normalized_symbol, period)
                    plan = result.get("plan", {}) if isinstance(result, dict) else {}
                    record = {
                        "time": datetime.now(timezone.utc).isoformat(),
                        "request": request,
                        "run_mode": "unified_research",
                        "run_id": str(result.get("run_id", "")),
                        "provider": str(plan.get("provider", "unknown")),
                        "data_source": str(plan.get("data_source", "unknown")),
                        "steps": [str(step) for step in plan.get("steps", [])],
                        "reason": str(plan.get("reason", "")),
                        "research_result": result,
                    }
                    st.session_state.history.insert(0, record)
                    st.success(f"统一研究完成。 / Unified research completed. run_id={record['run_id']}")
                except Exception as exc:
                    st.error(f"统一研究失败 / Unified research failed: {exc}")

    if run_fused_btn:
        _apply_runtime_config(
            model=runtime_model,
            api_base=runtime_api_base,
            api_key_override=runtime_key_override,
            temperature=float(runtime_temp),
            fallback_enabled=bool(runtime_fallback),
        )
        normalized_symbol = _normalize_symbol_for_run(symbol, search_market)
        if not request.strip() or not normalized_symbol:
            st.error("请求或代码不能为空。 / Request/Symbol is empty.")
        else:
            with st.spinner("正在执行统一研究工作流... / Running unified full+fused research..."):
                try:
                    _validate_request_symbol_consistency(request, normalized_symbol)
                    result = _run_unified_analysis(request, normalized_symbol, period)
                    plan = result.get("plan", {}) if isinstance(result, dict) else {}
                    record = {
                        "time": datetime.now(timezone.utc).isoformat(),
                        "request": request,
                        "run_mode": "unified_research",
                        "run_id": str(result.get("run_id", "")),
                        "provider": str(plan.get("provider", "unknown")),
                        "data_source": str(plan.get("data_source", "unknown")),
                        "steps": [str(step) for step in plan.get("steps", [])],
                        "reason": str(plan.get("reason", "")),
                        "research_result": result,
                    }
                    st.session_state.history.insert(0, record)
                    st.success(f"统一研究完成。 / Unified research completed. run_id={record['run_id']}")
                except Exception as exc:
                    st.error(f"统一研究失败 / Unified research failed: {exc}")

    if st.session_state.history:
        st.subheader("最新结果 / Latest Result")
        latest = st.session_state.history[0]
        research_result = latest.get("research_result")
        if latest.get("provider") == "fallback":
            st.warning(
                "Planner is using local fallback / 当前为本地回退规划。"
                " Please check OPENAI_API_KEY, OPENAI_API_BASE, OPENAI_MODEL_NAME / "
                "请检查以上环境变量是否在当前进程生效。"
            )
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("模型 / Provider", latest["provider"])
        with c2:
            st.metric("数据源 / Data Source", latest["data_source"])
        with c3:
            st.metric("步骤数 / Steps Count", len(latest["steps"]))
        run_mode_label = {
            "planner_only": "仅规划 / Plan Only",
            "unified_research": "统一研究 / Unified Full+Fused",
        }.get(str(latest.get("run_mode", "")).strip(), "未知 / Unknown")
        run_id_text = str(latest.get("run_id", "")).strip()
        if not run_id_text and isinstance(research_result, dict):
            run_id_text = str(research_result.get("run_id", "")).strip()
        if run_id_text:
            st.caption(f"当前运行模式 / Current Run Mode: {run_mode_label} | run_id=`{run_id_text}`")
        else:
            st.caption(f"当前运行模式 / Current Run Mode: {run_mode_label}")
        plan_tab, full_tab, fused_tab = st.tabs(
            [
                "规划 / Planning",
                "完整分析 / Full Analysis",
                "融合分析 / Fused Analysis",
            ]
        )

        with plan_tab:
            st.write("执行步骤 / Steps")
            plan_payload = (research_result.get("plan", {}) if isinstance(research_result, dict) else {})
            plan_steps = [str(step) for step in plan_payload.get("steps", latest.get("steps", []))]
            plan_text = "\n".join(_to_bilingual_step(step) for step in plan_steps)
            st.code(plan_text or "(none / 无)", language="text")
            st.write("理由 / Reason")
            reason_text = str(plan_payload.get("reason", latest.get("reason", ""))).strip()
            st.info(reason_text or "(empty / 空)")

        with full_tab:
            if isinstance(research_result, dict):
                sandbox = research_result.get("sandbox_artifacts", {})
                bundle_ref = research_result.get("data_bundle_ref", {})
                metrics = research_result.get("metrics", {})
                provenance = research_result.get("provenance", [])
                backend = str(sandbox.get("backend", "unknown")).strip() or "unknown"
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Success / 成功", "Yes / 是" if sandbox.get("success") else "No / 否")
                c2.metric("Retry Count / 重试次数", int(sandbox.get("retry_count", 0)))
                c3.metric("Data Source / 数据源", str(bundle_ref.get("data_source", latest.get("data_source", ""))))
                c4.metric("Sandbox Backend / 沙箱后端", backend)

                stderr_text = str(sandbox.get("stderr", ""))
                if "local-process fallback" in stderr_text.lower() or "local-process-fallback" in backend:
                    st.warning("当前结果来自本地进程降级执行，不是真容器沙箱。 / Result came from local-process fallback.")
                elif backend.startswith("docker:"):
                    st.success(f"已使用真实容器沙箱执行 / Real Docker sandbox used: `{backend}`")

                st.markdown("**沙箱代码 / Sandbox Code**")
                st.code(sandbox.get("code", ""), language="python")

                evidence_payload = {
                    "run_id": research_result.get("run_id", ""),
                    "bundle": {
                        "data_source": bundle_ref.get("data_source", ""),
                        "asof": bundle_ref.get("asof", ""),
                        "symbol": bundle_ref.get("symbol", ""),
                        "market": bundle_ref.get("market", ""),
                        "interval": bundle_ref.get("interval", ""),
                        "record_count": bundle_ref.get("record_count", 0),
                    },
                    "sandbox": {
                        "backend": backend,
                        "retry_count": int(sandbox.get("retry_count", 0)),
                        "success": bool(sandbox.get("success", False)),
                    },
                    "metrics": metrics,
                    "provenance_count": len(provenance) if isinstance(provenance, list) else 0,
                }
                st.markdown("**证据链 / Evidence Chain**")
                st.json(evidence_payload)
                st.markdown("**沙箱标准输出 / Sandbox Stdout**")
                st.code(sandbox.get("stdout", "") or "(empty / 空)", language="text")
                st.markdown("**沙箱错误输出 / Sandbox Stderr**")
                st.code(stderr_text or "(empty / 空)", language="text")
                st.markdown("**异常回溯 / Traceback**")
                st.code(str(sandbox.get("traceback") or "(none / 无)"), language="text")
                st.markdown("**指标溯源 / Metric Provenance**")
                st.json(provenance if isinstance(provenance, list) else [])
            else:
                st.info("暂无完整分析沙箱产物。 / No full-analysis sandbox artifacts yet.")

        with fused_tab:
            fused = None
            fused_insights = {}
            if isinstance(research_result, dict):
                fused_insights = research_result.get("fused_insights", {})
                if isinstance(fused_insights, dict):
                    fused = fused_insights.get("raw")
            if not isinstance(fused, dict):
                fused = latest.get("market_news_analysis")
            if isinstance(fused, dict):
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Latest Close / 最新收盘", float(fused.get("latest_close", 0.0)))
                c2.metric("Period Change / 区间涨跌(%)", float(fused.get("period_change_pct", 0.0)))
                c3.metric(
                    "Trend / 趋势",
                    f"{fused.get('trend_signal_zh', fused.get('trend_signal', ''))} ({fused.get('trend_signal', '')})",
                )
                c4.metric("Sentiment / 情绪分", float(fused.get("sentiment_score", 0.0)))

                st.markdown("**分析过程 / Analysis Process**")
                steps = [str(step) for step in fused_insights.get("analysis_steps", fused.get("analysis_steps", []))]
                st.code("\n".join(steps) if steps else "(none / 无)", language="text")

                st.markdown("**技术面快照 / Technical Snapshot**")
                snapshot_df = pd.DataFrame(
                    [
                        {
                            "Symbol(Name) / 标的(名称)": fused.get("symbol_display", fused.get("symbol", "")),
                            "Company / 公司名称": fused.get("company_name", ""),
                            "Symbol / 标的代码": fused.get("symbol", ""),
                            "Period / 分析周期": fused.get("period", ""),
                            "Start Close / 起始收盘价": fused.get("start_close", 0.0),
                            "Latest Close / 最新收盘价": fused.get("latest_close", 0.0),
                            "MA5 / 5日均线": fused.get("ma5", 0.0),
                            "MA20 / 20日均线": fused.get("ma20", 0.0),
                            "MA Bias(%) / 偏离20日线": fused.get("ma_bias_pct", 0.0),
                            "RSI14 / 强弱指标": fused.get("rsi14", 0.0),
                            "Volatility(%) / 波动率": fused.get("volatility_pct", 0.0),
                            "Period High / 区间最高": fused.get("period_high", 0.0),
                            "Period Low / 区间最低": fused.get("period_low", 0.0),
                            "Amplitude(%) / 区间振幅": fused.get("amplitude_pct", 0.0),
                            "5D Change(%) / 近5日涨跌": fused.get("short_change_pct", 0.0),
                            "Volume Ratio / 量能比": fused.get("volume_ratio", 0.0),
                            "Sentiment Label / 情绪标签": (
                                f"{fused.get('sentiment_label_zh', fused.get('sentiment_label', ''))}"
                                f" ({fused.get('sentiment_label', '')})"
                            ),
                        }
                    ]
                )
                st.dataframe(snapshot_df, use_container_width=True)

                st.markdown("**价格图表 / Price Chart**")
                chart_symbol = str(fused.get("symbol", "")).strip()
                chart_period = str(fused.get("period", period)).strip() or period
                chart_df = _cached_ohlcv_for_chart(chart_symbol, chart_period)
                chart_mode = st.radio(
                    "图表类型 / Chart Type",
                    ["折线图 / Line", "行情K线(均线+成交量) / Market Candlestick"],
                    horizontal=True,
                    key=f"chart_mode_{chart_symbol}_{chart_period}",
                )
                if chart_df.empty:
                    st.info("当前数据源未返回可绘制图表的OHLC数据。 / No chartable OHLC data from current data source.")
                elif chart_mode == "折线图 / Line":
                    line_df = chart_df.set_index("Date")[["Close"]].rename(columns={"Close": "Close / 收盘价"})
                    st.line_chart(line_df, use_container_width=True, height=300)
                else:
                    _render_candlestick(chart_df, title=f"{chart_symbol} ({chart_period}) Market Candlestick")

                with st.expander("查看原始行情表 / View Raw OHLC Table", expanded=False):
                    st.dataframe(chart_df, use_container_width=True)

                st.markdown("**详细分析 / Detailed Analysis**")
                for line in fused.get("detailed_analysis", []):
                    st.write(f"- {line}")

                st.markdown("**情景推演 / Scenario Analysis**")
                for line in fused.get("scenario_analysis", []):
                    st.write(f"- {line}")

                st.markdown("**风险提示 / Risk Points**")
                for line in fused.get("risk_points", []):
                    st.write(f"- {line}")

                st.markdown("**关键观察点 / Watch Points**")
                for line in fused.get("watch_points", []):
                    st.write(f"- {line}")

                st.markdown("**关键事件 / Key Events**")
                for event in fused.get("key_events", []):
                    st.write(f"- {event}")

                st.markdown("**新闻列表 / News Headlines**")
                news_items = fused.get("news_items", [])
                if news_items:
                    news_df = pd.DataFrame(news_items).rename(
                        columns={
                            "title": "Title / 标题",
                            "link": "Link / 链接",
                            "published_at": "Published At / 发布时间",
                            "summary": "Summary / 摘要",
                            "source": "Source / 来源",
                        }
                    )
                    st.dataframe(news_df, use_container_width=True)
                else:
                    st.info("未获取到股票相关新闻。 / No symbol-specific headlines found.")

                st.markdown("**综合判断 / Final Assessment**")
                summary = str(fused_insights.get("summary", fused.get("final_assessment", "")))
                st.info(summary)
                st.markdown("**综合解读 / Expanded Interpretation**")
                st.write(
                    f"- 当前趋势：{fused.get('trend_signal_zh', '')}，情绪：{fused.get('sentiment_label_zh', '')}（{fused.get('sentiment_score', 0.0):.2f}/100）。"
                )
                st.write(
                    f"- 技术位置：收盘价 {float(fused.get('latest_close', 0.0)):.2f}，MA20 {float(fused.get('ma20', 0.0)):.2f}，偏离 {float(fused.get('ma_bias_pct', 0.0)):.2f}%。"
                )
                st.write(
                    f"- 风险波动：RSI14 {float(fused.get('rsi14', 0.0)):.2f}，波动率 {float(fused.get('volatility_pct', 0.0)):.2f}% ，量能比 {float(fused.get('volume_ratio', 0.0)):.2f}。"
                )
            else:
                st.info("暂无行情+新闻融合分析结果。 / No market+news fused analysis yet.")

        st.subheader("运行历史 / Run History")
        st.dataframe(st.session_state.history, use_container_width=True)


if __name__ == "__main__":
    main()
