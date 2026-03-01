"""Async market data adapter based on yfinance."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from io import StringIO
from typing import Any

import pandas as pd
import requests
try:  # pragma: no cover - optional runtime dependency
    import yfinance as yf
except Exception:  # pragma: no cover - optional runtime dependency
    yf = None  # type: ignore[assignment]

from core.models import DataBundle
from core.tool_result import ToolResult, build_tool_result


@dataclass
class MarketDataResult:
    ok: bool
    symbol: str
    message: str
    records: list[dict[str, Any]]
    bundle: DataBundle | None = None
    tool_result: ToolResult | None = None


def market_data_result_to_tool_result(
    result: MarketDataResult,
    *,
    period: str = "",
    interval: str = "",
) -> ToolResult:
    if result.tool_result is not None:
        return result.tool_result
    raw_bundle = result.bundle.to_serializable_dict() if result.bundle is not None else None
    raw_payload = raw_bundle if raw_bundle is not None else {
        "symbol": result.symbol,
        "records": result.records,
    }
    return build_tool_result(
        source="market_data:yfinance",
        confidence=0.95 if result.ok else 0.0,
        raw=raw_payload,
        error="" if result.ok else result.message,
        meta={
            "symbol": result.symbol,
            "period": period,
            "interval": interval,
            "record_count": len(result.records),
        },
    )


_CN_COMPANY_SYMBOL_MAP: dict[str, str] = {
    "贵州茅台": "600519.SS",
    "宁德时代": "300750.SZ",
    "招商银行": "600036.SS",
    "中国平安": "601318.SS",
    "比亚迪": "002594.SZ",
    "五粮液": "000858.SZ",
    "隆基绿能": "601012.SS",
    "迈瑞医疗": "300760.SZ",
    "中芯国际": "688981.SS",
    "海康威视": "002415.SZ",
}

_CN_DYNAMIC_NAME_CACHE: dict[str, str] = {}
_HK_DYNAMIC_NAME_CACHE: dict[str, str] = {}
_US_DYNAMIC_NAME_CACHE: dict[str, str] = {}
_US_CACHE_REFRESHED: bool = False

# Curated A-share large-cap monitoring pool (100 symbols).
CN_TOP100_SYMBOLS: list[str] = [
    "600519.SS", "300750.SZ", "600036.SS", "601318.SS", "601166.SS", "601398.SS", "601288.SS", "601988.SS",
    "601939.SS", "601857.SS", "601658.SS", "601628.SS", "601601.SS", "601688.SS", "601995.SS", "601888.SS",
    "600276.SS", "600030.SS", "600309.SS", "600900.SS", "600031.SS", "600809.SS", "600585.SS", "600690.SS",
    "600703.SS", "600436.SS", "600089.SS", "600406.SS", "600887.SS", "600196.SS", "600132.SS", "600426.SS",
    "600000.SS", "600016.SS", "600104.SS", "600048.SS", "600050.SS", "600588.SS", "600660.SS", "600999.SS",
    "601012.SS", "601138.SS", "601211.SS", "601225.SS", "601229.SS", "601360.SS", "601555.SS", "601600.SS",
    "601669.SS", "601727.SS", "601800.SS", "601816.SS", "601818.SS", "601838.SS", "601865.SS", "601901.SS",
    "601919.SS", "601985.SS", "603259.SS", "603288.SS", "603501.SS", "603799.SS", "603986.SS", "605499.SS",
    "000001.SZ", "000002.SZ", "000063.SZ", "000333.SZ", "000338.SZ", "000568.SZ", "000596.SZ", "000625.SZ",
    "000651.SZ", "000661.SZ", "000725.SZ", "000858.SZ", "000876.SZ", "000938.SZ", "000977.SZ", "001979.SZ",
    "002027.SZ", "002142.SZ", "002230.SZ", "002241.SZ", "002304.SZ", "002352.SZ", "002371.SZ", "002415.SZ",
    "002460.SZ", "002475.SZ", "002594.SZ", "002714.SZ", "002812.SZ", "002916.SZ", "300014.SZ", "300122.SZ",
    "300124.SZ", "300274.SZ", "300308.SZ", "300760.SZ",
]


def _to_cn_symbol(code: str) -> str:
    normalized = code.strip().upper()
    if not normalized:
        return ""
    if "." in normalized:
        return normalized
    if re.fullmatch(r"\d{6}", normalized):
        if normalized.startswith(("6", "9")):
            return f"{normalized}.SS"
        return f"{normalized}.SZ"
    return normalized


def _fetch_cn_top100_by_market_cap() -> list[dict[str, str]]:
    try:
        import akshare as ak  # type: ignore
    except Exception:
        return []

    try:
        df = ak.stock_zh_a_spot_em()
    except Exception:
        return []

    if not isinstance(df, pd.DataFrame) or df.empty:
        return []

    code_col = "代码" if "代码" in df.columns else None
    name_col = "名称" if "名称" in df.columns else None
    mv_col = "总市值" if "总市值" in df.columns else None
    if not code_col or not name_col or not mv_col:
        return []

    data = df[[code_col, name_col, mv_col]].copy()
    data[mv_col] = pd.to_numeric(data[mv_col], errors="coerce")
    data = data.dropna(subset=[mv_col]).sort_values(mv_col, ascending=False).head(100)

    rows: list[dict[str, str]] = []
    for _, row in data.iterrows():
        symbol = _to_cn_symbol(str(row[code_col]))
        if not symbol:
            continue
        name = str(row[name_col]).strip() or symbol
        _CN_DYNAMIC_NAME_CACHE[symbol] = name
        rows.append({"symbol": symbol, "name": name})
    return rows


def get_cn_top100_watchlist(use_live_market_cap: bool = False) -> list[str]:
    if use_live_market_cap:
        rows = _fetch_cn_top100_by_market_cap()
        if rows:
            return [row["symbol"] for row in rows]
    return list(CN_TOP100_SYMBOLS)


def _http_get(url: str) -> str:
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn"}, timeout=20)
    response.raise_for_status()
    return response.text


def _fetch_cn_name_from_sina(symbol: str) -> str:
    normalized = normalize_market_symbol(symbol, market="cn")
    if normalized.endswith(".SZ"):
        code = f"sz{normalized[:6]}"
    elif normalized.endswith(".SS"):
        code = f"sh{normalized[:6]}"
    else:
        return normalized
    try:
        text = _http_get(f"https://hq.sinajs.cn/list={code}")
        match = re.search(r'="([^,]+),', text)
        if match and match.group(1).strip():
            return match.group(1).strip()
    except Exception:
        pass
    return normalized


def _fetch_cn_names_batch_from_sina(symbols: list[str]) -> dict[str, str]:
    normalized_symbols: list[str] = []
    code_pairs: list[tuple[str, str]] = []
    for raw in symbols:
        normalized = normalize_market_symbol(raw, market="cn")
        if normalized.endswith(".SZ"):
            code_pairs.append((normalized, f"sz{normalized[:6]}"))
            normalized_symbols.append(normalized)
        elif normalized.endswith(".SS"):
            code_pairs.append((normalized, f"sh{normalized[:6]}"))
            normalized_symbols.append(normalized)

    if not code_pairs:
        return {}

    query_codes = ",".join(code for _, code in code_pairs)
    try:
        text = _http_get(f"https://hq.sinajs.cn/list={query_codes}")
    except Exception:
        return {}

    raw_name_map: dict[str, str] = {}
    for match in re.finditer(r'var hq_str_(?P<code>[a-z]{2}\d{6})="(?P<body>[^"]*)";', text):
        code = match.group("code")
        body = match.group("body")
        name = body.split(",", 1)[0].strip()
        if not name:
            continue
        raw_name_map[code] = name

    out: dict[str, str] = {}
    for normalized, raw_code in code_pairs:
        name = raw_name_map.get(raw_code, "").strip()
        if not name:
            continue
        _CN_DYNAMIC_NAME_CACHE[normalized] = name
        out[normalized] = name
    return out


def _fetch_hk_name_from_sina(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized.endswith(".HK"):
        return normalized
    try:
        code = f"{int(normalized.split('.')[0]):05d}"
    except Exception:
        return normalized
    try:
        text = _http_get(f"https://hq.sinajs.cn/list=hk{code}")
        match = re.search(r'="([^"]+)"', text)
        if not match:
            return normalized
        parts = match.group(1).split(",")
        if len(parts) >= 2:
            # hk payload: english_name, chinese_name, ...
            cn_name = parts[1].strip()
            if cn_name:
                return cn_name
    except Exception:
        pass
    return normalized


def _fetch_us_top100_by_market_proxy() -> list[dict[str, str]]:
    # Proxy set: Nasdaq-100 constituents (100 names, high-cap US tech leaders).
    try:
        html = _http_get("https://en.wikipedia.org/wiki/Nasdaq-100")
        tables = pd.read_html(StringIO(html))
    except Exception:
        return []
    for table in tables:
        columns = [str(col).lower() for col in table.columns]
        if "ticker" in columns and any("company" in col for col in columns):
            rows: list[dict[str, str]] = []
            ticker_col = table.columns[columns.index("ticker")]
            company_col = next(col for col in table.columns if "company" in str(col).lower())
            for _, row in table.iterrows():
                raw_symbol = str(row[ticker_col]).strip().upper().replace(".", "-")
                name = str(row[company_col]).strip()
                if not raw_symbol:
                    continue
                _US_DYNAMIC_NAME_CACHE[raw_symbol] = name
                rows.append({"symbol": raw_symbol, "name": name})
            return rows[:100]
    return []


def _fetch_hk_top100_by_market_proxy() -> list[dict[str, str]]:
    # Proxy set: HSI + HSCEI union (sorted by appearance), truncated to top 100.
    pages = [
        "https://en.wikipedia.org/wiki/Hang_Seng_Index",
        "https://en.wikipedia.org/wiki/Hang_Seng_China_Enterprises_Index",
    ]
    seen: set[str] = set()
    rows: list[dict[str, str]] = []
    for page in pages:
        try:
            html = _http_get(page)
            tables = pd.read_html(StringIO(html))
        except Exception:
            continue
        for table in tables:
            cols = [str(col).lower() for col in table.columns]
            if "ticker" not in cols or "name" not in cols:
                continue
            ticker_col = table.columns[cols.index("ticker")]
            name_col = table.columns[cols.index("name")]
            for _, row in table.iterrows():
                raw = str(row[ticker_col]).strip()
                match = re.search(r"(\d{1,5})", raw)
                if not match:
                    continue
                symbol = f"{int(match.group(1)):04d}.HK"
                if symbol in seen:
                    continue
                name = str(row[name_col]).strip() or symbol
                _HK_DYNAMIC_NAME_CACHE[symbol] = name
                rows.append({"symbol": symbol, "name": name})
                seen.add(symbol)
                if len(rows) >= 100:
                    return rows
    return rows


def _is_number_value(value: Any) -> bool:
    try:
        float(value)
        return True
    except Exception:
        return False


def _contains_cjk(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", text))


def _looks_like_us_symbol(text: str) -> bool:
    return bool(re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", text)) and "." not in text


def _refresh_us_name_cache_from_eastmoney() -> None:
    global _US_CACHE_REFRESHED
    if _US_CACHE_REFRESHED:
        return
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1,
        "pz": 1000,
        "po": 1,
        "np": 1,
        "fltt": 2,
        "invt": 2,
        "fid": "f20",
        "fields": "f12,f14",
        "fs": "m:105,m:106,m:107",
    }
    try:
        data = requests.get(url, params=params, timeout=20).json()
    except Exception:
        _US_CACHE_REFRESHED = True
        return
    diff = ((data or {}).get("data") or {}).get("diff") or []
    for item in diff:
        symbol = str(item.get("f12", "")).strip().upper().replace(".", "-")
        name = str(item.get("f14", "")).strip()
        if _looks_like_us_symbol(symbol) and name:
            _US_DYNAMIC_NAME_CACHE[symbol] = name
    _US_CACHE_REFRESHED = True


def _refresh_hk_name_cache_from_eastmoney() -> None:
    if _HK_DYNAMIC_NAME_CACHE:
        return
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1,
        "pz": 4000,
        "po": 1,
        "np": 1,
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fields": "f12,f14",
        "fs": "m:116+t:3,m:128+t:3",
    }
    try:
        data = requests.get(url, params=params, timeout=15).json()
    except Exception:
        return
    diff = ((data or {}).get("data") or {}).get("diff") or []
    for item in diff:
        code = str(item.get("f12", "")).strip()
        name = str(item.get("f14", "")).strip()
        if re.fullmatch(r"\d{4,5}", code) and name:
            symbol = f"{int(code):04d}.HK"
            _HK_DYNAMIC_NAME_CACHE[symbol] = name


def _fetch_hk_top100_by_market_cap_eastmoney() -> list[dict[str, str]]:
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1,
        "pz": 6000,
        "po": 1,
        "np": 1,
        "fltt": 2,
        "invt": 2,
        "fid": "f20",
        "fields": "f12,f14,f20",
        "fs": "m:116+t:3,m:128+t:3",
    }
    try:
        data = requests.get(url, params=params, timeout=20).json()
    except Exception:
        return []
    diff = ((data or {}).get("data") or {}).get("diff") or []
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    excluded_keywords = ("牛", "熊", "ETF", "槓桿", "反向", "期指", "债", "債", "票据", "認購", "認沽", "窝轮", "摩通", "瑞银")
    for item in diff:
        code = str(item.get("f12", "")).strip()
        name = str(item.get("f14", "")).strip()
        market_cap = item.get("f20")
        if not re.fullmatch(r"\d{4,5}", code):
            continue
        if not name or not _is_number_value(market_cap):
            continue
        if any(key in name for key in excluded_keywords):
            continue
        symbol = f"{int(code):04d}.HK"
        if symbol in seen:
            continue
        seen.add(symbol)
        _HK_DYNAMIC_NAME_CACHE[symbol] = name
        rows.append({"symbol": symbol, "name": name, "market_cap": float(market_cap)})
    rows.sort(key=lambda x: x["market_cap"], reverse=True)
    return [{"symbol": row["symbol"], "name": row["name"]} for row in rows[:100]]


def _fetch_us_top100_by_market_cap_eastmoney() -> list[dict[str, str]]:
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1,
        "pz": 8000,
        "po": 1,
        "np": 1,
        "fltt": 2,
        "invt": 2,
        "fid": "f20",
        "fields": "f12,f14,f20",
        "fs": "m:105,m:106,m:107",
    }
    try:
        data = requests.get(url, params=params, timeout=20).json()
    except Exception:
        return []
    diff = ((data or {}).get("data") or {}).get("diff") or []
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    excluded_keywords = ("ETF", "TRUST", "FUND", "3X", "2X", "BEAR", "BULL")
    for item in diff:
        symbol = str(item.get("f12", "")).strip().upper().replace(".", "-")
        name = str(item.get("f14", "")).strip()
        market_cap = item.get("f20")
        if not _looks_like_us_symbol(symbol):
            continue
        if symbol in seen or not name or not _is_number_value(market_cap):
            continue
        upper_name = name.upper()
        if any(key in upper_name for key in excluded_keywords):
            continue
        seen.add(symbol)
        _US_DYNAMIC_NAME_CACHE[symbol] = name
        rows.append({"symbol": symbol, "name": name, "market_cap": float(market_cap)})
    rows.sort(key=lambda x: x["market_cap"], reverse=True)
    return [{"symbol": row["symbol"], "name": row["name"]} for row in rows[:100]]


def get_market_top100_constituents(market: str) -> list[dict[str, str]]:
    market_lower = market.strip().lower()
    if market_lower == "cn":
        rows = _fetch_cn_top100_by_market_cap()
        if rows:
            return rows
        return get_cn_top100_constituents(resolve_remote=True)
    if market_lower == "hk":
        rows = _fetch_hk_top100_by_market_cap_eastmoney()
        if not rows:
            rows = _fetch_hk_top100_by_market_proxy()
        return rows
    if market_lower == "us":
        rows = _fetch_us_top100_by_market_cap_eastmoney()
        if not rows:
            rows = _fetch_us_top100_by_market_proxy()
        return rows
    return []


def get_market_top100_watchlist(market: str) -> list[str]:
    rows = get_market_top100_constituents(market)
    out: list[str] = []
    seen: set[str] = set()
    for row in rows:
        symbol = row["symbol"]
        if symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def get_company_names_batch(
    symbols: list[str],
    *,
    market: str = "auto",
    resolve_remote: bool = False,
) -> dict[str, str]:
    if not symbols:
        return {}

    market_lower = market.strip().lower()
    normalized: list[str] = []
    for symbol in symbols:
        value = normalize_market_symbol(symbol, market=market_lower or "auto")
        if value:
            normalized.append(value)

    out: dict[str, str] = {}
    cn_candidates = [sym for sym in normalized if sym.endswith((".SZ", ".SS"))]
    if cn_candidates:
        out.update(_fetch_cn_names_batch_from_sina(cn_candidates))

    for symbol in normalized:
        if symbol in out:
            continue
        out[symbol] = get_company_name(symbol, resolve_remote=resolve_remote)
    return out


@lru_cache(maxsize=2048)
def get_company_name(symbol: str, resolve_remote: bool = False) -> str:
    text = symbol.strip().upper()
    if text.endswith(".HK") or re.fullmatch(r"\d{3,5}", text):
        market_guess = "hk"
    elif text.endswith((".SZ", ".SS")) or re.fullmatch(r"\d{6}", text):
        market_guess = "cn"
    else:
        market_guess = "auto"
    normalized = normalize_market_symbol(symbol, market=market_guess)
    if normalized in _US_DYNAMIC_NAME_CACHE:
        cached = _US_DYNAMIC_NAME_CACHE[normalized]
        if resolve_remote and _looks_like_us_symbol(normalized) and not _contains_cjk(cached):
            _refresh_us_name_cache_from_eastmoney()
            return _US_DYNAMIC_NAME_CACHE.get(normalized, cached)
        return cached
    if normalized in _HK_DYNAMIC_NAME_CACHE:
        return _HK_DYNAMIC_NAME_CACHE[normalized]
    if normalized in _CN_DYNAMIC_NAME_CACHE:
        return _CN_DYNAMIC_NAME_CACHE[normalized]
    reverse_map = {v: k for k, v in _CN_COMPANY_SYMBOL_MAP.items()}
    if normalized in reverse_map:
        return reverse_map[normalized]
    if not resolve_remote:
        return normalized
    try:
        if _looks_like_us_symbol(normalized):
            _refresh_us_name_cache_from_eastmoney()
            if normalized in _US_DYNAMIC_NAME_CACHE:
                return _US_DYNAMIC_NAME_CACHE[normalized]
        if normalized.endswith(".HK"):
            hk_name = _fetch_hk_name_from_sina(normalized)
            if hk_name and hk_name != normalized:
                _HK_DYNAMIC_NAME_CACHE[normalized] = hk_name
                return hk_name
            _refresh_hk_name_cache_from_eastmoney()
            if normalized in _HK_DYNAMIC_NAME_CACHE:
                return _HK_DYNAMIC_NAME_CACHE[normalized]
        if normalized.endswith((".SZ", ".SS")):
            name = _fetch_cn_name_from_sina(normalized)
            if name and name != normalized:
                _CN_DYNAMIC_NAME_CACHE[normalized] = name
                return name
        if yf is not None:
            ticker = yf.Ticker(normalized)
            info = ticker.info if isinstance(ticker.info, dict) else {}
            name = info.get("shortName") or info.get("longName") or ""
            if isinstance(name, str) and name.strip():
                return name.strip()
    except Exception:
        pass
    return normalized


def get_cn_top100_constituents(use_live_market_cap: bool = False, resolve_remote: bool = False) -> list[dict[str, str]]:
    if use_live_market_cap:
        rows = _fetch_cn_top100_by_market_cap()
        if rows:
            return rows
    return [{"symbol": symbol, "name": get_company_name(symbol, resolve_remote=resolve_remote)} for symbol in CN_TOP100_SYMBOLS]


def normalize_market_symbol(symbol: str, market: str = "auto") -> str:
    text = symbol.strip()
    if not text:
        return ""

    if text in _CN_COMPANY_SYMBOL_MAP:
        return _CN_COMPANY_SYMBOL_MAP[text]

    normalized = text.upper()
    if "." in normalized:
        return normalized

    market_lower = market.strip().lower()
    if re.fullmatch(r"\d{3,5}", normalized):
        if market_lower in {"hk", "hongkong"}:
            return f"{int(normalized):04d}.HK"
        return normalized
    if re.fullmatch(r"\d{6}", normalized):
        if market_lower in {"cn", "china", "a-share", "ashare"}:
            if normalized.startswith(("6", "9")):
                return f"{normalized}.SS"
            return f"{normalized}.SZ"
        return normalized
    return normalized


def infer_market_from_symbol(symbol: str) -> str:
    value = symbol.strip().upper()
    if value.endswith((".SZ", ".SS")):
        return "cn"
    if value.endswith(".HK"):
        return "hk"
    if re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", value):
        return "us"
    return "auto"


def _to_json_safe_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc).isoformat()
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            return value.to_pydatetime().replace(tzinfo=timezone.utc).isoformat()
        return value.to_pydatetime().astimezone(timezone.utc).isoformat()
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def _to_json_safe_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in records:
        normalized_row: dict[str, Any] = {}
        for key, value in row.items():
            normalized_row[str(key)] = _to_json_safe_value(value)
        out.append(normalized_row)
    return out


def build_data_bundle(
    *,
    symbol: str,
    period: str,
    interval: str,
    records: list[dict[str, Any]],
    data_source: str,
) -> DataBundle:
    safe_records = _to_json_safe_records(records)
    asof = datetime.now(timezone.utc)
    if safe_records:
        maybe_asof = safe_records[-1].get("Date")
        if isinstance(maybe_asof, str) and maybe_asof.strip():
            try:
                parsed = datetime.fromisoformat(maybe_asof.replace("Z", "+00:00"))
                asof = parsed.astimezone(timezone.utc)
            except Exception:
                pass
    sample_keys = sorted(safe_records[0].keys()) if safe_records else []
    return DataBundle(
        records=safe_records,
        metadata={
            "period": period,
            "record_count": len(safe_records),
            "columns": sample_keys,
        },
        data_source=data_source,
        asof=asof,
        symbol=symbol,
        market=infer_market_from_symbol(symbol),
        interval=interval,
    )


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.reset_index().copy()
    if "Date" in out.columns:
        out["Date"] = pd.to_datetime(out["Date"], errors="coerce", utc=True)

    numeric_cols = out.select_dtypes(include=["number"]).columns.tolist()
    if numeric_cols:
        out[numeric_cols] = out[numeric_cols].ffill().bfill().fillna(0)
    return out


async def fetch_market_data(symbol: str, period: str = "1mo", interval: str = "1d") -> MarketDataResult:
    print("[DEBUG] QuantNode fetch_market_data Start")
    normalized_symbol = normalize_market_symbol(symbol)
    if not normalized_symbol:
        message = "数据未找到: symbol 为空。"
        result = MarketDataResult(
            ok=False,
            symbol=normalized_symbol,
            message=message,
            records=[],
            bundle=None,
        )
        result.tool_result = market_data_result_to_tool_result(result, period=period, interval=interval)
        return result

    if yf is None:
        message = "数据未找到: yfinance 未安装。"
        result = MarketDataResult(
            ok=False,
            symbol=normalized_symbol,
            message=message,
            records=[],
            bundle=None,
        )
        result.tool_result = market_data_result_to_tool_result(result, period=period, interval=interval)
        return result

    try:
        ticker = yf.Ticker(normalized_symbol)
        df = await asyncio.to_thread(ticker.history, period=period, interval=interval)
    except Exception as exc:  # pragma: no cover - network/runtime variability
        message = f"数据未找到: yfinance 请求失败 ({exc})"
        result = MarketDataResult(
            ok=False,
            symbol=normalized_symbol,
            message=message,
            records=[],
            bundle=None,
        )
        result.tool_result = market_data_result_to_tool_result(result, period=period, interval=interval)
        return result

    if df.empty:
        message = "数据未找到"
        result = MarketDataResult(
            ok=False,
            symbol=normalized_symbol,
            message=message,
            records=[],
            bundle=None,
        )
        result.tool_result = market_data_result_to_tool_result(result, period=period, interval=interval)
        return result

    normalized = _normalize_dataframe(df)
    records = normalized.to_dict(orient="records")
    bundle = build_data_bundle(
        symbol=normalized_symbol,
        period=period,
        interval=interval,
        records=records,
        data_source="yfinance",
    )
    result = MarketDataResult(
        ok=True,
        symbol=normalized_symbol,
        message="ok",
        records=bundle.records,
        bundle=bundle,
    )
    result.tool_result = market_data_result_to_tool_result(result, period=period, interval=interval)
    return result
