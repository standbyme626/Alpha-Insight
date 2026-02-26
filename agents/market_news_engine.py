"""Market + news fused analysis engine for Planner Console."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd

from core.models import DataBundle
from tools.market_data import fetch_market_data, get_company_name, normalize_market_symbol
from tools.news_data import fetch_symbol_news


POSITIVE_KEYWORDS = (
    "surge",
    "rise",
    "gain",
    "growth",
    "beat",
    "bullish",
    "upgrade",
    "profit",
    "buyback",
    "record",
    "上涨",
    "增长",
    "盈利",
    "利好",
)
NEGATIVE_KEYWORDS = (
    "drop",
    "fall",
    "decline",
    "loss",
    "downgrade",
    "bearish",
    "risk",
    "weak",
    "cut",
    "warning",
    "probe",
    "lawsuit",
    "下跌",
    "亏损",
    "利空",
    "风险",
    "调查",
)


@dataclass
class MarketNewsAnalysisResult:
    symbol: str
    company_name: str
    symbol_display: str
    period: str
    request: str
    latest_close: float
    start_close: float
    period_change_pct: float
    ma5: float
    ma20: float
    rsi14: float
    volatility_pct: float
    trend_signal: str
    trend_signal_zh: str
    sentiment_score: float
    sentiment_label: str
    sentiment_label_zh: str
    sentiment_positive_hits: int
    sentiment_negative_hits: int
    period_high: float
    period_low: float
    amplitude_pct: float
    ma_bias_pct: float
    latest_volume: float
    avg_volume: float
    volume_ratio: float
    short_change_pct: float
    key_events: list[str]
    detailed_analysis: list[str]
    scenario_analysis: list[str]
    risk_points: list[str]
    watch_points: list[str]
    final_assessment: str
    analysis_steps: list[str]
    news_items: list[dict[str, Any]]
    market_data_source: str
    market_data_rows: int
    market_data_asof: str


def _compute_rsi(close: pd.Series, period: int = 14) -> float:
    if close.empty:
        return 50.0
    delta = close.diff()
    up = delta.clip(lower=0)
    down = (-delta).clip(lower=0)
    roll_up = up.rolling(period).mean()
    roll_down = down.rolling(period).mean().replace(0, pd.NA)
    rs = roll_up / roll_down
    rsi = (100 - (100 / (1 + rs))).fillna(50)
    return float(rsi.iloc[-1])


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _score_sentiment(news_items: list[dict[str, Any]]) -> tuple[float, str, int, int]:
    score = 50.0
    pos_hits_total = 0
    neg_hits_total = 0
    for item in news_items:
        text = f"{item.get('title', '')} {item.get('summary', '')}".lower()
        pos = sum(1 for kw in POSITIVE_KEYWORDS if kw in text)
        neg = sum(1 for kw in NEGATIVE_KEYWORDS if kw in text)
        pos_hits_total += pos
        neg_hits_total += neg
        score += 2.5 * pos
        score -= 2.5 * neg
    score = _clamp(score, 0.0, 100.0)
    if score >= 60:
        label = "positive"
    elif score <= 40:
        label = "negative"
    else:
        label = "neutral"
    return round(score, 2), label, pos_hits_total, neg_hits_total


def _resolve_trend(latest_close: float, ma5: float, ma20: float, rsi14: float) -> str:
    if latest_close >= ma5 >= ma20 and rsi14 < 70:
        return "bullish"
    if latest_close <= ma5 <= ma20 and rsi14 > 30:
        return "bearish"
    return "sideways"


def _trend_label_zh(trend_signal: str) -> str:
    mapping = {
        "bullish": "偏强",
        "bearish": "偏弱",
        "sideways": "震荡",
    }
    return mapping.get(trend_signal, trend_signal)


def _sentiment_label_zh(sentiment_label: str) -> str:
    mapping = {
        "positive": "偏多",
        "negative": "偏空",
        "neutral": "中性",
    }
    return mapping.get(sentiment_label, sentiment_label)


def _build_key_events(news_items: list[dict[str, Any]], limit: int = 3) -> list[str]:
    if not news_items:
        return ["所选区间未抓到与该标的强相关的重大新闻。"]
    out: list[str] = []
    for item in news_items[:limit]:
        when = str(item.get("published_at", "")).strip()
        title = str(item.get("title", "")).strip()
        if when:
            out.append(f"{when}: {title}")
        else:
            out.append(title)
    return out


def _build_detailed_analysis(
    *,
    latest_close: float,
    start_close: float,
    period_change_pct: float,
    short_change_pct: float,
    ma5: float,
    ma20: float,
    ma_bias_pct: float,
    trend_signal: str,
    rsi14: float,
    volatility_pct: float,
    period_high: float,
    period_low: float,
    amplitude_pct: float,
    volume_ratio: float,
    sentiment_label: str,
    sentiment_score: float,
) -> list[str]:
    trend_zh = _trend_label_zh(trend_signal)
    sentiment_zh = _sentiment_label_zh(sentiment_label)
    volume_text = "放量" if volume_ratio >= 1.15 else ("缩量" if volume_ratio <= 0.85 else "量能平稳")
    lines = [
        (
            "价格结构：区间起点/终点分别为 "
            f"{start_close:.2f}/{latest_close:.2f}，区间涨跌幅 {period_change_pct:.2f}% ，最近5个交易日涨跌幅 {short_change_pct:.2f}% 。"
        ),
        (
            f"趋势与均线：当前趋势判定为{trend_zh}，MA5={ma5:.2f}，MA20={ma20:.2f}，"
            f"收盘价相对MA20偏离 {ma_bias_pct:.2f}% 。"
        ),
        (
            f"动量与波动：RSI14={rsi14:.2f}，波动率={volatility_pct:.2f}% ，"
            f"区间最高/最低={period_high:.2f}/{period_low:.2f}，振幅={amplitude_pct:.2f}% 。"
        ),
        f"成交量观察：最新量/均量比={volume_ratio:.2f}（{volume_text}）。",
        f"新闻情绪：当前为{sentiment_zh}（情绪分 {sentiment_score:.2f}/100）。",
    ]
    return lines


def _build_assessment(
    *,
    trend_signal: str,
    rsi14: float,
    sentiment_label: str,
    sentiment_score: float,
) -> str:
    trend_zh = _trend_label_zh(trend_signal)
    sentiment_zh = _sentiment_label_zh(sentiment_label)
    if trend_signal == "bullish" and sentiment_score >= 55:
        core = "技术面与新闻面同向偏强，短线更偏向强势节奏。"
    elif trend_signal == "bearish" and sentiment_score <= 45:
        core = "技术面与新闻面同向偏弱，短线需优先关注回撤风险。"
    else:
        core = "技术面与新闻面信号分歧，当前更接近震荡市，需等待更明确催化。"

    risk = ""
    if rsi14 >= 70:
        risk = " 当前 RSI 偏高，需警惕短期过热后的波动放大。"
    elif rsi14 <= 30:
        risk = " 当前 RSI 偏低，需关注超跌反抽与趋势延续的博弈。"

    return (
        f"综合判断：{core}{risk} "
        f"情绪方向为{sentiment_zh}（{sentiment_score:.2f}/100），趋势判定为{trend_zh}。"
        " 本页面输出仅用于信息分析与研究讨论，不构成任何投资建议。"
    )


def _build_scenario_analysis(
    *,
    trend_signal: str,
    sentiment_score: float,
    ma_bias_pct: float,
) -> list[str]:
    out: list[str] = []
    if trend_signal == "bullish":
        out.append("偏强情景：若价格继续站稳 MA20 且情绪分维持在 55 以上，短线可能延续上行。")
    elif trend_signal == "bearish":
        out.append("偏弱情景：若价格持续运行在 MA20 下方且情绪分低于 45，回撤压力可能延续。")
    else:
        out.append("震荡情景：若价格继续围绕 MA20 波动且情绪无明显方向，短期大概率维持区间震荡。")

    if ma_bias_pct >= 4:
        out.append("价格相对 MA20 偏离较大（高位），需防范回归均值的波动。")
    elif ma_bias_pct <= -4:
        out.append("价格相对 MA20 偏离较大（低位），关注是否出现放量修复。")
    else:
        out.append("价格与 MA20 偏离不大，市场分歧相对温和。")

    if sentiment_score >= 60:
        out.append("情绪偏多，如有业绩/政策催化，可能强化当前方向。")
    elif sentiment_score <= 40:
        out.append("情绪偏空，需警惕负面事件叠加技术破位。")
    else:
        out.append("情绪中性，后续方向更依赖量价和关键位突破。")
    return out


def _build_risk_points(
    *,
    rsi14: float,
    volatility_pct: float,
    sentiment_score: float,
) -> list[str]:
    points = [
        f"当前波动率约 {volatility_pct:.2f}% ，需按波动率匹配仓位与风险预算。",
        "新闻存在时滞与噪声，情绪分只应作为辅助信号，不宜单独决策。",
    ]
    if rsi14 >= 70:
        points.append("RSI 处于偏高区，短线追涨风险上升。")
    elif rsi14 <= 30:
        points.append("RSI 处于偏低区，需区分超跌反弹与趋势延续。")
    else:
        points.append("RSI 位于中性区，趋势强弱仍需后续K线确认。")

    if sentiment_score <= 40:
        points.append("情绪偏空，需防范突发利空放大波动。")
    elif sentiment_score >= 60:
        points.append("情绪偏多，需警惕预期兑现后的回撤。")
    return points


def _build_watch_points(
    *,
    latest_close: float,
    ma5: float,
    ma20: float,
    period_high: float,
    period_low: float,
) -> list[str]:
    return [
        f"短线观察位：MA5={ma5:.2f}（价格强弱第一观察位）。",
        f"中短线分界：MA20={ma20:.2f}（是否站稳决定节奏偏强或偏弱）。",
        f"区间上沿：{period_high:.2f}；若放量突破，可能打开上行空间。",
        f"区间下沿：{period_low:.2f}；若放量失守，需警惕下行加速。",
        f"当前收盘价：{latest_close:.2f}，建议结合次日量价验证方向延续性。",
    ]


async def run_market_news_analysis(
    *,
    request: str,
    symbol: str,
    period: str = "1mo",
    interval: str = "1d",
    news_limit: int = 8,
    market_data_bundle: DataBundle | dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_symbol = normalize_market_symbol(symbol, market="auto")
    company_name = get_company_name(normalized_symbol, resolve_remote=True)
    symbol_display = (
        f"{normalized_symbol}({company_name})"
        if company_name and company_name != normalized_symbol
        else normalized_symbol
    )
    bundle_payload: dict[str, Any] | None = None
    if isinstance(market_data_bundle, DataBundle):
        bundle_payload = market_data_bundle.to_serializable_dict()
    elif isinstance(market_data_bundle, dict):
        bundle_payload = market_data_bundle

    if bundle_payload and isinstance(bundle_payload.get("records"), list) and bundle_payload.get("records"):
        bundle_symbol = normalize_market_symbol(str(bundle_payload.get("symbol", "")), market="auto").strip().upper()
        if bundle_symbol and bundle_symbol != normalized_symbol:
            raise RuntimeError(
                f"Market data bundle symbol mismatch: expected {normalized_symbol}, got {bundle_symbol}"
            )
        records = list(bundle_payload.get("records", []))
        market_data_source = str(bundle_payload.get("data_source", "bundle")).strip() or "bundle"
        market_data_asof = str(bundle_payload.get("asof", "")).strip()
        fetch_step = (
            "复用共享行情包 / Reuse shared market bundle"
            f" [symbol={normalized_symbol}, rows={len(records)}, period={period}, interval={interval}]"
        )
    else:
        result = await fetch_market_data(normalized_symbol, period=period, interval=interval)
        if not result.ok or not result.records:
            raise RuntimeError(f"Market data unavailable for {normalized_symbol}: {result.message}")
        records = list(result.records)
        market_data_source = str((result.bundle.data_source if result.bundle else "api")).strip() or "api"
        market_data_asof = str((result.bundle.asof.isoformat() if result.bundle else "")).strip()
        fetch_step = (
            "拉取行情数据 / Fetch OHLCV from market API (yfinance)"
            f" [symbol={normalized_symbol}, rows={len(records)}, period={period}, interval={interval}]"
        )

    df = pd.DataFrame(records)
    if "Close" not in df.columns or len(df) < 2:
        raise RuntimeError("Insufficient close-price data for analysis.")

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)
    close = pd.to_numeric(df["Close"], errors="coerce").ffill().bfill()
    volume = pd.to_numeric(df.get("Volume"), errors="coerce").fillna(0) if "Volume" in df.columns else pd.Series([0.0] * len(df))
    if close.empty:
        raise RuntimeError("Close series is empty after normalization.")

    latest_close = float(close.iloc[-1])
    start_close = float(close.iloc[0])
    period_change_pct = 0.0 if start_close == 0 else ((latest_close - start_close) / start_close) * 100.0
    ma5 = float(close.rolling(5).mean().fillna(close).iloc[-1])
    ma20 = float(close.rolling(20).mean().fillna(close).iloc[-1])
    rsi14 = _compute_rsi(close, period=14)
    volatility_pct = float(close.pct_change().dropna().std() * 100.0) if len(close) > 2 else 0.0
    period_high = float(close.max())
    period_low = float(close.min())
    amplitude_pct = 0.0 if period_low == 0 else ((period_high - period_low) / period_low) * 100.0
    ma_bias_pct = 0.0 if ma20 == 0 else ((latest_close - ma20) / ma20) * 100.0
    latest_volume = float(volume.iloc[-1]) if not volume.empty else 0.0
    avg_volume = float(volume.mean()) if not volume.empty else 0.0
    volume_ratio = 0.0 if avg_volume == 0 else latest_volume / avg_volume
    short_change_pct = 0.0
    if len(close) >= 6:
        base = float(close.iloc[-6])
        short_change_pct = 0.0 if base == 0 else ((latest_close - base) / base) * 100.0

    news_items = await fetch_symbol_news(normalized_symbol, limit=news_limit)
    sentiment_score, sentiment_label, pos_hits_total, neg_hits_total = _score_sentiment(news_items)
    trend_signal = _resolve_trend(latest_close, ma5, ma20, rsi14)
    trend_signal_zh = _trend_label_zh(trend_signal)
    sentiment_label_zh = _sentiment_label_zh(sentiment_label)
    key_events = _build_key_events(news_items, limit=3)
    detailed_analysis = _build_detailed_analysis(
        latest_close=latest_close,
        start_close=start_close,
        period_change_pct=period_change_pct,
        short_change_pct=short_change_pct,
        ma5=ma5,
        ma20=ma20,
        ma_bias_pct=ma_bias_pct,
        trend_signal=trend_signal,
        rsi14=rsi14,
        volatility_pct=volatility_pct,
        period_high=period_high,
        period_low=period_low,
        amplitude_pct=amplitude_pct,
        volume_ratio=volume_ratio,
        sentiment_label=sentiment_label,
        sentiment_score=sentiment_score,
    )
    scenario_analysis = _build_scenario_analysis(
        trend_signal=trend_signal,
        sentiment_score=sentiment_score,
        ma_bias_pct=ma_bias_pct,
    )
    risk_points = _build_risk_points(
        rsi14=rsi14,
        volatility_pct=volatility_pct,
        sentiment_score=sentiment_score,
    )
    watch_points = _build_watch_points(
        latest_close=latest_close,
        ma5=ma5,
        ma20=ma20,
        period_high=period_high,
        period_low=period_low,
    )
    final_assessment = _build_assessment(
        trend_signal=trend_signal,
        rsi14=rsi14,
        sentiment_label=sentiment_label,
        sentiment_score=sentiment_score,
    )
    analysis_steps = [
        fetch_step,
        (
            "计算技术指标 / Compute MA5, MA20, RSI14, volatility, volume ratio"
            f" [MA5={ma5:.2f}, MA20={ma20:.2f}, RSI14={rsi14:.2f}, Volatility={volatility_pct:.2f}%]"
        ),
        f"抓取相关新闻 / Fetch symbol-linked news from RSS feeds [count={len(news_items)}]",
        (
            "情绪打分 / Score news sentiment by keyword hits"
            f" [positive_hits={pos_hits_total}, negative_hits={neg_hits_total}, score={sentiment_score:.2f}]"
        ),
        (
            "信号融合输出 / Fuse technical + sentiment into final assessment"
            f" [trend={trend_signal}, sentiment={sentiment_label}]"
        ),
    ]

    payload = MarketNewsAnalysisResult(
        symbol=normalized_symbol,
        company_name=company_name,
        symbol_display=symbol_display,
        period=period,
        request=request,
        latest_close=round(latest_close, 4),
        start_close=round(start_close, 4),
        period_change_pct=round(period_change_pct, 4),
        ma5=round(ma5, 4),
        ma20=round(ma20, 4),
        rsi14=round(rsi14, 4),
        volatility_pct=round(volatility_pct, 4),
        trend_signal=trend_signal,
        trend_signal_zh=trend_signal_zh,
        sentiment_score=sentiment_score,
        sentiment_label=sentiment_label,
        sentiment_label_zh=sentiment_label_zh,
        sentiment_positive_hits=pos_hits_total,
        sentiment_negative_hits=neg_hits_total,
        period_high=round(period_high, 4),
        period_low=round(period_low, 4),
        amplitude_pct=round(amplitude_pct, 4),
        ma_bias_pct=round(ma_bias_pct, 4),
        latest_volume=round(latest_volume, 4),
        avg_volume=round(avg_volume, 4),
        volume_ratio=round(volume_ratio, 4),
        short_change_pct=round(short_change_pct, 4),
        key_events=key_events,
        detailed_analysis=detailed_analysis,
        scenario_analysis=scenario_analysis,
        risk_points=risk_points,
        watch_points=watch_points,
        final_assessment=final_assessment,
        analysis_steps=analysis_steps,
        news_items=news_items,
        market_data_source=market_data_source,
        market_data_rows=int(len(df)),
        market_data_asof=market_data_asof,
    )
    return asdict(payload)
