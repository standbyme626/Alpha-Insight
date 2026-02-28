from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any
from urllib.parse import urlparse


FORBIDDEN_VISIBLE_KEYS = {
    "schema_version",
    "_schema_version",
    "action_version",
    "traceback",
    "raw_error",
}

_POSITIVE_KEYWORDS = (
    "beat",
    "growth",
    "upgrade",
    "surge",
    "record",
    "profit",
    "buyback",
    "上涨",
    "增长",
    "利好",
    "超预期",
    "创新高",
)
_NEGATIVE_KEYWORDS = (
    "miss",
    "downgrade",
    "drop",
    "decline",
    "warning",
    "probe",
    "lawsuit",
    "risk",
    "下跌",
    "利空",
    "预警",
    "调查",
    "诉讼",
)

_EVENT_KEYWORDS: dict[str, tuple[str, ...]] = {
    "财报": (
        "earnings",
        "results",
        "revenue",
        "guidance",
        "quarter",
        "profit",
        "loss",
        "财报",
        "业绩",
        "营收",
        "利润",
        "亏损",
    ),
    "监管": (
        "sec",
        "regulator",
        "antitrust",
        "probe",
        "lawsuit",
        "compliance",
        "监管",
        "调查",
        "罚款",
        "诉讼",
    ),
    "产品": (
        "product",
        "launch",
        "release",
        "partnership",
        "chip",
        "model",
        "产品",
        "发布",
        "新品",
        "合作",
    ),
    "宏观": (
        "fed",
        "cpi",
        "inflation",
        "rate",
        "macro",
        "economy",
        "利率",
        "通胀",
        "宏观",
        "经济",
        "政策",
    ),
}


@dataclass(frozen=True)
class TopNewsItem:
    title: str
    published_at: str
    source: str
    publisher: str
    url: str
    impact: str
    category: str
    sentiment: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ThemeDigestItem:
    category: str
    count: int
    impact: str
    representative_title: str
    representative_time: str
    representative_source: str
    representative_publisher: str
    representative_url: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsDigest:
    window_days: int
    window_label: str
    total_count: int
    source_coverage: list[str]
    event_distribution: dict[str, int]
    sentiment_score: int
    sentiment_direction: str
    sentiment_range: str
    sentiment_method: str
    sentiment_sample_size: int
    top_themes: list[ThemeDigestItem]
    top_news: list[TopNewsItem]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["top_themes"] = [item.to_dict() for item in self.top_themes]
        data["top_news"] = [item.to_dict() for item in self.top_news]
        return data


def redact_user_visible_payload(value: Any) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key).strip()
            if not key_text:
                continue
            if _is_forbidden_key(key_text):
                continue
            out[key_text] = redact_user_visible_payload(item)
        return out
    if isinstance(value, list):
        return [redact_user_visible_payload(item) for item in value]
    return value


def extract_news_items(result: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = result.get("news")
    if not isinstance(candidates, list):
        candidates = result.get("news_items")
    if not isinstance(candidates, list):
        fused = result.get("fused_insights")
        if isinstance(fused, dict):
            raw = fused.get("raw")
            if isinstance(raw, dict):
                raw_items = raw.get("news_items")
                if isinstance(raw_items, list):
                    candidates = raw_items
    if not isinstance(candidates, list):
        return []
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in candidates:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        link = str(item.get("link", "")).strip()
        key = (link or title).lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item)
    out.sort(key=_news_sort_key, reverse=True)
    return out


def build_news_digest(news_items: list[dict[str, Any]], *, window_days: int = 7) -> NewsDigest:
    items = [item for item in news_items if isinstance(item, dict)]
    total = len(items)
    source_coverage = _source_coverage(items)
    event_distribution = {key: 0 for key in ("财报", "监管", "产品", "宏观", "其他")}
    score = 50
    top_rows: list[TopNewsItem] = []
    top_theme_rows: list[ThemeDigestItem] = []
    representative_by_category: dict[str, dict[str, str]] = {}
    sentiment_rank = {"偏多": 2, "中性": 1, "偏空": 0}
    for row in items:
        title = str(row.get("title", "")).strip() or "（无标题）"
        summary = str(row.get("summary", "")).strip()
        url = _resolve_news_url(row)
        publisher = _resolve_news_publisher(row, url=url)
        source = publisher
        category = classify_event_category(f"{title} {summary}")
        event_distribution[category] = event_distribution.get(category, 0) + 1
        sentiment = classify_sentiment(f"{title} {summary}")
        representative = representative_by_category.get(category)
        if representative is None:
            representative_by_category[category] = {
                "title": _clip(title, 56),
                "published_at": _format_news_time(str(row.get("published_at", "")).strip()),
                "source": _clip(source, 22),
                "publisher": _clip(publisher, 48),
                "url": url,
                "sentiment": sentiment,
            }
        else:
            previous_sentiment = str(representative.get("sentiment", "中性")).strip() or "中性"
            if sentiment_rank.get(sentiment, 1) > sentiment_rank.get(previous_sentiment, 1):
                representative["sentiment"] = sentiment
        if sentiment == "偏多":
            score += 8
        elif sentiment == "偏空":
            score -= 8
        if len(top_rows) < 5:
            top_rows.append(
                TopNewsItem(
                    title=_clip(title, 56),
                    published_at=_format_news_time(str(row.get("published_at", "")).strip()),
                    source=_clip(source, 22),
                    publisher=_clip(publisher, 48),
                    url=url,
                    impact=build_news_impact(category=category, sentiment=sentiment),
                    category=category,
                    sentiment=sentiment,
                )
            )

    category_order = {"财报": 0, "监管": 1, "产品": 2, "宏观": 3, "其他": 4}
    ranked_categories = sorted(
        ((name, int(count)) for name, count in event_distribution.items() if int(count) > 0),
        key=lambda item: (-item[1], category_order.get(item[0], 99)),
    )
    for category, count in ranked_categories[:3]:
        representative = representative_by_category.get(category, {})
        sentiment = str(representative.get("sentiment", "中性")).strip() or "中性"
        top_theme_rows.append(
            ThemeDigestItem(
                category=category,
                count=int(count),
                impact=build_news_impact(category=category, sentiment=sentiment),
                representative_title=str(representative.get("title", "（无标题）")).strip() or "（无标题）",
                representative_time=str(representative.get("published_at", "未知时间")).strip() or "未知时间",
                representative_source=str(representative.get("source", "未知来源")).strip() or "未知来源",
                representative_publisher=str(representative.get("publisher", "未知来源")).strip() or "未知来源",
                representative_url=str(representative.get("url", "")).strip(),
            )
        )

    score = max(0, min(100, score))
    direction = "中性"
    if score >= 58:
        direction = "偏多"
    elif score <= 42:
        direction = "偏空"
    score_low = max(0, score - 5)
    score_high = min(100, score + 5)
    return NewsDigest(
        window_days=max(1, int(window_days)),
        window_label=f"近{max(1, int(window_days))}天",
        total_count=total,
        source_coverage=source_coverage,
        event_distribution=event_distribution,
        sentiment_score=score,
        sentiment_direction=direction,
        sentiment_range=f"{score_low}-{score_high}",
        sentiment_method="lexicon",
        sentiment_sample_size=total,
        top_themes=top_theme_rows,
        top_news=top_rows,
    )


def build_news_digest_from_result(result: dict[str, Any], *, window_days: int = 7) -> NewsDigest:
    return build_news_digest(extract_news_items(result), window_days=window_days)


def format_top_news_lines(digest: NewsDigest) -> list[str]:
    if not digest.top_news:
        return ["暂无可展示新闻。"]
    lines: list[str] = []
    for index, item in enumerate(digest.top_news, start=1):
        lines.append(
            f"{index}. {item.title}\n"
            f"   时间：{item.published_at}｜媒体：{item.publisher}\n"
            f"   链接：{item.url or 'N/A'}\n"
            f"   影响：{item.impact}"
        )
    return lines


def format_cluster_lines(digest: NewsDigest) -> list[str]:
    total = max(1, int(digest.total_count))
    lines: list[str] = []
    for category in ("财报", "监管", "产品", "宏观", "其他"):
        count = int(digest.event_distribution.get(category, 0))
        ratio = round((count / total) * 100, 1)
        lines.append(f"- {category}: {count} 条 ({ratio}%)")
    return lines


def classify_event_category(text: str) -> str:
    lowered = str(text or "").lower()
    for category, keywords in _EVENT_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return category
    return "其他"


def classify_sentiment(text: str) -> str:
    lowered = str(text or "").lower()
    pos = sum(1 for key in _POSITIVE_KEYWORDS if key in lowered)
    neg = sum(1 for key in _NEGATIVE_KEYWORDS if key in lowered)
    if pos > neg:
        return "偏多"
    if neg > pos:
        return "偏空"
    return "中性"


def build_news_impact(*, category: str, sentiment: str) -> str:
    direction = {"偏多": "偏利多", "偏空": "偏利空", "中性": "中性影响"}.get(sentiment, "中性影响")
    focus = {
        "财报": "关注业绩兑现与预期差。",
        "监管": "关注监管节奏与合规风险。",
        "产品": "关注落地进度与商业化转化。",
        "宏观": "关注利率与流动性方向。",
        "其他": "关注后续增量信息确认。",
    }.get(category, "关注后续增量信息确认。")
    return f"{direction}，{focus}"


def _clip(value: str, limit: int) -> str:
    text = str(value or "").strip()
    if len(text) <= max(1, int(limit)):
        return text
    return text[: max(1, int(limit) - 1)] + "…"


def _is_forbidden_key(key: str) -> bool:
    lowered = str(key).lower()
    return any(token in lowered for token in FORBIDDEN_VISIBLE_KEYS)


def _news_sort_key(item: dict[str, Any]) -> tuple[int, str]:
    raw = str(item.get("published_at", "")).strip()
    if not raw:
        return (0, "")
    normalized = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        return (1, dt.isoformat())
    except ValueError:
        return (0, raw)


def _format_news_time(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return "未知时间"
    normalized = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        return dt.strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return value[:16]


def _source_coverage(items: list[dict[str, Any]]) -> list[str]:
    seen: list[str] = []
    for item in items:
        url = _resolve_news_url(item)
        source = _resolve_news_publisher(item, url=url)
        if source and source not in seen:
            seen.append(source)
    return seen[:6]


def _resolve_news_url(item: dict[str, Any]) -> str:
    for key in ("url", "link", "canonical_url"):
        value = str(item.get(key, "")).strip()
        if value:
            return value
    return ""


def _resolve_news_publisher(item: dict[str, Any], *, url: str) -> str:
    for key in ("publisher", "source", "media"):
        value = str(item.get(key, "")).strip()
        if value:
            return value
    if url:
        parsed = urlparse(url)
        host = parsed.netloc.strip().lower()
        if host.startswith("www."):
            host = host[4:]
        if host:
            return host
    return "未知来源"
