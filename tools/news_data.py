"""Lightweight multi-source news fetcher for ticker symbols."""

from __future__ import annotations

import asyncio
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from functools import lru_cache
from typing import Any
from urllib.parse import quote_plus

import aiohttp
from core.connectors import BaseConnector, ConnectorError, ConnectorErrorCode, RetryPolicy, Throttler
from core.runtime_config import resolve_runtime_config
from core.tool_result import ToolResult, build_tool_result


def _clean_html_text(text: str) -> str:
    if not text:
        return ""
    cleaned = re.sub(r"<[^>]+>", " ", text)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _parse_pub_date(value: str) -> str:
    if not value:
        return ""
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            return dt.isoformat() + "Z"
        return dt.astimezone().isoformat()
    except Exception:
        return value


def _sort_key(value: dict[str, Any]) -> tuple[int, str]:
    raw = str(value.get("published_at", "")).strip()
    if not raw:
        return (1, "")
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return (0, dt.isoformat())
    except Exception:
        return (1, raw)


def _dedupe_news_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        link = str(row.get("link", "")).strip().lower()
        title = str(row.get("title", "")).strip().lower()
        key = link or title
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _estimate_news_confidence(rows: list[dict[str, Any]], feed_count: int) -> float:
    if not rows:
        return 0.0
    if feed_count <= 1:
        return 0.7
    if len(rows) >= 5:
        return 0.95
    if len(rows) >= 2:
        return 0.85
    return 0.75


class RSSNewsConnector(BaseConnector):
    def __init__(self, *, timeout_seconds: float, retry_policy: RetryPolicy, throttle_rate_per_sec: float):
        super().__init__(
            name="news_rss",
            timeout_seconds=timeout_seconds,
            retry_policy=retry_policy,
            throttler=Throttler(throttle_rate_per_sec),
        )

    async def fetch_feed(self, *, session: aiohttp.ClientSession, url: str) -> str:
        async def _request() -> str:
            async with session.get(url) as resp:
                status = int(resp.status)
                if status == 429:
                    raise ConnectorError(
                        code=ConnectorErrorCode.RATE_LIMIT,
                        message=f"news_rss rate limited ({status})",
                        retriable=True,
                        status_code=status,
                    )
                if status in {401, 403}:
                    raise ConnectorError(
                        code=ConnectorErrorCode.AUTH,
                        message=f"news_rss auth failed ({status})",
                        retriable=False,
                        status_code=status,
                    )
                if 500 <= status <= 599:
                    raise ConnectorError(
                        code=ConnectorErrorCode.UPSTREAM_5XX,
                        message=f"news_rss upstream error ({status})",
                        retriable=True,
                        status_code=status,
                    )
                if status >= 400:
                    raise ConnectorError(
                        code=ConnectorErrorCode.DATA_INVALID,
                        message=f"news_rss rejected request ({status})",
                        retriable=False,
                        status_code=status,
                    )
                return await resp.text()

        return await self.call("fetch_feed", _request)


@lru_cache(maxsize=1)
def _news_rss_connector() -> RSSNewsConnector:
    cfg = resolve_runtime_config().config.connectors.news_rss
    if not cfg.enabled:
        raise ConnectorError(
            code=ConnectorErrorCode.DATA_INVALID,
            message="news_rss connector disabled by config",
            retriable=False,
        )
    retry = cfg.retry
    return RSSNewsConnector(
        timeout_seconds=cfg.timeout_seconds,
        throttle_rate_per_sec=cfg.throttle_rate_per_sec,
        retry_policy=RetryPolicy(
            max_attempts=retry.max_attempts,
            base_backoff_seconds=retry.base_backoff_seconds,
            max_backoff_seconds=retry.max_backoff_seconds,
            jitter_seconds=retry.jitter_seconds,
        ),
    )


async def fetch_symbol_news_tool_result(
    symbol: str,
    *,
    limit: int = 10,
    timeout_seconds: int = 20,
    company_name: str = "",
) -> ToolResult:
    """
    Fetch symbol news from Yahoo Finance RSS.

    Returns newest-first list of:
    {title, link, published_at, summary, source}
    """
    normalized = symbol.strip().upper()
    if not normalized:
        return build_tool_result(
            source="news:rss",
            confidence=0.0,
            raw=[],
            error="symbol is empty",
            meta={"symbol": normalized, "limit": int(limit)},
        )

    request_limit = max(3, int(limit))
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    feeds: list[tuple[str, str]] = [
        (
            f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={normalized}&region=US&lang=en-US",
            "YahooFinanceRSS",
        )
    ]

    query_terms = [f"{normalized} stock"]
    if "." in normalized:
        base_symbol = normalized.split(".", 1)[0].strip().upper()
        if base_symbol and base_symbol != normalized:
            query_terms.append(f"{base_symbol} stock")
    normalized_company = str(company_name or "").strip()
    if normalized_company and normalized_company.upper() != normalized:
        query_terms.append(f"\"{normalized_company}\" stock")

    for idx, term in enumerate(dict.fromkeys(query_terms)):
        feeds.append(
            (
                f"https://news.google.com/rss/search?q={quote_plus(term)}&hl=en-US&gl=US&ceid=US:en",
                f"GoogleNewsRSS#{idx + 1}",
            )
        )

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            results = await asyncio.gather(
                *[
                    _fetch_rss(
                        session,
                        url,
                        source=source,
                        limit=request_limit,
                    )
                    for url, source in feeds
                ]
            )
    except Exception as exc:
        return build_tool_result(
            source="news:rss",
            confidence=0.0,
            raw=[],
            error=str(exc),
            meta={"symbol": normalized, "limit": int(limit), "feed_count": len(feeds)},
        )

    merged = _dedupe_news_rows([item for group in results for item in group])
    rows = sorted(merged, key=_sort_key, reverse=True)[: max(1, int(limit))]
    return build_tool_result(
        source="news:rss",
        confidence=_estimate_news_confidence(rows, feed_count=len(feeds)),
        raw=rows,
        error="",
        meta={"symbol": normalized, "limit": int(limit), "feed_count": len(feeds)},
    )


async def fetch_symbol_news(
    symbol: str,
    *,
    limit: int = 10,
    timeout_seconds: int = 20,
    company_name: str = "",
) -> list[dict[str, Any]]:
    """
    Legacy adapter: keep existing list return while internal pipeline uses ToolResult.

    Returns newest-first list of:
    {title, link, published_at, summary, source}
    """
    tool_result = await fetch_symbol_news_tool_result(
        symbol,
        limit=limit,
        timeout_seconds=timeout_seconds,
        company_name=company_name,
    )
    raw = tool_result.raw
    return raw if isinstance(raw, list) else []


async def _fetch_rss(
    session: aiohttp.ClientSession,
    url: str,
    *,
    source: str,
    limit: int,
) -> list[dict[str, Any]]:
    try:
        payload = await _news_rss_connector().fetch_feed(session=session, url=url)
    except ConnectorError:
        return []

    return _parse_rss_items(payload, source=source, limit=limit)


def _parse_rss_items(payload: str, *, source: str, limit: int) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(payload)
    except Exception:
        return []

    items = root.findall(".//item")
    rows: list[dict[str, Any]] = []
    for item in items[: max(1, limit)]:
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub_raw = (item.findtext("pubDate") or "").strip()
        desc_raw = (item.findtext("description") or "").strip()
        if not title:
            continue
        rows.append(
            {
                "title": title,
                "link": link,
                "published_at": _parse_pub_date(pub_raw),
                "summary": _clean_html_text(desc_raw),
                "source": source,
            }
        )

    return sorted(rows, key=_sort_key, reverse=True)
