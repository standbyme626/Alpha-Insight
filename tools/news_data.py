"""Lightweight news fetcher for ticker symbols (Yahoo Finance RSS)."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote_plus

import aiohttp


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


async def fetch_symbol_news(symbol: str, *, limit: int = 10, timeout_seconds: int = 20) -> list[dict[str, Any]]:
    """
    Fetch symbol news from Yahoo Finance RSS.

    Returns newest-first list of:
    {title, link, published_at, summary, source}
    """
    normalized = symbol.strip().upper()
    if not normalized:
        return []

    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        yahoo_items = await _fetch_rss(
            session,
            f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={normalized}&region=US&lang=en-US",
            source="YahooFinanceRSS",
            limit=limit,
        )
        if yahoo_items:
            return yahoo_items

        query = quote_plus(f"{normalized} stock")
        google_items = await _fetch_rss(
            session,
            f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en",
            source="GoogleNewsRSS",
            limit=limit,
        )
        return google_items


async def _fetch_rss(
    session: aiohttp.ClientSession,
    url: str,
    *,
    source: str,
    limit: int,
) -> list[dict[str, Any]]:
    try:
        async with session.get(url) as resp:
            if resp.status >= 400:
                return []
            payload = await resp.text()
    except Exception:
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

    # Keep deterministic ordering when timestamp parsing fails.
    def _sort_key(x: dict[str, Any]) -> tuple[int, str]:
        raw = str(x.get("published_at", ""))
        if not raw:
            return (1, "")
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return (0, dt.isoformat())
        except Exception:
            return (1, raw)

    return sorted(rows, key=_sort_key, reverse=True)
