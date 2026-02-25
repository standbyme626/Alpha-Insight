"""Scraper node with Crawl4AI-first and aiohttp fallback."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass

import aiohttp

try:  # pragma: no cover - optional dependency
    from crawl4ai import AsyncWebCrawler
except Exception:  # pragma: no cover - optional dependency
    AsyncWebCrawler = None  # type: ignore[assignment]


@dataclass
class ScrapeOutput:
    ok: bool
    markdown: str
    source: str


def _to_markdown_like(text: str) -> str:
    lines = [line.strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    return "\n".join(lines)


def _clean_html(html: str) -> str:
    text = re.sub(r"<script[\\s\\S]*?</script>", "", html, flags=re.IGNORECASE)
    text = re.sub(r"<style[\\s\\S]*?</style>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "\n", text)
    return _to_markdown_like(text)


async def scrape_web(url: str) -> ScrapeOutput:
    print("[DEBUG] QuantNode scraper.scrape_web Start")
    if AsyncWebCrawler is not None:
        return await _scrape_with_crawl4ai(url)
    return await _scrape_with_http(url)


async def _scrape_with_crawl4ai(url: str) -> ScrapeOutput:
    print("[DEBUG] QuantNode scraper._scrape_with_crawl4ai Start")

    async def _run() -> ScrapeOutput:
        async with AsyncWebCrawler(verbose=False) as crawler:
            result = await crawler.arun(url=url)
            markdown = getattr(result, "markdown", "") or getattr(result, "text", "")
            if not markdown:
                html = getattr(result, "html", "")
                markdown = _clean_html(html)
            return ScrapeOutput(ok=bool(markdown.strip()), markdown=markdown.strip(), source="crawl4ai")

    return await asyncio.create_task(_run())


async def _scrape_with_http(url: str) -> ScrapeOutput:
    print("[DEBUG] QuantNode scraper._scrape_with_http Start")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=30) as response:
                html = await response.text()
                markdown = _clean_html(html)
                return ScrapeOutput(ok=bool(markdown.strip()), markdown=markdown, source="aiohttp")
    except Exception as exc:  # pragma: no cover - network/runtime variability
        return ScrapeOutput(ok=False, markdown=f"scrape failed: {exc}", source="aiohttp")
