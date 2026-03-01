from __future__ import annotations

import pytest

from tools.news_data import fetch_symbol_news


@pytest.mark.asyncio
async def test_fetch_symbol_news_merges_multi_sources_and_dedup(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str, int]] = []

    async def fake_fetch(_session, url: str, *, source: str, limit: int):  # noqa: ANN001
        calls.append((url, source, limit))
        if source == "YahooFinanceRSS":
            return [
                {
                    "title": "A",
                    "link": "https://example.com/a",
                    "published_at": "2026-02-27T10:00:00+00:00",
                    "summary": "",
                    "source": source,
                }
            ]
        if source == "GoogleNewsRSS#1":
            return [
                {
                    "title": "A-dup",
                    "link": "https://example.com/a",
                    "published_at": "2026-02-27T09:00:00+00:00",
                    "summary": "",
                    "source": source,
                },
                {
                    "title": "B",
                    "link": "https://example.com/b",
                    "published_at": "2026-02-27T11:00:00+00:00",
                    "summary": "",
                    "source": source,
                },
            ]
        if source == "GoogleNewsRSS#2":
            return [
                {
                    "title": "C",
                    "link": "https://example.com/c",
                    "published_at": "2026-02-27T08:00:00+00:00",
                    "summary": "",
                    "source": source,
                }
            ]
        return []

    monkeypatch.setattr("tools.news_data._fetch_rss", fake_fetch)
    rows = await fetch_symbol_news("0700.HK", limit=3, company_name="Tencent")

    assert len(rows) == 3
    assert [str(item["link"]) for item in rows] == [
        "https://example.com/b",
        "https://example.com/a",
        "https://example.com/c",
    ]
    assert any(source == "GoogleNewsRSS#2" for _, source, _ in calls)

