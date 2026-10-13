"""Unit tests for Marketaux news source provider."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import httpx
import pytest
import respx

from scripts.fetch_marketaux import MarketauxSource
from scripts.helpers.base import SourceError

NOW = datetime(2026, 10, 12, 16, 0, 0, tzinfo=timezone.utc)
SINCE = NOW - timedelta(days=7)
UNTIL = NOW

IN = "2026-10-12T15:59:30Z"  # Inside window
OUT = "2026-01-01T10:00:00Z"  # Outside window (should be filtered out)


def run(source, query="TSLA"):
    """Helper runner that spins up an httpx client and executes fetch."""

    async def go():
        async with httpx.AsyncClient() as client:
            return await source.fetch(query, SINCE, UNTIL, client)

    return asyncio.run(go())


def test_marketaux_missing_key_raises_error(monkeypatch):
    monkeypatch.delenv("MARKETAUX_API_KEY", raising=False)
    with pytest.raises(SourceError, match="MARKETAUX_API_KEY"):
        MarketauxSource()


@respx.mock
def test_marketaux_fetch_success():
    respx.get("https://api.marketaux.com/v1/news/all").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {
                        "title": "Tesla robotaxi event announced",
                        "published_at": IN,
                        "url": "https://news.com/tsla-1",
                        "description": "Elon announces launch.",
                        "entities": [{"symbol": "TSLA"}],
                    },
                    {
                        "title": "Stale news",
                        "published_at": OUT,
                        "url": "https://news.com/tsla-old",
                        "entities": [{"symbol": "TSLA"}],
                    },
                ],
                "meta": {"returned": 1, "limit": 3},
            },
        )
    )
    out = run(MarketauxSource(api_key="mock_key"), query="TSLA")
    assert len(out) == 1
    assert out[0].title == "Tesla robotaxi event announced"
    assert out[0].source == "marketaux"
    assert "TSLA" in out[0].symbols


@respx.mock
def test_marketaux_pagination():
    # Page 1 returns full batch of 3 items, so fetch calls page 2
    respx.get(
        "https://api.marketaux.com/v1/news/all", params__contains={"page": "1"}
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {"title": "News 1", "published_at": IN, "url": "https://x/1"},
                    {"title": "News 2", "published_at": IN, "url": "https://x/2"},
                    {"title": "News 3", "published_at": IN, "url": "https://x/3"},
                ],
                "meta": {"returned": 3, "limit": 3},
            },
        )
    )
    # Page 2 returns 1 item, ending pagination
    respx.get(
        "https://api.marketaux.com/v1/news/all", params__contains={"page": "2"}
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {"title": "News 4", "published_at": IN, "url": "https://x/4"},
                ],
                "meta": {"returned": 1, "limit": 3},
            },
        )
    )
    out = run(MarketauxSource(api_key="mock_key", max_pages=2), query="TSLA")
    assert len(out) == 4
    assert [a.title for a in out] == ["News 1", "News 2", "News 3", "News 4"]


@respx.mock
def test_marketaux_http_error_becomes_source_error():
    respx.get("https://api.marketaux.com/v1/news/all").mock(
        return_value=httpx.Response(500, text="internal server error")
    )
    with pytest.raises(SourceError) as exc:
        run(MarketauxSource(api_key="mock_key"))
    assert exc.value.status == 500
    assert "internal server error" in str(exc.value)
