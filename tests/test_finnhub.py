"""Unit tests for Finnhub news source provider."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import httpx
import pytest
import respx

from scripts.fetch_finnhub import FinnhubSource
from scripts.helpers.base import SourceError

NOW = datetime(2026, 10, 12, 16, 0, 0, tzinfo=timezone.utc)
SINCE = NOW - timedelta(days=7)
UNTIL = NOW

IN = "2026-10-12T15:59:30Z"  # Inside window
OUT = "2026-01-01T10:00:00Z"  # Outside window (should be filtered out)


def run(source, query="AAPL"):
    """Helper runner that spins up an httpx client and executes fetch."""

    async def go():
        async with httpx.AsyncClient() as client:
            return await source.fetch(query, SINCE, UNTIL, client)

    return asyncio.run(go())


def test_finnhub_missing_key_raises_error(monkeypatch):
    monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
    with pytest.raises(SourceError, match="FINNHUB_API_KEY"):
        FinnhubSource()


@respx.mock
def test_finnhub_fetch_success():
    respx.get("https://finnhub.io/api/v1/company-news").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "datetime": 1791820770,  # Matches IN timestamp
                    "headline": "Apple Beats Q3",
                    "summary": "Revenue up 10%",
                    "url": "https://news.com/aapl",
                    "related": "AAPL",
                },
                {
                    "datetime": 1704067200,  # Matches OUT timestamp
                    "headline": "Old News",
                    "url": "https://news.com/old",
                },
            ],
        )
    )
    out = run(FinnhubSource(api_key="mock_key"), query="AAPL")
    assert len(out) == 1
    assert out[0].title == "Apple Beats Q3"
    assert out[0].symbols == ("AAPL",)
    assert out[0].source == "finnhub"
    assert out[0].url == "https://news.com/aapl"


@respx.mock
def test_finnhub_http_error_becomes_source_error():
    respx.get("https://finnhub.io/api/v1/company-news").mock(
        return_value=httpx.Response(429, text="rate limit exceeded")
    )
    with pytest.raises(SourceError) as exc:
        run(FinnhubSource(api_key="mock_key"))
    assert exc.value.status == 429
    assert "rate limit exceeded" in str(exc.value)


@respx.mock
def test_finnhub_invalid_json():
    respx.get("https://finnhub.io/api/v1/company-news").mock(
        return_value=httpx.Response(200, text="not valid json")
    )
    with pytest.raises(SourceError, match="invalid JSON"):
        run(FinnhubSource(api_key="mock_key"))
