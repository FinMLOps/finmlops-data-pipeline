"""Unit tests for the ingestion runner and HTTP retries."""

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest
import respx

from scripts import ingest
from scripts.fetch_finnhub import FinnhubSource
from scripts.fetch_marketaux import MarketauxSource

# fixed time so the tests always give the same result
NOW = datetime(2026, 10, 12, 16, 0, 0, tzinfo=timezone.utc)
SINCE = NOW - timedelta(days=1)

# fake Finnhub response with one article from an hour ago
FINNHUB_OK = [
    {
        "headline": "A",
        "datetime": int((NOW - timedelta(hours=1)).timestamp()),
        "url": "https://a.com",
        "related": "AAPL",
    }
]


# runs before every test so the sources don't complain about missing API keys
@pytest.fixture(autouse=True)
def api_keys(monkeypatch):
    monkeypatch.setenv("FINNHUB_API_KEY", "mock_finnhub_key")
    monkeypatch.setenv("MARKETAUX_API_KEY", "mock_marketaux_key")


# small helper to read back the jsonl file that ingest wrote
def read_jsonl(path):
    with open(path) as f:
        return [json.loads(line) for line in f]


# respx.mock fakes the HTTP calls so no real API is hit (saves our quota)
@respx.mock
def test_run_writes_raw_payload_as_received(tmp_path):
    respx.get(FinnhubSource.URL).mock(return_value=httpx.Response(200, json=FINNHUB_OK))

    # tmp_path is a temp folder from pytest, so we don't write into the real data/raw
    (path,) = ingest.run(["AAPL", "MSFT"], SINCE, NOW, ["finnhub"], tmp_path)

    records = read_jsonl(path)
    assert len(records) == 2  # one per ticker; dedup happens in preprocessing
    assert records[0] == FINNHUB_OK[0]  # each line is exactly what the API sent


# if one source fails, we should still save the data from the other one
@respx.mock
def test_run_tolerates_partial_failure(tmp_path):
    respx.get(FinnhubSource.URL).mock(return_value=httpx.Response(200, json=FINNHUB_OK))
    respx.get(MarketauxSource.URL).mock(
        return_value=httpx.Response(401, text="bad key")
    )

    (path,) = ingest.run(["AAPL"], SINCE, NOW, root=tmp_path)

    assert "finnhub" in path  # only finnhub file gets written


# a missing API key should only skip that source, not the whole run
@respx.mock
def test_run_skips_source_with_missing_key(tmp_path, monkeypatch):
    monkeypatch.delenv("MARKETAUX_API_KEY")
    respx.get(FinnhubSource.URL).mock(return_value=httpx.Response(200, json=FINNHUB_OK))

    (path,) = ingest.run(["AAPL"], SINCE, NOW, root=tmp_path)

    assert "finnhub" in path  # finnhub still ran


# if everything fails, run() should raise so the Airflow task fails and alerts us
@respx.mock
def test_run_raises_when_every_request_fails(tmp_path):
    # finnhub sends a broken article (no headline/datetime), marketaux sends a bad key error
    respx.get(FinnhubSource.URL).mock(return_value=httpx.Response(200, json=[{}]))
    respx.get(MarketauxSource.URL).mock(
        return_value=httpx.Response(401, text="bad key")
    )

    with pytest.raises(RuntimeError):
        ingest.run(["AAPL"], SINCE, NOW, root=tmp_path)


# raw files go in YYYY/MM/DD folders and should never be overwritten
def test_write_raw_never_overwrites(tmp_path):
    item = FINNHUB_OK[0]

    (path,) = ingest.write_raw({"finnhub": [item]}, NOW, tmp_path)
    assert path == str(tmp_path / "2026/10/12/finnhub_160000_000000.jsonl")

    # writing again with the same time should error instead of replacing the file
    with pytest.raises(FileExistsError):
        ingest.write_raw({"finnhub": [item]}, NOW, tmp_path)


# 429 (rate limit) and 503 (server down) should be retried until it works
@respx.mock
def test_retries_server_errors(tmp_path):
    # side_effect gives a different response on each call: fail, fail, then success
    route = respx.get(FinnhubSource.URL).mock(
        side_effect=[
            httpx.Response(429),
            httpx.Response(503),
            httpx.Response(200, json=FINNHUB_OK),
        ]
    )

    ingest.run(["AAPL"], SINCE, NOW, ["finnhub"], tmp_path)

    assert route.call_count == 3  # 2 failures + 1 success


# 401 (bad key) won't fix itself, so there's no point retrying it
@respx.mock
def test_does_not_retry_client_errors(tmp_path):
    route = respx.get(FinnhubSource.URL).mock(return_value=httpx.Response(401))

    with pytest.raises(RuntimeError):
        ingest.run(["AAPL"], SINCE, NOW, ["finnhub"], tmp_path)

    assert route.call_count == 1  # only called once, no retries
