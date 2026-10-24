"""Fetch news for TICKERS and save raw JSONL to data/raw/YYYY/MM/DD/ (never overwritten)."""

import asyncio
import json
import logging
import sys
from datetime import timedelta
from pathlib import Path

import httpx

from scripts.config import TICKERS
from scripts.fetch_finnhub import FinnhubSource
from scripts.fetch_marketaux import MarketauxSource
from scripts.helpers import SourceError, utcnow

log = logging.getLogger(__name__)

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
SOURCES = {"finnhub": FinnhubSource, "marketaux": MarketauxSource}


async def fetch_all(sources, tickers, since, until):
    """Fetch every (source, ticker) pair at once; failed pairs give None."""

    async def one(source, ticker):
        try:
            articles = await source.fetch(ticker, since, until, client)
        except (SourceError, KeyError, TypeError, ValueError) as exc:
            log.error("%s [%s]: %s", source.name, ticker, exc)
            return source, None
        log.info("%s [%s]: %d articles", source.name, ticker, len(articles))
        return source, articles

    async with httpx.AsyncClient(timeout=30) as client:
        return await asyncio.gather(*(one(s, t) for s in sources for t in tickers))


def write_raw(by_source, run_at, root=RAW_DIR):
    """Write one JSONL file per source, keeping the untouched API payload."""
    day_dir = root / run_at.strftime("%Y/%m/%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, articles in by_source.items():
        if not articles:
            continue
        path = day_dir / f"{name}_{run_at:%H%M%S}.jsonl"
        with path.open("x") as f:  # "x": never overwrite raw data
            for a in articles:
                f.write(json.dumps({**a.to_dict(), "raw": dict(a.raw)}) + "\n")
        log.info("wrote %d articles to %s", len(articles), path)
        paths.append(str(path))
    return paths


def run(tickers=TICKERS, since=None, until=None, sources=SOURCES, root=RAW_DIR):
    """Airflow-callable entry point; returns the written file paths."""
    run_at = utcnow()
    until = until or run_at
    since = since or until - timedelta(days=1)
    clients = [SOURCES[name]() for name in sources]
    results = asyncio.run(fetch_all(clients, tickers, since, until))

    if all(articles is None for _, articles in results):
        raise RuntimeError("all fetch requests failed")

    by_source = {c.name: [] for c in clients}
    for source, articles in results:
        by_source[source.name] += articles or []
    return write_raw(by_source, run_at, root)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    for path in run(sys.argv[1:] or TICKERS):
        print(path)
