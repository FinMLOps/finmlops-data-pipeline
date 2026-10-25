"""Fetch news for TICKERS and save raw JSONL to data/raw/YYYY/MM/DD/ (never overwritten)."""

import argparse
import asyncio
import json
import logging
from datetime import timedelta
from pathlib import Path

import httpx

from scripts.config import TICKERS
from scripts.fetch_finnhub import FinnhubSource
from scripts.fetch_marketaux import MarketauxSource
from scripts.helpers import SourceError, parse_dt, utcnow

log = logging.getLogger(__name__)

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
SOURCES = {"finnhub": FinnhubSource, "marketaux": MarketauxSource}


async def fetch_all(sources, tickers, since, until):
    """Fetch every (source, ticker) pair at once; failed pairs give None."""

    async def one(source, ticker):
        try:
            items = await source.fetch(ticker, since, until, client)
        except SourceError as exc:
            log.error("%s [%s]", exc, ticker)  # exc already starts with the source name
            return source, None
        except (AttributeError, KeyError, TypeError, ValueError) as exc:
            log.error("%s [%s]: malformed response: %r", source.name, ticker, exc)
            return source, None
        log.info("%s [%s]: %d items", source.name, ticker, len(items))
        return source, items

    async with httpx.AsyncClient(timeout=30) as client:
        return await asyncio.gather(*(one(s, t) for s in sources for t in tickers))


def write_raw(by_source, run_at, root=RAW_DIR, day=None):
    """Write one JSONL file per source, each line an API item exactly as received.

    Files go in the folder for `day` (the start of the fetch window), so an
    11 PM-midnight run that finishes after midnight still lands in its own day.
    """
    day_dir = root / (day or run_at).strftime("%Y/%m/%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, items in by_source.items():
        if not items:
            continue
        path = day_dir / f"{name}_{run_at:%H%M%S_%f}.jsonl"
        with path.open("x") as f:  # "x": never overwrite raw data
            for item in items:
                f.write(json.dumps(item) + "\n")
        log.info("wrote %d items to %s", len(items), path)
        paths.append(str(path))
    return paths


def run(tickers=TICKERS, since=None, until=None, sources=SOURCES, root=RAW_DIR):
    """Airflow-callable entry point; returns the written file paths."""
    run_at = utcnow()
    day = since or run_at  # explicit window -> its start date; otherwise today
    until = until or run_at
    since = since or until - timedelta(days=1)
    clients = []
    for name in sources:
        try:
            clients.append(SOURCES[name]())
        except SourceError as exc:  # e.g. missing API key: skip this source only
            log.error("%s", exc)
    if not clients:
        raise RuntimeError("no sources could be set up (check API keys in .env)")
    results = asyncio.run(fetch_all(clients, tickers, since, until))

    if all(items is None for _, items in results):
        raise RuntimeError("all fetch requests failed")

    by_source = {c.name: [] for c in clients}
    for source, items in results:
        by_source[source.name] += items or []
    return write_raw(by_source, run_at, root, day=day)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Fetch raw news into data/raw/.")
    parser.add_argument("tickers", nargs="*", default=TICKERS)
    parser.add_argument(
        "--source", action="append", choices=list(SOURCES), help="only this source (repeatable)"
    )
    parser.add_argument(
        "--since", type=parse_dt, help="window start, ISO time (default: 24h before --until)"
    )
    parser.add_argument("--until", type=parse_dt, help="window end, ISO time (default: now)")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    for path in run(args.tickers, args.since, args.until, args.source or SOURCES):
        print(path)


if __name__ == "__main__":
    main()
