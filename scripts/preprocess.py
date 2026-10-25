"""Map raw API items to one table and clean them into data/processed/news.parquet."""

import html
import json
import logging
import re
from pathlib import Path

import pandas as pd

from scripts.config import TICKERS
from scripts.helpers import make_id, parse_dt

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
OUT_PATH = ROOT / "data" / "processed" / "news.parquet"
COLUMNS = [
    "id",
    "source",
    "publisher",
    "title",
    "summary",
    "published",
    "url",
    "symbols",
    "language",
]
ALIASES = {"GOOG": "GOOGL"}  # Alphabet's two share classes count as one company


def make_row(source, publisher, title, summary, published, url, symbols, language="en"):
    """Build one common row; the id is stable across runs (source + url)."""
    published = parse_dt(published)  # unix seconds or ISO string -> UTC datetime
    key = url or f"{title}|{published.isoformat()}"
    values = [
        make_id(source, key),
        source,
        publisher,
        title,
        summary,
        published,
        url,
        symbols,
        language,
    ]
    return dict(zip(COLUMNS, values))


def finnhub_row(item):
    return make_row(
        "finnhub",
        publisher=item.get("source"),  # e.g. "Yahoo"
        title=item.get("headline"),
        summary=item.get("summary"),
        published=item["datetime"],
        url=item.get("url"),
        symbols=(item.get("related") or "").split(","),  # "related" can be "AAPL,MSFT"
    )


def marketaux_row(item):
    return make_row(
        "marketaux",
        publisher=item.get("source"),  # e.g. "cnbc.com"
        title=item.get("title"),
        summary=item.get("description") or item.get("snippet"),
        published=item["published_at"],
        url=item.get("url"),
        symbols=[e.get("symbol") for e in item.get("entities") or []],
        language=item.get("language", "en"),
    )


MAPPERS = {"finnhub": finnhub_row, "marketaux": marketaux_row}


def load_raw(raw_dir=RAW_DIR):
    """Read raw API items and map each source's format to common rows."""
    rows = []
    for path in sorted(raw_dir.rglob("*.jsonl")):
        source = path.name.split("_")[0]  # finnhub_..., marketaux_...
        with path.open() as f:
            for n, line in enumerate(f, 1):
                try:
                    item = json.loads(line)
                    item = item.get(
                        "raw", item
                    )  # files saved before this change wrap the item
                    rows.append(MAPPERS[source](item))
                except (AttributeError, KeyError, TypeError, ValueError) as exc:
                    log.warning("skipping bad record %s:%d: %r", path, n, exc)
    return rows


def clean_text(text):
    """Unescape HTML entities, drop tags, and collapse whitespace."""
    if not isinstance(text, str):
        return ""
    text = re.sub(r"<[^>]+>", " ", html.unescape(text))
    return " ".join(text.split())


def to_tickers(symbols):
    """Map raw symbols to tracked tickers: MSFT.MI -> MSFT, GOOG -> GOOGL, AVGO -> dropped."""
    out = set()
    for s in symbols if isinstance(symbols, list) else []:
        base = str(s).upper().split(".")[0]
        base = ALIASES.get(base, base)
        if base in TICKERS:
            out.add(base)
    return sorted(out)


def preprocess(rows):
    df = pd.DataFrame(rows, columns=COLUMNS)
    df["title"] = df["title"].map(clean_text)
    df["summary"] = df["summary"].map(clean_text)
    df["published"] = pd.to_datetime(df["published"], utc=True)
    df = df[(df["title"] != "") & (df["language"] == "en")]

    # one row per tracked ticker the article is about
    df["ticker"] = df["symbols"].map(to_tickers)
    df = df.explode("ticker").dropna(subset=["ticker"])

    # same headline for the same ticker (across sources or runs): keep the earliest
    df["headline_key"] = (
        df["title"].str.lower().str.replace(r"[^a-z0-9]", "", regex=True)
    )
    df = df.sort_values("published").drop_duplicates(["headline_key", "ticker"])

    df["text"] = df["title"].where(
        df["summary"] == "", df["title"] + "\n\n" + df["summary"]
    )
    cols = ["id", "source", "publisher", "ticker", "title", "summary", "text", "published", "url"]
    return df[cols].reset_index(drop=True)


def run(raw_dir=RAW_DIR, out_path=OUT_PATH):
    """Airflow-callable entry point; returns the Parquet path."""
    rows = load_raw(raw_dir)
    if not rows:
        raise RuntimeError(f"no raw records found in {raw_dir}")
    df = preprocess(rows)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    log.info("kept %d rows from %d raw records -> %s", len(df), len(rows), out_path)
    return str(out_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    print(run())
