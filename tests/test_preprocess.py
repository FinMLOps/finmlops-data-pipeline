"""Unit tests for the preprocessing step."""

import json

import pandas as pd
import pytest

from scripts import preprocess

# 2026-10-24 15:00 UTC as unix seconds (finnhub sends timestamps like this)
TS = 1792854000


# fake raw finnhub item, shaped like the real API output
def finnhub_item(headline="Apple beats earnings", related="AAPL", datetime=TS, **extra):
    return {
        "headline": headline,
        "datetime": datetime,
        "related": related,
        "summary": "summary",
        "url": f"https://x.com/{headline}",
        **extra,
    }


# fake raw marketaux item, shaped like the real API output
def marketaux_item(title="Nvidia rallies", symbols=("NVDA",), language="en"):
    return {
        "title": title,
        "published_at": "2026-10-24T15:00:00.000000Z",
        "description": "desc",
        "url": f"https://x.com/{title}",
        "language": language,
        "entities": [{"symbol": s} for s in symbols],
    }


# writes raw items to a jsonl file the same way ingest.py does
def write_raw(raw_dir, name, items):
    raw_dir.mkdir(parents=True, exist_ok=True)
    lines = [i if isinstance(i, str) else json.dumps(i) for i in items]
    (raw_dir / name).write_text("\n".join(lines) + "\n")


# runs the mapping + cleaning on a list of finnhub items
def process(*items):
    return preprocess.preprocess([preprocess.finnhub_row(i) for i in items])


# finnhub fields get mapped to our common columns
def test_finnhub_row_maps_fields():
    row = preprocess.finnhub_row(finnhub_item(related="AAPL,MSFT"))

    assert row["title"] == "Apple beats earnings"
    assert row["symbols"] == ["AAPL", "MSFT"]  # comma separated "related" is split
    assert str(row["published"]) == "2026-10-24 15:00:00+00:00"  # unix seconds -> UTC


# marketaux fields get mapped too (different names than finnhub)
def test_marketaux_row_maps_fields():
    row = preprocess.marketaux_row(
        marketaux_item(symbols=["NVDA", "AVGO"], language="es")
    )

    assert row["title"] == "Nvidia rallies"
    assert row["summary"] == "desc"
    assert row["symbols"] == ["NVDA", "AVGO"]
    assert row["language"] == "es"


# html leftovers and extra spaces should be cleaned out of the text
def test_clean_text_removes_html_and_whitespace():
    assert (
        preprocess.clean_text("Apple &amp; <b>Nvidia</b>\n  rally")
        == "Apple & Nvidia rally"
    )
    assert preprocess.clean_text(None) == ""  # missing summary becomes empty string


# foreign listings and GOOG map to our tickers, untracked ones get dropped
def test_to_tickers_standardizes_symbols():
    assert preprocess.to_tickers(["MSFT.MI", "GOOG", "AVGO", "msft"]) == [
        "GOOGL",
        "MSFT",
    ]
    assert preprocess.to_tickers(None) == []


# empty titles, non-english text and untracked tickers are dropped
def test_preprocess_drops_bad_records():
    rows = [
        preprocess.finnhub_row(finnhub_item()),
        preprocess.finnhub_row(finnhub_item(headline="   ")),
        preprocess.finnhub_row(finnhub_item(headline="Only untracked", related="AVGO")),
        preprocess.marketaux_row(marketaux_item(title="Noticias", language="es")),
    ]

    df = preprocess.preprocess(rows)

    assert df["title"].tolist() == ["Apple beats earnings"]
    assert str(df["published"].dt.tz) == "UTC"


# an article about two of our tickers turns into two rows (one per ticker)
def test_preprocess_one_row_per_ticker():
    df = preprocess.preprocess(
        [preprocess.marketaux_row(marketaux_item(symbols=["AAPL", "NVDA", "JNJ"]))]
    )

    assert sorted(df["ticker"]) == ["AAPL", "NVDA"]


# same headline for the same ticker should only be kept once (the earliest one)
def test_preprocess_keeps_earliest_duplicate():
    df = process(
        finnhub_item(headline="Apple Beats Earnings!", datetime=TS + 3600),
        finnhub_item(headline="apple beats earnings", datetime=TS),
    )

    assert len(df) == 1
    assert df["published"][0] == pd.Timestamp(TS, unit="s", tz="UTC")


# text column is what FinBERT reads: title + summary (or just title if no summary)
def test_preprocess_builds_text_column():
    df = process(finnhub_item(summary=None))

    assert df["text"][0] == "Apple beats earnings"


# bad json, missing timestamps and unknown sources are skipped, not crashed on
def test_load_raw_skips_bad_records(tmp_path):
    no_time = {k: v for k, v in finnhub_item().items() if k != "datetime"}
    write_raw(tmp_path, "finnhub_1.jsonl", [finnhub_item(), "{not json", no_time])
    write_raw(tmp_path, "unknown_1.jsonl", [finnhub_item()])

    assert len(preprocess.load_raw(tmp_path)) == 1


# files saved by the old ingest.py wrap the api item under "raw"; they still load
def test_load_raw_reads_old_wrapped_format(tmp_path):
    write_raw(
        tmp_path,
        "finnhub_1.jsonl",
        [{"id": "x", "title": "old", "raw": finnhub_item()}],
    )

    (row,) = preprocess.load_raw(tmp_path)
    assert row["title"] == "Apple beats earnings"


# run() reads both sources and writes a parquet file
def test_run_writes_parquet(tmp_path):
    raw_dir = tmp_path / "raw" / "2026" / "10" / "24"
    write_raw(raw_dir, "finnhub_1.jsonl", [finnhub_item()])
    write_raw(raw_dir, "marketaux_1.jsonl", [marketaux_item()])

    out = preprocess.run(tmp_path / "raw", tmp_path / "news.parquet")

    assert sorted(pd.read_parquet(out)["source"]) == ["finnhub", "marketaux"]


# no raw data at all should fail loudly so the Airflow task fails
def test_run_raises_when_no_raw_data(tmp_path):
    with pytest.raises(RuntimeError):
        preprocess.run(tmp_path, tmp_path / "news.parquet")
