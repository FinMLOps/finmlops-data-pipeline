"""Unit tests for TFDV-based raw data validation (raw API items as received)."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pytest
import tensorflow_data_validation as tfdv

from scripts.config import TICKERS
from scripts.helpers import parse_dt
from scripts.preprocess import finnhub_row, marketaux_row
from scripts.tfdv_validate import (
    DataValidationError,
    alert_text,
    coverage,
    coverage_anomalies,
    infer_schema,
    load_day,
    run,
    to_frame,
    to_row,
    validate,
)

DAY = date(2026, 10, 20)
MIDNIGHT = int(datetime(2026, 10, 20, tzinfo=timezone.utc).timestamp())


def finnhub_item(i=0):
    """A raw Finnhub item, as the API sends it (and ingest.py saves it)."""
    return {
        "category": "company",
        "datetime": MIDNIGHT + (i % 24) * 3600 + 900,  # hh:15 on DAY
        "headline": f"Headline number {i} about earnings",
        "id": i,
        "related": TICKERS[i % len(TICKERS)],
        "source": "Reuters" if i % 3 else "Yahoo",
        "summary": "Revenue grew but missed estimates." * (1 + i % 3),
        "url": f"https://news.example.com/{i}",
    }


def marketaux_item(i=0):
    """A raw Marketaux item, as the API sends it."""
    return {
        "uuid": f"u{i}",
        "title": f"Headline number {i} about earnings",
        "description": "Revenue grew but missed estimates." * (1 + i % 3),
        "url": f"https://news.example.com/{i}",
        "language": "en",
        "published_at": f"2026-10-20T{i % 24:02d}:15:00.000000Z",
        "source": "reuters.com" if i % 3 else "yahoo.com",
        "entities": [{"symbol": TICKERS[i % len(TICKERS)]}],
    }


def item(i):
    """Odd i -> finnhub item, even i -> marketaux item."""
    return finnhub_item(i) if i % 2 else marketaux_item(i)


def row(i=0, **overrides):
    """The common row preprocess.load_raw builds from item(i), with fields overridden."""
    mapped = finnhub_row(item(i)) if i % 2 else marketaux_row(item(i))
    mapped.update(overrides)
    return mapped


def good_batch(n=40):
    return [row(i) for i in range(n)]


def good_items(n=40):
    return [(i, item(i)) for i in range(n)]


def write_day(root, day, items, suffix="120000"):
    """Write (i, raw item) pairs into finnhub_/marketaux_ files the way ingest.py does."""
    day_dir = root / day.strftime("%Y/%m/%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    files = {}
    for i, it in items:
        files.setdefault("finnhub" if i % 2 else "marketaux", []).append(json.dumps(it))
    paths = []
    for name, lines in files.items():
        path = day_dir / f"{name}_{suffix}.jsonl"
        path.write_text("\n".join(lines) + "\n")
        paths.append(path)
    return paths


def frame(rows):
    return to_frame(rows, DAY)


@pytest.fixture(scope="module")
def schema():
    return infer_schema(tfdv.generate_statistics_from_dataframe(frame(good_batch())))


def test_to_row_good_record():
    r = to_row(row(3), DAY)
    assert r["source"] == "finnhub"
    assert r["symbol"] == TICKERS[3]
    assert r["n_symbols"] == 1
    assert r["title_len"] == len("Headline number 3 about earnings")
    assert r["has_url"] == 1
    assert r["publisher"] == "yahoo"
    assert r["published_hour"] == 3
    assert r["age_days"] == 0


def test_to_row_age_needs_day():
    assert to_row(row(3))["age_days"] is None
    assert to_row(row(published=parse_dt("2026-10-10T00:00:00Z")), DAY)["age_days"] == 10


def test_to_row_prefers_tracked_ticker():
    r = to_row(row(symbols=["XYZ", "MSFT"]))
    assert r["symbol"] == "MSFT"
    assert r["n_symbols"] == 2


def test_to_row_standardizes_ticker():
    assert to_row(row(symbols=["MSFT.MI"]))["symbol"] == "MSFT"  # same mapping as preprocess


def test_to_row_bad_values_become_missing():
    r = to_row(
        row(title="   ", symbols="AAPL", published="2026-10-20", source=7, url=None, publisher=None)
    )
    assert r["title_len"] is None  # empty text
    assert r["symbol"] is None and r["n_symbols"] is None  # wrong type
    assert r["published_hour"] is None  # wrong type
    assert r["source"] is None
    assert r["has_url"] == 0
    assert r["publisher"] is None


def test_to_row_empty_finnhub_related():
    r = to_row(finnhub_row({**finnhub_item(1), "related": ""}))
    assert r["symbol"] is None and r["n_symbols"] == 0


def test_to_row_falls_back_to_url_host():
    r = to_row(row(publisher=None, url="https://www.cnbc.com/a"))
    assert r["publisher"] == "cnbc.com"


def test_to_frame_keeps_int_dtype_with_missing():
    df = to_frame([row(0), row(1, title="")])
    assert str(df["title_len"].dtype) == "Int64"
    assert df["title_len"].isna().sum() == 1


def test_load_day_counts_unreadable_lines(tmp_path):
    no_time = {k: v for k, v in finnhub_item(1).items() if k != "datetime"}
    lines = [json.dumps(finnhub_item(1)), "{truncated", "", "[1, 2]", json.dumps(no_time)]
    (tmp_path / "finnhub_1.jsonl").write_text("\n".join(lines) + "\n")
    (tmp_path / "scraper_1.jsonl").write_text(json.dumps(finnhub_item(3)) + "\n")  # unknown source

    rows, corrupt = load_day(tmp_path)
    assert len(rows) == 1
    assert corrupt == 4  # bad json, json list, missing timestamp, unknown source


def test_schema_pins_domains(schema):
    features = {f.name: f for f in schema.feature}
    assert set(features["source"].string_domain.value) == {"finnhub", "marketaux"}
    assert "AAPL" in features["symbol"].string_domain.value
    assert not features["publisher"].HasField("domain")
    assert features["published_hour"].int_domain.max == 23
    assert schema.dataset_constraints.num_examples_drift_comparator.min_fraction_threshold == 0.5
    longest = max(len(r["title"]) for r in good_batch())
    assert features["title_len"].int_domain.max == longest * 3
    assert (features["age_days"].int_domain.min, features["age_days"].int_domain.max) == (0, 7)


def test_clean_batch_has_no_anomalies(schema):
    _, anomalies = validate(frame([row(i) for i in range(5, 45)]), schema)
    assert not anomalies.anomaly_info


def test_corrupted_records_are_flagged(schema):
    """Acceptance criterion: missing tickers, empty text, and wrong types are caught."""
    batch = good_batch(30) + [
        row(100, symbols=[]),  # missing ticker
        row(101, title=""),  # empty text
        row(102, published=12345, symbols="AAPL"),  # wrong types
    ]
    _, anomalies = validate(frame(batch), schema)
    assert {"symbol", "title_len", "published_hour"} <= set(anomalies.anomaly_info)


def test_unknown_ticker_and_source_flagged(schema):
    batch = good_batch(30) + [row(200, symbols=["ZZZZ"], source="scraper")]
    _, anomalies = validate(frame(batch), schema)
    assert {"symbol", "source"} <= set(anomalies.anomaly_info)


def test_outlier_lengths_flagged(schema):
    batch = good_batch(30) + [row(300, title="x" * 5000, summary="y" * 100_000)]
    _, anomalies = validate(frame(batch), schema)
    assert {"title_len", "summary_len"} <= set(anomalies.anomaly_info)


def test_stale_and_future_articles_flagged(schema):
    stale = good_batch(30) + [row(400, published=parse_dt("2026-09-01T12:00:00Z"))]
    _, anomalies = validate(frame(stale), schema)
    assert "age_days" in anomalies.anomaly_info

    future = good_batch(30) + [row(401, published=parse_dt("2026-10-25T12:00:00Z"))]
    _, anomalies = validate(frame(future), schema)
    assert "age_days" in anomalies.anomaly_info


def test_volume_drop_flagged(schema):
    previous, _ = validate(frame(good_batch(40)), schema)
    _, anomalies = validate(frame(good_batch(10)), schema, previous)
    assert anomalies.dataset_anomaly_info.reason


def test_symbol_drift_flagged(schema):
    previous, _ = validate(frame(good_batch(40)), schema)
    skewed = [row(i, symbols=["AAPL"]) for i in range(40)]
    _, anomalies = validate(frame(skewed), schema, previous)
    assert "symbol" in anomalies.anomaly_info


def test_run_missing_day_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        run(DAY, raw_root=tmp_path / "raw", out_root=tmp_path / "val")


def test_run_bootstraps_schema_then_catches_bad_day(tmp_path):
    raw, out, schema_path = tmp_path / "raw", tmp_path / "val", tmp_path / "schema.pbtxt"
    write_day(raw, DAY, good_items())

    report = run(DAY, raw_root=raw, out_root=out, schema_path=schema_path)
    assert report["n_records"] == 40 and report["anomalies"] == {}
    assert schema_path.exists()
    day_out = out / "2026/10/20"
    assert {p.name for p in day_out.iterdir()} == {
        "stats.pbtxt",
        "stats.html",
        "anomalies.pbtxt",
        "report.json",
    }
    assert "facets" in (day_out / "stats.html").read_text()

    bad_day = date(2026, 10, 21)
    empty_title = {**finnhub_item(99), "headline": ""}
    paths = write_day(raw, bad_day, good_items(30) + [(99, empty_title)])
    with paths[0].open("a") as f:
        f.write("{not json\n")
    with pytest.raises(DataValidationError) as exc:
        run(bad_day, raw_root=raw, out_root=out, schema_path=schema_path)
    assert "title_len" in exc.value.report["anomalies"]
    assert exc.value.report["corrupt_lines"] == 1
    saved = json.loads((out / "2026/10/21/report.json").read_text())
    assert saved == exc.value.report


def test_run_all_lines_corrupt(tmp_path):
    day_dir = tmp_path / "raw" / DAY.strftime("%Y/%m/%d")
    day_dir.mkdir(parents=True)
    (day_dir / "finnhub_120000.jsonl").write_text("garbage\n")
    with pytest.raises(DataValidationError) as exc:
        run(DAY, raw_root=tmp_path / "raw", out_root=tmp_path / "val", schema_path=tmp_path / "s.pbtxt")
    assert exc.value.report["n_records"] == 0


def test_alert_text_lists_anomalies():
    report = {
        "day": "2026-10-21",
        "files": ["a.jsonl"],
        "n_records": 31,
        "corrupt_lines": 1,
        "anomalies": {"title_len": ["Column dropped"]},
    }
    text = alert_text(report)
    assert "2026-10-21" in text
    assert "- title_len: Column dropped" in text
    assert "corrupt lines: 1" in text


def test_coverage_counts_sources_tickers_and_duplicates():
    counts = coverage(good_batch(14))
    assert counts["source"] == {"marketaux": 7, "finnhub": 7}
    assert set(counts["ticker"]) == set(TICKERS)
    assert counts["duplicate_rate"] == {"marketaux": 0.0, "finnhub": 0.0}


def test_coverage_flags_missing_source_and_ticker():
    rows = [r for r in good_batch(28) if r["source"] == "finnhub" and r["symbols"] != ["AMZN"]]
    problems = coverage_anomalies(coverage(rows))
    assert "no articles from marketaux" in problems
    assert "no articles for AMZN" in problems


def test_coverage_flags_one_source_halving():
    # total barely moves (finnhub dominates), but marketaux drops from 20 to 5
    previous = {"source": {"finnhub": 200, "marketaux": 20}}
    counts = {"source": {"finnhub": 210, "marketaux": 5}, "ticker": dict.fromkeys(TICKERS, 1), "duplicate_rate": {}}
    assert coverage_anomalies(counts, previous) == ["marketaux: 5 articles vs 20 on the previous day"]


def test_coverage_flags_duplicate_spike():
    rows = [row(i, url="https://same.example/x") for i in range(14)]
    problems = coverage_anomalies(coverage(rows))
    assert any("repeat a URL" in p for p in problems)


def test_non_english_article_flagged(schema):
    batch = good_batch(30) + [row(500, language="de")]
    _, anomalies = validate(frame(batch), schema)
    assert "language" in anomalies.anomaly_info


def test_volume_spike_flagged(schema):
    previous, _ = validate(frame(good_batch(10)), schema)
    _, anomalies = validate(frame(good_batch(40)), schema, previous)
    assert anomalies.dataset_anomaly_info.reason  # 4x more articles than the previous day
