"""Validate a day of raw news JSONL with TensorFlow Data Validation (TFDV).

Maps data/raw/YYYY/MM/DD/*.jsonl (API items as received) to common rows with
preprocess.load_raw, flattens them into one row of features per article, generates
statistics, and checks them against a frozen schema (data/schema/schema.pbtxt)
and the previous day's statistics (drift and volume drop). Outputs go to
data/validation/YYYY/MM/DD/. Any anomaly raises DataValidationError so the
Airflow task fails; the CLI also sends an email alert.
"""

import argparse
import json
import logging
import math
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import tensorflow_data_validation as tfdv
from tensorflow_metadata.proto.v0 import schema_pb2

from scripts import preprocess
from scripts.email_alerts import send_alert
from scripts.config import TICKERS
from scripts.helpers import utcnow
from scripts.ingest import RAW_DIR, SOURCES

log = logging.getLogger(__name__)

DATA_DIR = RAW_DIR.parent
SCHEMA_PATH = DATA_DIR / "schema" / "schema.pbtxt"
VALIDATION_DIR = DATA_DIR / "validation"

REQUIRED = ("source", "symbol", "title_len", "published_hour")
INT_FEATURES = (
    "title_len",
    "summary_len",
    "n_symbols",
    "has_url",
    "published_hour",
    "age_days",
)
CATEGORICAL_DRIFT = 0.3  # max L-infinity distance between day-over-day distributions
NUMERIC_DRIFT = 0.3  # max Jensen-Shannon divergence for numeric features
MIN_VOLUME_FRACTION = 0.5  # alert when articles drop by more than 50% vs previous day
OUTLIER_FACTOR = 3  # length/count outlier: more than 3x the largest baseline value
MAX_AGE_DAYS = 7  # stale: published more than a week before the batch day


class DataValidationError(RuntimeError):
    """Raised when a batch has anomalies; carries the report for logging/alerts."""

    def __init__(self, report):
        self.report = report
        names = ", ".join(report["anomalies"]) or "unreadable data"
        super().__init__(f"data anomalies on {report['day']}: {names}")


def load_day(day_dir):
    """Map a day's raw API items to common rows; count lines that couldn't be read or mapped."""
    rows = preprocess.load_raw(day_dir)  # logs and skips bad json, missing timestamps, unknown sources
    lines = 0
    for path in day_dir.glob("*.jsonl"):
        with open(path) as f:
            lines += sum(1 for line in f if line.strip())
    return rows, lines - len(rows)


def _text_len(value):
    """Length of non-empty text; None for empty or non-string values."""
    return len(value.strip()) if isinstance(value, str) and value.strip() else None


def _publisher(row):
    name = row.get("publisher")
    if isinstance(name, str) and name.strip():
        return name.strip().lower()
    url = row.get("url")
    host = urlparse(url).netloc.lower() if isinstance(url, str) else ""
    return host.removeprefix("www.") or None


def to_row(row, day=None):
    """Flatten one common row (from preprocess) into scalar features; bad values become None.

    `day` is the batch date; it gives `age_days` (days between publication and batch).
    """
    symbols = row.get("symbols")
    if not isinstance(symbols, list) or not all(isinstance(s, str) for s in symbols):
        symbols = None
    else:
        symbols = [s for s in symbols if s]  # finnhub's "related" can be empty
    tracked = preprocess.to_tickers(symbols)  # MSFT.MI -> MSFT, GOOG -> GOOGL

    published = row.get("published")
    if not isinstance(published, datetime):
        published = None

    source = row.get("source")
    url = row.get("url")
    return {
        "source": source if isinstance(source, str) and source else None,
        "symbol": (tracked or symbols or [None])[0],
        "n_symbols": len(symbols) if symbols is not None else None,
        "title_len": _text_len(row.get("title")),
        "summary_len": _text_len(row.get("summary")),
        "has_url": int(isinstance(url, str) and bool(url)),
        "publisher": _publisher(row),
        "published_hour": published.hour if published else None,
        "age_days": (day - published.date()).days if published and day else None,
    }


def to_frame(records, day=None):
    df = pd.DataFrame([to_row(r, day) for r in records])
    for col in INT_FEATURES:
        df[col] = df[col].astype("Int64")  # nullable ints keep INT type with missing values
    return df


def infer_schema(stats):
    """Infer a schema from baseline stats, then pin the domain rules we care about."""
    schema = tfdv.infer_schema(stats, infer_feature_shape=False)

    for name in REQUIRED:
        presence = tfdv.get_feature(schema, name).presence
        presence.min_fraction = 1.0
        presence.min_count = 1

    tfdv.set_domain(schema, "source", _string_domain("source", SOURCES))
    tfdv.set_domain(schema, "symbol", _string_domain("symbol", TICKERS))
    tfdv.set_domain(schema, "published_hour", _int_domain(0, 23))
    tfdv.set_domain(schema, "has_url", _int_domain(0, 1))
    if _has_feature(schema, "age_days"):
        tfdv.set_domain(schema, "age_days", _int_domain(0, MAX_AGE_DAYS))  # future or stale

    # outliers: anything far beyond the largest value seen in the baseline
    for name, lo in (("title_len", 1), ("summary_len", 1), ("n_symbols", 0)):
        if _has_feature(schema, name):
            top = _baseline_max(stats, name)
            tfdv.set_domain(schema, name, _int_domain(lo, math.ceil(top * OUTLIER_FACTOR)))

    publisher = tfdv.get_feature(schema, "publisher")  # open set: never pin a domain
    publisher.ClearField("domain")
    publisher.presence.min_fraction = 0.0
    # drop the top-level domains infer_schema made for features we replaced above
    kept = [d for d in schema.string_domain if d.name not in ("source", "symbol", "publisher")]
    del schema.string_domain[:]
    schema.string_domain.extend(kept)

    for name in ("source", "symbol"):
        tfdv.get_feature(schema, name).drift_comparator.infinity_norm.threshold = (
            CATEGORICAL_DRIFT
        )
    for name in ("title_len", "summary_len"):
        if not _has_feature(schema, name):
            continue  # e.g. baseline had no summaries at all
        comparator = tfdv.get_feature(schema, name).drift_comparator
        comparator.jensen_shannon_divergence.threshold = NUMERIC_DRIFT

    volume = schema.dataset_constraints.num_examples_drift_comparator
    volume.min_fraction_threshold = MIN_VOLUME_FRACTION
    return schema


def _has_feature(schema, name):
    return any(f.name == name for f in schema.feature)


def _baseline_max(stats, name):
    feature = tfdv.get_feature_stats(stats.datasets[0], tfdv.FeaturePath([name]))
    return max(feature.num_stats.max, 1)


def _string_domain(name, values):
    return schema_pb2.StringDomain(name=name, value=sorted(values))


def _int_domain(lo, hi):
    domain = schema_pb2.IntDomain(min=lo)
    if hi is not None:
        domain.max = hi
    return domain


def summarize(anomalies):
    """Turn a TFDV Anomalies proto into {feature: [reasons]}."""
    out = {
        name: [r.description or r.short_description for r in info.reason]
        for name, info in anomalies.anomaly_info.items()
    }
    dataset = anomalies.dataset_anomaly_info
    if dataset.reason:
        out["__dataset__"] = [r.description for r in dataset.reason]
    return out


def validate(df, schema, previous=None):
    """Return (stats, anomalies) for a flattened batch."""
    stats = tfdv.generate_statistics_from_dataframe(df)
    anomalies = tfdv.validate_statistics(stats, schema, previous_statistics=previous)
    return stats, anomalies


def write_html(stats, previous, path, day):
    """Facets overview of today's stats (side by side with the previous day, if any)."""
    from tensorflow_data_validation.utils.display_util import get_statistics_html

    if previous is not None:
        body = get_statistics_html(stats, previous, lhs_name=str(day), rhs_name="previous day")
    else:
        body = get_statistics_html(stats, lhs_name=str(day))
    path.write_text(
        f"<!doctype html><html><head><meta charset='utf-8'>"
        f"<title>TFDV stats {day}</title></head><body>{body}</body></html>"
    )


def alert_text(report):
    """Plain-text alert body from a validation report."""
    lines = [f"Data validation failed for {report['day']}."]
    lines.append(f"Records: {report['n_records']}, corrupt lines: {report['corrupt_lines']}")
    for name, reasons in report["anomalies"].items():
        lines.append(f"- {name}: {'; '.join(reasons)}")
    lines.append("Files: " + ", ".join(report["files"]))
    return "\n".join(lines)


def _day_dir(root, day):
    return root / day.strftime("%Y/%m/%d")


def previous_stats(day, root=VALIDATION_DIR):
    """Stats from the latest validated day before `day`, if any."""
    earlier = []
    for path in root.glob("*/*/*/stats.pbtxt"):
        y, m, d = path.parent.relative_to(root).parts
        if (past := date(int(y), int(m), int(d))) < day:
            earlier.append((past, path))
    if not earlier:
        return None
    return tfdv.load_stats_text(str(max(earlier)[1]))


def run(
    day=None,
    raw_root=RAW_DIR,
    out_root=VALIDATION_DIR,
    schema_path=SCHEMA_PATH,
    refresh_schema=False,
):
    """Airflow-callable entry point; returns the report or raises DataValidationError."""
    day = day or utcnow().date()
    if isinstance(day, str):
        day = date.fromisoformat(day)
    elif isinstance(day, datetime):
        day = day.date()

    day_dir = _day_dir(raw_root, day)
    paths = sorted(day_dir.glob("*.jsonl"))
    if not paths:
        raise FileNotFoundError(f"no raw files for {day} under {raw_root}")

    records, corrupt = load_day(day_dir)
    report = {
        "day": day.isoformat(),
        "files": [str(p) for p in paths],
        "n_records": len(records),
        "corrupt_lines": corrupt,
        "anomalies": {},
    }
    if not records:
        raise DataValidationError(report)
    df = to_frame(records, day)

    schema_path = Path(schema_path)
    if refresh_schema or not schema_path.exists():
        stats = tfdv.generate_statistics_from_dataframe(df)
        schema_path.parent.mkdir(parents=True, exist_ok=True)
        tfdv.write_schema_text(infer_schema(stats), str(schema_path))
        log.warning("inferred new schema from %s at %s; review and commit it", day, schema_path)
    schema = tfdv.load_schema_text(str(schema_path))

    previous = previous_stats(day, out_root)
    stats, anomalies = validate(df, schema, previous)
    report["anomalies"] = summarize(anomalies)

    out_dir = _day_dir(out_root, day)
    out_dir.mkdir(parents=True, exist_ok=True)
    tfdv.write_stats_text(stats, str(out_dir / "stats.pbtxt"))
    write_html(stats, previous, out_dir / "stats.html", day)
    tfdv.write_anomalies_text(anomalies, str(out_dir / "anomalies.pbtxt"))
    (out_dir / "report.json").write_text(json.dumps(report, indent=2))
    log.info("validated %d records from %d files for %s", len(records), len(paths), day)

    if report["anomalies"] or corrupt:
        for name, reasons in report["anomalies"].items():
            log.error("anomaly in %s: %s", name, "; ".join(reasons))
        if corrupt:
            log.error("%d corrupt lines skipped", corrupt)
        raise DataValidationError(report)
    return report


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("day", nargs="?", help="YYYY-MM-DD (default: today, UTC)")
    parser.add_argument(
        "--refresh-schema", action="store_true", help="re-infer the schema from this day"
    )
    args = parser.parse_args()
    try:
        result = run(args.day, refresh_schema=args.refresh_schema)
    except DataValidationError as exc:
        send_alert(f"[FinMLOps] data anomalies on {exc.report['day']}", alert_text(exc.report))
        raise
    except FileNotFoundError as exc:
        send_alert("[FinMLOps] no raw data to validate", str(exc))
        raise
    print(json.dumps(result, indent=2))
