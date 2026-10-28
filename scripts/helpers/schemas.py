"""Timestamp parsing and id helpers shared by the ingest and preprocess steps."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

UTC = timezone.utc
_EPOCH_MS_THRESHOLD = 1e11


def to_utc(dt: datetime) -> datetime:
    """Ensure datetime is timezone-aware in UTC."""
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def parse_dt(value: Any) -> datetime:
    """Parse timestamps (Finnhub's unix seconds, Marketaux's ISO 8601) into aware UTC."""
    if isinstance(value, datetime):
        return to_utc(value)

    if isinstance(value, (int, float)):
        seconds = value / 1000.0 if value > _EPOCH_MS_THRESHOLD else float(value)
        return datetime.fromtimestamp(seconds, tz=UTC)

    text = str(value).strip()

    if text.isdigit():
        val = int(text)
        seconds = val / 1000.0 if val > _EPOCH_MS_THRESHOLD else float(val)
        return datetime.fromtimestamp(seconds, tz=UTC)

    # compact form YYYYMMDDTHHMMSS (e.g. 20241108T012400)
    if len(text) == 15 and text[8] == "T" and text[:8].isdigit() and text[9:].isdigit():
        return datetime(
            int(text[:4]),
            int(text[4:6]),
            int(text[6:8]),
            int(text[9:11]),
            int(text[11:13]),
            int(text[13:15]),
            tzinfo=UTC,
        )

    # standard ISO 8601 (Marketaux)
    try:
        return to_utc(datetime.fromisoformat(text))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"unrecognised timestamp: {value!r}") from exc


def make_id(*parts: str) -> str:
    """Deterministic short id from a handful of strings (source, url, title...)."""
    digest = hashlib.sha1(
        "\x1f".join(parts).encode("utf-8"),
        usedforsecurity=False,
    ).hexdigest()
    return digest[:16]


def utcnow() -> datetime:
    return datetime.now(tz=UTC)
