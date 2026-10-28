"""Unit tests for timestamp parsing and id helpers."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from scripts.helpers.schemas import UTC, make_id, parse_dt, to_utc


def test_to_utc():
    sample_time_utc_unaware = datetime(2026, 10, 12, 15, 59, 30)
    sample_time_utc_aware = to_utc(sample_time_utc_unaware)
    assert sample_time_utc_aware.tzinfo == UTC

    sample_time_already_utc_aware = datetime(
        2026, 10, 12, 15, 59, 30, tzinfo=timezone.utc
    )
    assert to_utc(sample_time_already_utc_aware) == sample_time_already_utc_aware


def test_parse_dt():
    CORRECT_OUTPUT = "2026-10-12T15:59:30+00:00"

    # Unix timestamp in seconds
    assert parse_dt(1791820770).isoformat() == CORRECT_OUTPUT
    assert parse_dt("1791820770").isoformat() == CORRECT_OUTPUT  # numeric string

    # Unix timestamp in milliseconds
    assert parse_dt(1791820770000).isoformat() == CORRECT_OUTPUT
    assert parse_dt("1791820770000").isoformat() == CORRECT_OUTPUT

    assert parse_dt("20261012T155930").isoformat() == CORRECT_OUTPUT
    assert parse_dt("2026-10-12T15:59:30Z").isoformat() == CORRECT_OUTPUT

    # Invalid timestamp
    with pytest.raises(ValueError, match="unrecognised timestamp"):
        parse_dt("not_a_valid_date")


def test_make_id():
    id1 = make_id("marketaux", "https://news.com/1", "ABC")
    id2 = make_id("marketaux", "https://news.com/1", "ABC")
    id3 = make_id("marketaux", "https://news.com/2", "XYZ")

    assert id1 == id2
    assert id1 != id3
    assert len(id1) == 16

