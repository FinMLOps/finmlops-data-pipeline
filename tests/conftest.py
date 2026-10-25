"""Shared pytest fixtures."""

from __future__ import annotations

import pytest

from scripts.helpers import base


@pytest.fixture(autouse=True)
def no_retry_backoff(monkeypatch):
    """Keep retry logic but skip the real sleeps between attempts."""
    monkeypatch.setattr(base, "RETRY_BACKOFF_FACTOR", 0.0)
