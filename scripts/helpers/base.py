"""Base class and common utilities for financial news API sources."""

from __future__ import annotations

import asyncio
import logging
import os
import urllib.parse
from abc import ABC, abstractmethod
from collections.abc import Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar

from dotenv import load_dotenv

load_dotenv()

if TYPE_CHECKING:
    import httpx

log = logging.getLogger(__name__)

REQUEST_MAX_RETRIES = 3  # retries after the first failed attempt
RETRY_BACKOFF_FACTOR = (
    0.5  # first wait between retries, doubled each time (0.5s, 1s, 2s)
)
RETRY_MAX_BACKOFF = 30.0  # longest single wait allowed, in seconds
RETRY_STATUSES = frozenset(
    {429, 500, 502, 503, 504}
)  # HTTP status codes that trigger a retry


def redact(text: str, secret: str | None) -> str:
    """Replace sensitive API keys in text (URLs, error messages) to protect logs.

    Note:
        Secrets shorter than 8 characters (e.g. short test or demo keys) are ignored
        to prevent accidental over-redaction of common substrings.
    """
    if not secret or len(secret) < 8:
        return text

    text = text.replace(secret, "[REDACTED]")

    encoded = urllib.parse.quote(secret, safe="")
    if encoded != secret:
        text = text.replace(encoded, "[REDACTED]")

    return text


class SourceError(RuntimeError):
    """Raised when an API request fails, stripping secrets from logs."""

    def __init__(
        self,
        source: str,
        message: str,
        status: int | None = None,
        *,
        secret: str | None = None,
    ) -> None:
        self.source = source
        self.status = status
        prefix = f"{source}: " if source else ""
        suffix = f" (HTTP {status})" if status else ""
        super().__init__(f"{prefix}{redact(message, secret)}{suffix}")


class NewsSource(ABC):
    """Abstract base class for financial news providers (Marketaux, Finnhub, Alpha Vantage)."""

    type_name: ClassVar[str]
    env_key: ClassVar[str | None] = None

    def __init__(
        self, api_key: str | None = None, name: str | None = None, **options: Any
    ) -> None:
        self.name = name or self.type_name
        self.api_key = api_key or (
            os.environ.get(self.env_key) if self.env_key else None
        )
        self.options = options
        if not self.api_key:
            hint = (
                f"set {self.env_key} or pass api_key="
                if self.env_key
                else "pass api_key="
            )
            raise SourceError(self.name, f"API key required; {hint}")

    def __repr__(self) -> str:
        return f"<{type(self).__name__} name={self.name!r}>"

    def _error(self, message: str, status: int | None = None) -> SourceError:
        """Create a SourceError pre-configured with this source's name and API key."""
        return SourceError(self.name, message, status, secret=self.api_key)

    @abstractmethod
    async def fetch(
        self, query: str, since: datetime, until: datetime, client: httpx.AsyncClient
    ) -> list[dict[str, Any]]:
        """Fetch raw API items for a ticker symbol in [since, until]."""

    async def _get_json(
        self,
        client: httpx.AsyncClient,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        import httpx

        for attempt in range(REQUEST_MAX_RETRIES + 1):
            last = attempt == REQUEST_MAX_RETRIES
            try:
                response = await client.get(url, params=params, headers=headers)
            except httpx.HTTPError as exc:
                if last:
                    raise self._error(f"request failed: {exc}") from None
            else:
                if response.status_code not in RETRY_STATUSES or last:
                    break
            log.warning("%s: retry %d/%d", self.name, attempt + 1, REQUEST_MAX_RETRIES)
            await asyncio.sleep(
                min(RETRY_BACKOFF_FACTOR * 2**attempt, RETRY_MAX_BACKOFF)
            )

        if response.status_code >= 400:
            safe_text = _short(redact(response.text, self.api_key))
            raise self._error(safe_text, response.status_code)

        try:
            return response.json()
        except ValueError:
            safe_text = _short(redact(response.text, self.api_key))
            raise self._error(f"invalid JSON: {safe_text}") from None


def in_window(published: datetime, since: datetime, until: datetime) -> bool:
    return since <= published <= until


def _short(text: str, limit: int = 200) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 3] + "..."
