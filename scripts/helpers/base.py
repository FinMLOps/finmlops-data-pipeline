"""Base class and common utilities for financial news API sources."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar, Iterable, Mapping

from dotenv import load_dotenv

load_dotenv()

if TYPE_CHECKING:
    import httpx

from .schemas import (
    UTC,
    Article,
    make_id,
    parse_dt,
    to_utc,
)


class SourceError(RuntimeError):
    """Raised when an API request fails."""

    def __init__(
        self,
        source: str,
        message: str,
        status: int | None = None,
    ) -> None:
        self.source = source
        self.status = status
        prefix = f"{source}: " if source else ""
        suffix = f" (HTTP {status})" if status else ""
        super().__init__(f"{prefix}{message}{suffix}")


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

    @abstractmethod
    async def fetch(
        self, query: str, since: datetime, until: datetime, client: httpx.AsyncClient
    ) -> list[Article]:
        """Fetch articles for a ticker symbol in [since, until]."""

    async def _get_json(
        self,
        client: httpx.AsyncClient,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        import httpx

        try:
            response = await client.get(url, params=params, headers=headers)
        except httpx.HTTPError as exc:
            raise SourceError(self.name, f"request failed: {exc}") from exc

        if response.status_code >= 400:
            raise SourceError(
                self.name,
                _short(response.text),
                response.status_code,
            )

        try:
            return response.json()
        except ValueError as exc:
            raise SourceError(
                self.name, f"invalid JSON: {_short(response.text)}"
            ) from exc

    def _article(
        self,
        *,
        title: str,
        published: datetime,
        url: str | None = None,
        summary: str | None = None,
        symbols: Iterable[str] = (),
        raw: Mapping[str, Any] | None = None,
    ) -> Article:
        key = url or f"{title}|{published.isoformat()}"
        return Article(
            id=make_id(self.name, key),
            source=self.name,
            title=title.strip(),
            published=published,
            url=url,
            summary=(summary or "").strip() or None,
            symbols=tuple(s.upper() for s in symbols if s),
            raw=dict(raw or {}),
        )


def in_window(published: datetime, since: datetime, until: datetime) -> bool:
    return since <= published <= until


def _short(text: str, limit: int = 200) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 3] + "..."
