"""https://www.marketaux.com/documentation"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # only annotations need httpx here; fetching imports it for real
    import httpx

from scripts.helpers import NewsSource, SourceError, in_window, parse_dt

log = logging.getLogger(__name__)


class MarketauxSource(NewsSource):
    type_name = "marketaux"
    env_key = "MARKETAUX_API_KEY"
    URL = "https://api.marketaux.com/v1/news/all"

    async def fetch(
        self, query: str, since: datetime, until: datetime, client: httpx.AsyncClient
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for page in range(1, int(self.options.get("max_pages", 3)) + 1):
            params = {
                "symbols": query.upper(),
                "published_after": since.strftime("%Y-%m-%dT%H:%M"),
                "published_before": until.strftime("%Y-%m-%dT%H:%M"),
                "language": self.options.get("language", "en"),
                "page": page,
                "api_token": self.api_key,
            }
            try:
                data = await self._get_json(client, self.URL, params=params)
            except SourceError as exc:
                if page == 1:
                    raise  # nothing fetched yet: report the failure
                log.warning("%s; keeping %d items from earlier pages", exc, len(out))
                break
            items = data.get("data") or []
            out += [i for i in items if in_window(parse_dt(i["published_at"]), since, until)]
            meta = data.get("meta") or {}
            if not items or meta.get("returned", 0) < meta.get("limit", 1):
                break
        return out
