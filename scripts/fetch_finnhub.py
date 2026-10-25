"""https://finnhub.io/docs/api/company-news"""

from __future__ import annotations
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import httpx


from scripts.helpers import NewsSource, in_window, parse_dt


class FinnhubSource(NewsSource):
    type_name = "finnhub"
    env_key = "FINNHUB_API_KEY"
    URL = "https://finnhub.io/api/v1/company-news"

    async def fetch(
        self, query: str, since: datetime, until: datetime, client: httpx.AsyncClient
    ) -> list[dict[str, Any]]:
        params = {
            "symbol": query.upper(),
            "from": since.date().isoformat(),
            "to": until.date().isoformat(),
            "token": self.api_key,
        }
        items = await self._get_json(client, self.URL, params=params)
        # keep items exactly as Finnhub sent them, only trimmed to the requested window
        return [i for i in items or [] if in_window(parse_dt(i["datetime"]), since, until)]
