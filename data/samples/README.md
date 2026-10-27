# Data samples

The full data lives in DVC (see [docs/DVC_GUIDE.md](../../docs/DVC_GUIDE.md)), and you need access to our bucket to pull it. These are small samples from one day, 22 Oct 2026, committed to git so anyone can see what the data looks like at each step without setting anything up.

| File | What it is |
|---|---|
| `raw/finnhub_2026-10-22.jsonl` | the first 10 Finnhub items of the day, exactly as the API sent them (one JSON object per line) |
| `raw/marketaux_2026-10-22.jsonl` | the first 10 Marketaux items of the day, exactly as the API sent them |
| `processed/news_2026-10-22.parquet` | 50 rows of the cleaned data (40 Finnhub, 10 Marketaux), the same columns as `data/processed/news.parquet` |
| `processed/news_2026-10-22.csv` | the same 50 rows as CSV, so GitHub can show them as a table |
| `validation/report_2026-10-22.json` | that day's validation report (article counts per source and ticker, no anomalies) |
| `validation/stats_2026-10-22.html` | that day's TFDV stats page; download it and open it in a browser (it needs internet to load) |

> [!NOTE]
> The raw samples and the cleaned rows aren't the same articles: the raw files are the first items each API returned, and the cleaned rows are the first ones for that day after cleaning, deduplication and splitting per ticker.

> [!IMPORTANT]
> We only commit a few rows. The articles belong to their publishers and the APIs' terms limit redistribution, so the full dataset stays in our private DVC bucket.
