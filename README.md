# FinMLOps Data Pipeline

[![pytest](https://github.com/FinMLOps/finmlops-data-pipeline/actions/workflows/pytest.yml/badge.svg?branch=main)](https://github.com/FinMLOps/finmlops-data-pipeline/actions/workflows/pytest.yml)

This is the data pipeline for our FinMLOps project. It pulls financial news for the Magnificent 7 (AAPL, MSFT, NVDA, GOOGL, META, AMZN, TSLA) from the Finnhub and Marketaux APIs, checks the raw data with TensorFlow Data Validation, and cleans it into a single Parquet file that we'll later use for sentiment modeling.

> [!IMPORTANT]
> - We use [`uv`](https://docs.astral.sh/uv/), which is superior to `pip`.
> - Python is pinned to `3.11` (`>=3.11,<3.12`) to ensure compatibility and stability across pipeline dependencies.

## How it works

The pipeline has three steps:

```text
ingest -> tfdv_validate -> preprocess
```

**Ingest** (`scripts/ingest.py`) fetches news for every ticker from both APIs at the same time and saves the responses as-is, one JSONL file per source, under `data/raw/YYYY/MM/DD/`. We never overwrite raw files. If an API rate limits us or has a server error, the request is retried a few times, and if one source or ticker fails the rest still get saved.

**Validate** (`scripts/tfdv_validate.py`) checks one day of raw data against a saved schema using TFDV. It catches things like missing fields, tickers we don't track, weirdly long text, old or future-dated articles, and days where we got way fewer articles than usual. More details [below](#data-validation).

**Preprocess** (`scripts/preprocess.py`) takes all the raw data and turns it into `data/processed/news.parquet`. It maps the two APIs' different formats to the same columns, strips HTML, converts timestamps to UTC, fixes ticker variants (`MSFT.MI` becomes `MSFT`, `GOOG` becomes `GOOGL`), drops non-English articles, and removes duplicate headlines (keeping the earliest one). Each row is one article for one ticker, so an article about both Apple and Nvidia shows up twice.

## Setup

Install the dependencies:
```bash
uv sync
```

Make a `.env` file from the template (this won't touch it if you already have one):
```bash
[ -f .env ] || cp .env.example .env
```

Then add your API keys to `.env`:
```text
MARKETAUX_API_KEY=your_marketaux_api_key
FINNHUB_API_KEY=your_finnhub_api_key
```

The Gmail settings in there are only needed if you want email alerts (see [Alerts](#alerts)).

## Running it

### Running the steps yourself

```bash
# fetch the last 24 hours for all tickers and both sources
uv run python -m scripts.ingest

# or just some tickers
uv run python -m scripts.ingest AAPL TSLA

# or one source and an exact time window (this is how Airflow will call it)
uv run python -m scripts.ingest --source finnhub --since 2026-10-25T10:00:00+00:00 --until 2026-10-25T11:00:00+00:00

# validate a day of raw data (defaults to today, UTC)
uv run python -m scripts.tfdv_validate 2026-10-25

# clean everything in data/raw/ into data/processed/news.parquet
uv run python -m scripts.preprocess
```

### Running Airflow

We run Airflow 3.3 locally with Docker Compose, so you'll need Docker installed.

```bash
make up      # builds the image and starts Airflow, UI at http://localhost:8080
make down    # stops it (run history is kept)
```

There's no login on the UI since it's only meant to run on your own machine.

`make up` runs the containers as your own user, otherwise on Linux the files Airflow writes into the repo would end up owned by someone else. If you don't have `make`, use `AIRFLOW_UID=$(id -u) docker compose up -d --build` instead. The first build takes a few minutes because of TensorFlow.

A few notes on the setup:
- We don't use the official Airflow image. It's based on an older Debian, and TFDV 1.21 won't install on it, so our `Dockerfile` installs Airflow on top of `python:3.11-slim-trixie` instead.
- The pipeline scripts run in their own Python environment inside the image (`/opt/venv`, built from `uv.lock`), separate from Airflow's. That way TFDV and TensorFlow can't mess with Airflow's packages.
- Postgres is only there for Airflow's own bookkeeping (DAG runs, task status, history). Our data doesn't go in it.
- `dags/ingest_news_dag.py` has the ingestion DAGs. `ingest_finnhub` runs every hour and `ingest_marketaux` every 6 hours, and each run fetches only its own time window, so runs never overlap. Turn the DAGs on in the UI to start collecting.

### Why hourly for Finnhub and every 6 hours for Marketaux

We track 7 tickers, and each run makes one request per ticker (Marketaux can take up to 3 pages per ticker), so the schedules come down to each API's free-plan limits:

| | Requests per run | Runs per day | Requests per day | Free plan limit |
|---|---|---|---|---|
| Finnhub, hourly | 7 tickers x 1 = 7 | 24 | 7 x 24 = 168 | ~60 per minute, no daily cap |
| Marketaux, every 6 hours | 7 tickers x up to 3 pages = 21 | 4 | 21 x 4 = 84 at most | ~100 per day |

Finnhub's only limit is per minute, and 7 requests at once is well under 60, so we could run it more often. We don't, because its company news endpoint only filters by date, so every call returns the whole day's articles anyway. Running more than once an hour would mostly re-download the same articles.

Marketaux is the one that's limited. Every 6 hours uses at most 84 of the ~100 daily requests and leaves some room for retries. Running every 4 hours would be 21 x 6 = 126 requests, which is over the limit. In practice we use fewer than 84 because tickers with less news stop after the first page.

## Data validation

`tfdv_validate.py` reads a day of raw files, turns each article into a few simple features, and checks them against the schema in `data/schema/schema.pbtxt`:

| Feature | What we check |
|---|---|
| `source` | required, must be `finnhub` or `marketaux` |
| `symbol` | required, must be one of our tickers, and the mix of tickers shouldn't shift too much from the day before |
| `title_len` | required, can't be empty, flagged if over 3x the longest title in the baseline, and checked for drift |
| `published_hour` | required, 0 to 23 |
| `age_days` | how old the article is compared to the day we're validating, must be 0 to 7 |
| `summary_len` | optional, flagged if over 3x the baseline max, and checked for drift |
| `n_symbols` | optional, flagged if over 3x the baseline max |
| `has_url`, `publisher` | optional, we keep `publisher` for bias slicing later |
| number of articles | flagged if we got less than half the articles of the last validated day (usually means an API quietly failed). Validate full days only, a partial day will trip this |

Lines it can't read (bad JSON, no timestamp, unknown source) get skipped and counted. Every run saves the stats, any anomalies, a JSON report and an HTML stats page to `data/validation/YYYY/MM/DD/`. The HTML page needs internet to load. If anything looks wrong it raises an error, so the Airflow task fails, and when you run it from the terminal it also sends an email alert.

If there's no schema yet, the first run creates one from that day's data. You should look over it and commit it, after that it stays the same. To rebuild it from a good day on purpose:
```bash
uv run python -m scripts.tfdv_validate 2026-10-20 --refresh-schema
```

## Alerts

`scripts/alert_utils.py` sends alert emails through Gmail. Add these to your `.env` to turn it on:

```text
GMAIL_USERNAME=bot@gmail.com          # the Gmail account (needs an app password)
GMAIL_APP_PASSWORD=...
ALERT_RECIPIENTS=a@x.com,b@x.com
```

If they're blank, alerts are just skipped with a warning and everything else still runs. If sending fails, that gets logged but the original error still comes through.

Note that the CI failure email uses GitHub secrets, which only GitHub Actions can see. The pipeline can't read those, so you need to put the Gmail settings in your own `.env` too.

## Tests

```bash
uv run pytest                              # everything
uv run pytest tests/test_preprocess.py     # or one file
```

The tests don't hit any APIs or send real emails, those are all mocked. CI runs them on every push and PR to `main`.

## Project structure

```text
finmlops-data-pipeline/
├── .github/workflows/
│   ├── pytest.yml            # runs the tests in CI
│   └── claude.yml            # Claude PR review assistant
├── dags/
│   └── ingest_news_dag.py    # ingestion DAGs (Finnhub hourly, Marketaux every 6 hours)
├── data/
│   ├── raw/                  # raw API responses by date (not in git)
│   ├── processed/            # news.parquet (not in git)
│   ├── schema/               # TFDV schema (in git)
│   └── validation/           # TFDV stats and reports by date (not in git)
├── logs/                     # Airflow task logs (not in git)
├── scripts/
│   ├── config.py             # the tickers we track
│   ├── ingest.py             # fetches news and saves raw JSONL
│   ├── tfdv_validate.py      # TFDV checks on raw data
│   ├── preprocess.py         # cleans raw data into Parquet
│   ├── alert_utils.py        # email alerts
│   ├── fetch_finnhub.py      # Finnhub API
│   ├── fetch_marketaux.py    # Marketaux API
│   └── helpers/
│       ├── base.py           # shared API code (keys, retries, hiding keys in logs)
│       └── schemas.py        # data classes and date parsing
├── tests/                    # one test file per script
├── Dockerfile                # Airflow image with our dependencies
├── docker-compose.yaml       # local Airflow setup
├── Makefile                  # make up / make down
├── .env.example              # template for .env
├── pyproject.toml
└── uv.lock
```

## What's left

- Airflow DAGs for hourly Finnhub ingestion, daily Marketaux ingestion, and a daily validate + preprocess run
- Data versioning with DVC (pushing to Google Cloud Storage)
- Bias detection with data slicing
- Speeding up the pipeline using Airflow's Gantt chart
