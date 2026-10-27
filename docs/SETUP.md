# Setup and running things by hand

The [README](../README.md#quick-start-airflow) has the quick start. This page has everything around it: what you need, which machines work, the `.env` settings, how the Docker setup works, and how to run each step yourself without Airflow.

## What you need

- **Docker** with Compose (Docker Desktop on Mac/Windows) to run Airflow
- **[uv](https://docs.astral.sh/uv/getting-started/installation/)** if you want to run the scripts or tests outside Docker
- **API keys** for [Finnhub](https://finnhub.io/register) and [Marketaux](https://www.marketaux.com/register), both free
- `make` (already there on Linux and macOS)

## Supported platforms

The only picky dependency is TFDV, which only publishes packages for Linux on x86_64 (glibc 2.39+) and macOS on Apple Silicon. In practice:

| Machine | Airflow in Docker (`make up`) | Scripts and tests with `uv` |
|---|---|---|
| Linux, Intel/AMD (Fedora 40+, Ubuntu 24.04+) | works | works |
| Linux, Intel/AMD, older distro (e.g. Ubuntu 22.04) | works | TFDV won't install (glibc too old) |
| Windows, Intel/AMD | works with Docker Desktop + WSL2 | use WSL2 with Ubuntu 24.04 |
| Mac, Intel | works | TFDV won't install |
| Mac, Apple Silicon (M1 to M4) | probably won't build (no TFDV package for Linux arm64), untested | works |

Everything except `tfdv_validate.py` works anywhere.

> [!WARNING]
> On an Apple Silicon Mac, `make up` will probably fail while installing TFDV. Try adding `platform: linux/amd64` under `x-airflow-common` in `docker-compose.yaml`. It builds the x86_64 image under emulation, which is slow and might not run TensorFlow, so let us know how it goes.

## The `.env` file

`cp .env.example .env` gives you a template. The API keys are required:

```text
MARKETAUX_API_KEY=your_marketaux_api_key
FINNHUB_API_KEY=your_finnhub_api_key
```

The Gmail settings are optional and only used for alert emails (the Gmail account needs an [app password](https://myaccount.google.com/apppasswords), not your normal password):

```text
GMAIL_USERNAME=bot@gmail.com
GMAIL_APP_PASSWORD=abcd efgh ijkl mnop
ALERT_RECIPIENTS=a@x.com,b@x.com
```

If they're blank, alerts are just skipped with a warning and everything else still runs.

> [!IMPORTANT]
> The CI failure email uses GitHub secrets, which only GitHub Actions can see. The pipeline can't read those, so you need to put the Gmail settings in your own `.env` too.

> [!NOTE]
> Docker reads `.env` when it creates the containers, so after changing it, run `make up` again.

## Keeping the computer awake

Airflow only runs while your computer is on and awake. If it sleeps, `catchup` fetches the missed windows once it wakes up, but keep it awake around **06:00 to 06:30 UTC** (2:00 to 2:30 AM US Eastern), when the last windows of the day come in and `process_news` runs.

On Linux, this blocks sleep (including closing the lid) for as long as it runs:
```bash
systemd-inhibit --what=sleep:idle:handle-lid-switch --why="Airflow collecting data" sleep infinity
```
On a Mac, `caffeinate -i` does the same. Keep the terminal open and the laptop plugged in, and press Ctrl+C to get normal sleep back.

## Useful Airflow commands

```bash
# run a DAG once right now (from the UI: the play button)
docker compose exec airflow-scheduler airflow dags trigger ingest_finnhub

# run process_news for a specific day without waiting (this one validates 2026-10-25)
docker compose exec airflow-scheduler airflow dags test process_news 2026-10-26T06:30:00+00:00

# make Airflow pick up a new or changed DAG file now (otherwise it can take up to 5 minutes)
docker compose exec airflow-scheduler airflow dags reserialize

# see what's running
docker compose ps
```

Task logs are in the UI (click a run, then a task) and also in `logs/`. The UI shows times in your local time zone; the DAGs are scheduled in UTC.

## How the Docker setup works

- **`Dockerfile`**: we don't use the official Airflow image. It's based on an older Debian, and TFDV 1.21 won't install on it, so we install Airflow on top of `python:3.11-slim-trixie` instead. It's a two-stage build: the first stage builds the pipeline's Python environment (`/opt/venv`, from `uv.lock`) with a compiler, and the final image only copies the result. The pipeline scripts run with that environment, separate from Airflow's, so TFDV and TensorFlow can't mess with Airflow's packages. Downloads are kept in Docker's build cache, so rebuilding after a dependency change takes about a minute instead of three.
- **`docker-compose.yaml`**: Postgres, the scheduler, the API server (the UI), and the DAG processor. The repo is mounted into the containers, so DAGs read and write the real `data/` folder, and `.env` is passed to all of them. Postgres is only there for Airflow's own bookkeeping (DAG runs, task status, history). Our data doesn't go in it.
- **`Makefile`**: `make up`, `make down` and `make snapshot`.

> [!NOTE]
> The image is about 4 GB, almost all of it TensorFlow.

## Running the scripts yourself (no Airflow)

You can run each step by hand too, which is handy for testing. Install the dependencies first:
```bash
uv sync
```

Then:
```bash
# fetch the last 24 hours for all tickers and both sources
uv run python -m scripts.ingest

# or just some tickers
uv run python -m scripts.ingest AAPL TSLA

# or one source and an exact time window (this is how the DAGs call it)
uv run python -m scripts.ingest --source finnhub --since 2026-10-25T10:00:00+00:00 --until 2026-10-25T11:00:00+00:00

# validate a day of raw data (defaults to today, UTC, so pass a finished day)
uv run python -m scripts.tfdv_validate 2026-10-25

# clean everything in data/raw/ into data/processed/news.parquet
uv run python -m scripts.preprocess
```

To fill in a past day, fetch the whole day in one go. The file lands in that day's folder:
```bash
uv run python -m scripts.ingest --source finnhub --since 2026-10-20T00:00:00+00:00 --until 2026-10-21T00:00:00+00:00
```
Finnhub's free plan goes back about a year.

> [!CAUTION]
> Every manual ingest run uses API requests from the same daily limits as the DAGs, so running it by hand while the DAGs are on can use up the Marketaux quota (a full day costs up to 21 Marketaux requests). Marketaux also has a short-term rate limit, so when fetching several past days, run them about a minute apart, not back to back.

> [!WARNING]
> Don't fetch a window that's already in `data/raw/`. Raw files are never overwritten, so you'd end up with the same articles twice. If you need to redo a day, move its old files out of `data/raw/` first.
