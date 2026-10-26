# How we covered the data pipeline rubric

We went through the data pipeline submission guidelines one point at a time and wrote down where each part lives in our repo. If you want to actually set things up and run them, the [README](../README.md) has all the steps.

> [!NOTE]
> All paths are from the root of the repo. Our GitHub org keeps the model in a separate repo, so this whole repo is the data pipeline and we didn't add an extra `Data-Pipeline/` folder.

## 2. Key components

### 2.1 Data acquisition

We get our news from two free APIs, Finnhub and Marketaux, for the 7 tickers we track (the Magnificent 7). Every response is saved exactly the way the API sent it, so we always have the original data to go back to.

| What | Where |
|---|---|
| Getting news from Finnhub (one request per ticker) | `scripts/fetch_finnhub.py` |
| Getting news from Marketaux (one request per ticker, up to 3 pages) | `scripts/fetch_marketaux.py` |
| Code both APIs share: reading keys from `.env`, retrying with backoff on 429/5xx errors, hiding keys in logs | `scripts/helpers/base.py` |
| Fetching all sources and tickers at the same time and saving the raw responses | `scripts/ingest.py` → `data/raw/YYYY/MM/DD/<source>_<time>.jsonl` |
| The list of tickers | `scripts/config.py` |
| Dependencies | `pyproject.toml` and `uv.lock` (we use uv, which pins exact versions like a `requirements.txt` would), plus the `Dockerfile` for Airflow |
| Template for the API keys | `.env.example` |

To keep it reproducible, every run fetches one exact time window (`--since` and `--until`), so running the same window again gets the same articles. Raw files are also never overwritten.

### 2.2 Data preprocessing

Since we get our data from APIs and don't collect it by hand, it already comes in pretty clean, so we had very little cleaning to do. We still wrote a preprocessing step to strip HTML tags, decode HTML entities (like `&amp;` → `&`) and collapse extra whitespace, just to be safe. When we ran it on all our raw data so far, it only changed 6 out of 297 records (2%), and all 6 were HTML entities. There were no HTML tags, no messy whitespace, no empty titles, and every article was already in English.

Most of the actual work in preprocessing is getting both APIs into the same format and deciding what one row means. Everything is in `scripts/preprocess.py`:

| Step | Function |
|---|---|
| Read all the raw files, skipping (and logging) any broken lines | `load_raw()` |
| Turn each API's fields into one common format | `finnhub_row()`, `marketaux_row()` and `make_row()`, picked through `MAPPERS` |
| Strip tags, decode entities, collapse whitespace | `clean_text()` |
| Standardize tickers (`MSFT.MI` → `MSFT`, `GOOG` → `GOOGL`) and drop the ones we don't track | `to_tickers()` |
| Convert times to UTC, drop empty or non-English articles, make one row per article per ticker, remove duplicate headlines (keeping the earliest one), and build the `text` column (title + summary) for the model | `preprocess()` |
| Save everything to `data/processed/news.parquet` | `run()` |

If we ever add another API, we only need to write one new `*_row()` function and add it to `MAPPERS`.

### 2.3 Test modules

We have 75 tests using pytest. None of them call the real APIs or send real emails (we mock those with `respx` and a fake SMTP server), so they're free to run as often as we want. GitHub Actions runs them on every push and PR to `main` (`.github/workflows/pytest.yml`).

| File | Tests | What they check |
|---|---|---|
| `tests/test_preprocess.py` | 12 | mapping fields for each API, HTML cleanup, standardizing tickers, dropping bad records, one row per ticker, keeping the earliest duplicate, broken raw lines, our old raw format, no raw data at all |
| `tests/test_tfdv_validate.py` | 29 | building the schema from several days, comparing with the same weekday, schema domains, bad values, unknown tickers or sources, outliers, stale and future articles, ticker drift, too few or too many articles, a missing source or ticker, one source dropping by half, duplicate spikes, non-English articles, corrupt lines, a missing day |
| `tests/test_ingest.py` | 10 | saving raw data as received, one source failing while the other still works, missing API keys, every request failing, never overwriting files, retrying 5xx but not 4xx, CLI options, date folders |
| `tests/test_schemas.py` | 9 | parsing timestamps and converting them to UTC, IDs, the article data classes |
| `tests/test_finnhub.py` | 5 | missing key, a normal fetch, HTTP errors, invalid JSON, window boundaries |
| `tests/test_marketaux.py` | 5 | missing key, a normal fetch, pagination, HTTP errors, keeping earlier pages when a later page fails |
| `tests/test_alert_utils.py` | 5 | email sent or skipped, a broken email never crashes anything, what the Airflow failure email contains (with and without the task log) |

### 2.4 Pipeline orchestration (Airflow DAGs)

We have three DAGs:

| DAG | File | Runs (UTC) | Tasks |
|---|---|---|---|
| `ingest_finnhub` | `dags/ingest_news_dag.py` | every hour | `ingest`, which fetches exactly that hour |
| `ingest_marketaux` | `dags/ingest_news_dag.py` | every 6 hours | `ingest`, which fetches exactly those 6 hours |
| `process_news` | `dags/process_news_dag.py` | every day at 00:30 | `tfdv_validate` >> `preprocess` |

- Every run gets its own `[start, end)` time window from `CronDataIntervalTimetable`. With `catchup=True`, if the laptop running Airflow was asleep, Airflow goes back and runs the windows it missed.
- `preprocess` only runs if validation passed, so bad data never makes it into `news.parquet`.
- Ingest retries twice, 5 minutes apart. Validation doesn't retry, since bad data won't fix itself on a retry.
- Airflow runs in Docker (`Dockerfile`, `docker-compose.yaml`), and `make up` / `make down` start and stop it.

### 2.5 Data versioning with DVC

| What | Where |
|---|---|
| Pointer to the raw data | `data/raw.dvc` |
| The preprocess stage (its inputs, script and output) | `dvc.yaml` |
| The exact data versions from the last run | `dvc.lock` |
| Shared DVC settings | `.dvc/config` |
| One command to take a snapshot (`dvc add` → `dvc repro` → `dvc push` → commit the pointer files) | `make snapshot` in the `Makefile` |
| How to set it up and use it | `docs/dvc_guide.md` |

> [!NOTE]
> Our DVC remote is a Google Cloud Storage bucket (`gs://finmlops-pipeline-dvc/dvc`) in our teammate Kshitij's personal Google Cloud account, so everyone on the team can push and pull the same data. We went with a personal account because the Google Cloud access from our university accounts came with a lot of settings and restrictions. Before we had the bucket we used a local folder as the remote.

### 2.6 Tracking and logging

- All our scripts use Python's `logging` (`ingest.py`, `fetch_marketaux.py`, `helpers/base.py`, `preprocess.py`, `tfdv_validate.py`, `alert_utils.py`). They log how many articles came in per source and ticker, retries, skipped records, files written and any anomalies.
- Airflow saves a log for every task attempt in `logs/`, and you can read them in the UI.
- When something fails, the team gets an email (see 2.8).

### 2.7 Data schema and statistics generation

We used TFDV for this, in `scripts/tfdv_validate.py`. The `tfdv_validate` task runs it every day.

- **Schema:** `data/schema/schema.pbtxt`. We first made it from a single day (24 Oct 2026, 274 articles), but once we had a month of data that turned out to be too narrow: normal weekday articles with long summaries or lots of tickers got flagged as outliers. So we rebuilt it from two full weeks (12 to 23 Oct, 6,020 articles) with `--baseline 2026-10-12 2026-10-23` and committed it to git, so every later day gets checked against the same thing. `infer_schema()` adds allowed values, required fields and ranges on top of what TFDV infers.
- **Statistics:** each day's stats, anomalies, a JSON report and an HTML stats page are saved to `data/validation/YYYY/MM/DD/`.
- **Over time:** each day is compared with the same weekday a week earlier to catch drift (L-infinity distance for categories, Jensen-Shannon for numbers) and big changes in how many articles we got. We started by comparing with the day before, but after backfilling a month of data every Saturday got flagged for low volume and every Monday for a spike, since weekends only get about a third of the weekday news. Comparing Saturday with last Saturday got rid of those false alarms.

### 2.8 Anomaly detection and alerts

| Problem | How we catch it |
|---|---|
| Missing values | `source`, `symbol`, `title_len` and `published_hour` are required, and empty titles get flagged |
| Wrong formats | `source`, `symbol` and `language` have to be values we expect, `published_hour` has to be 0–23, and lines we can't read are skipped and counted |
| Outliers | a title, summary or number of tickers more than 3x the biggest one in our baseline |
| Old articles | `age_days` has to be between 0 and 7 |
| Drift | the ticker mix and text lengths compared with the same weekday a week earlier |
| Volume | less than half or more than 3x the articles of the same weekday a week earlier |
| Coverage | a source or ticker with no articles at all, or one source dropping by half (`coverage_anomalies()`) |
| Duplicates | more than 60% of a source's articles repeating a URL |

If anything is found, `tfdv_validate` raises an error. The Airflow task fails, `process_news` stops before preprocessing, and `scripts/alert_utils.py` emails the team through Gmail. Any Airflow task that fails also sends an email with the DAG, task, time window, error and the last lines of its output (`airflow_failure_callback`). Failing tests in CI send their own email too (`.github/workflows/pytest.yml`).

### 2.9 Pipeline flow optimization

Right now the Gantt chart doesn't tell us much. We only have a few days of data, so every task finishes in a few seconds and there's no slow bar to look at. We need a few weeks of data before a bottleneck actually shows up.

We already know where it'll be, though. `ingest_finnhub` and `ingest_marketaux` only have one task each and only fetch their own time window, so they take about the same time every run. In `process_news`, `tfdv_validate` also only looks at one day. But `preprocess` re-reads all the raw data we've ever collected every time it runs, so it gets a little slower every day (on day 40 it processes 40 days of data just to add one). We also can't run `tfdv_validate` and `preprocess` in parallel, because the whole point is to not preprocess data that failed validation.

So our plan is:
1. Once we have a few weeks of data, take a Gantt screenshot of `process_news` showing `preprocess` as the long bar.
2. Change `preprocess` to only process the day that was just validated and save it as its own file (like `data/processed/2026/10/26.parquet`), so it takes the same time no matter how much history we have.
3. Take another screenshot and compare.

> [!IMPORTANT]
> Still to do: the before and after screenshots. We'll add them here once there's enough data for the difference to show.

What we've already done to keep things fast:
- Ingestion fetches all tickers at the same time (async `httpx` in `scripts/ingest.py`) and not one after another.
- Finnhub and Marketaux have separate DAGs, so they run in parallel and one failing doesn't hold up the other.
- We cleaned up the Docker build with cache mounts and two stages, and rebuilding after a dependency change went from 166 s to 51 s.

## 3. Data bias detection using data slicing

When we started collecting data, we thought one ticker might end up with a lot more articles than the others. That did happen: so far NVDA has 122 of our 280 articles (~44%), about 4-6x more than any other ticker (TSLA has 31, AMZN has 21). Most of this is from one day (24 Oct), but it's what we expected, since NVDA is in the news a lot more.

But this is about how much news each company gets, not bias in the usual sense. Our data is stock market news about 7 companies. There are no people in it, so there's nothing like age, gender or location to slice by, and we couldn't find a kind of bias that would actually apply unless we really dug deep looking for flaws. Because of that, the bias detection and mitigation section (3.1 to 3.4) doesn't really apply to our project, and we confirmed with our professor that we can skip it.

> [!NOTE]
> We still keep an eye on how many articles each source and ticker gets. The daily validation alerts us if a source or ticker disappears or a source drops by half (see 2.8). When we pick articles to hand-label for the model, we'll take the same number from each ticker so NVDA doesn't take over.

## 4. Additional guidelines

| Guideline | Where |
|---|---|
| Folder structure | `dags/`, `data/`, `scripts/`, `tests/`, `logs/`, `dvc.yaml` and `README.md` (the README has the full tree under "Project structure") |
| README | `README.md` covers setup, running the pipeline, how the code is organized, DVC and reproducibility |
| Reproducibility | a fixed Python version (`.python-version`), locked dependencies (`uv.lock`), Docker for Airflow, `.env.example`, the schema in git and DVC for the data |
| Code style | small functions that each do one thing, one file per API, shared code in `scripts/helpers/` |
| Error handling | retries with backoff for 429/5xx, one source or ticker failing doesn't stop the rest, a missing API key just skips that source, broken raw lines are skipped and counted, raw files are never overwritten, a broken alert never hides the real error, and Airflow retries failed tasks |
