# FinMLOps Data Pipeline

[![pytest](https://github.com/FinMLOps/finmlops-data-pipeline/actions/workflows/pytest.yml/badge.svg?branch=main)](https://github.com/FinMLOps/finmlops-data-pipeline/actions/workflows/pytest.yml)

An end-to-end data pipeline in DAG format covering data acquisition, preprocessing, testing, data versioning, and workflow management.

> [!IMPORTANT]
> - We use [`uv`](https://docs.astral.sh/uv/), which is superior to `pip`.
> - Python is pinned to `3.11` (`>=3.11,<3.12`) to ensure compatibility and stability across pipeline dependencies.

## Setup

Install dependencies:
```bash
uv sync
```

Configure environment:
```bash
cp .env.example .env
```

Required keys in `.env`:
```text
MARKETAUX_API_KEY=your_marketaux_api_key
FINNHUB_API_KEY=your_finnhub_api_key
```

## Tests

Run all unit tests:
```bash
uv run pytest
```

Run individual test files:
```bash
uv run pytest tests/test_schemas.py
uv run pytest tests/test_finnhub.py
uv run pytest tests/test_marketaux.py
```

## Project Structure

```text
finmlops-data-pipeline/
├── .github/
│   └── workflows/
│       ├── pytest.yml        # Automated unit test suite
│       └── claude.yml        # Claude PR review assistant
├── dags/
│   └── .gitkeep              # Airflow DAGs
├── data/
│   └── .gitkeep              # Data storage
├── logs/
│   └── .gitkeep              # Execution logs
├── scripts/
│   ├── fetch_finnhub.py      # Finnhub source
│   ├── fetch_marketaux.py    # Marketaux source
│   └── helpers/
│       ├── __init__.py       # Exports
│       ├── base.py           # Base provider
│       └── schemas.py        # Data schemas
├── tests/
│   ├── test_finnhub.py       # Finnhub tests
│   ├── test_marketaux.py     # Marketaux tests
│   └── test_schemas.py       # Schema tests
├── .env.example              # .env template
├── pyproject.toml            # Project config
├── uv.lock                   # Dependency lockfile
└── README.md                 # This document ;)
```
