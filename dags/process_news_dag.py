"""Daily processing: validate yesterday's raw data with TFDV, then rebuild news.parquet."""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

from scripts.alert_utils import airflow_failure_callback

RUN = 'cd "$PROJECT_DIR" && "$PIPELINE_PYTHON" -m '
# dvc and the pipeline's python both live in /opt/venv (the image has no uv or make)
DVC = 'cd "$PROJECT_DIR" && export PATH=/opt/venv/bin:$PATH && '

# Upload the new snapshot to GCS and push the pointer files to GitHub, so the model repo
# can pick it up with `dvc update`. Off unless AUTO_SNAPSHOT=true, because it needs a GCS
# service account and a GitHub token in .env (see docs/PIPELINE.md#automated-snapshots).
# Only runs on main, so it never commits into someone's feature branch.
PUBLISH = """
set -e
cd "$PROJECT_DIR"
export PATH=/opt/venv/bin:$PATH
if [ "$AUTO_SNAPSHOT" != "true" ]; then echo "AUTO_SNAPSHOT is off, skipping"; exit 0; fi
if [ "$(git rev-parse --abbrev-ref HEAD)" != "main" ]; then echo "not on main, skipping"; exit 0; fi
if git diff --quiet -- data/raw.dvc dvc.lock; then echo "data unchanged, nothing to publish"; exit 0; fi
ORIGIN="https://x-access-token:${GITHUB_TOKEN}@github.com/FinMLOps/finmlops-data-pipeline.git"
dvc push
git commit -m "Data snapshot {{ data_interval_start.strftime('%Y-%m-%d') }} (automated)" -- data/raw.dvc dvc.lock
git pull --rebase --autostash "$ORIGIN" main
git push "$ORIGIN" HEAD:main
"""

with DAG(
    dag_id="process_news",
    # 06:30 UTC: ingest runs LAG_HOURS (6) late, so the day's last windows are only
    # fetched at 06:00 the next day; the extra 30 min covers their retries
    schedule=CronDataIntervalTimetable("30 6 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 10, 26, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": airflow_failure_callback,  # email once retries run out
    },
    tags=["process"],
):
    validate = BashOperator(
        task_id="tfdv_validate",
        # the day that just ended (the run at 2026-10-27 06:30 validates 2026-10-26)
        bash_command=RUN
        + "scripts.tfdv_validate {{ data_interval_start.strftime('%Y-%m-%d') }}",
        retries=0,  # anomalies won't go away on a retry
    )
    preprocess = BashOperator(
        task_id="preprocess",
        # record the raw data we're building from, then rebuild news.parquet through DVC,
        # so dvc.lock says exactly which raw data and code produced it
        bash_command=DVC + "dvc add data/raw && dvc repro",
    )
    publish = BashOperator(
        task_id="publish_snapshot",
        bash_command=PUBLISH,
    )

    # only clean the data if it passed validation, and only publish what was cleaned
    validate >> preprocess >> publish
