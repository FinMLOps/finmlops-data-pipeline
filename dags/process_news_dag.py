"""Daily processing: validate yesterday's raw data with TFDV, then rebuild news.parquet."""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

from scripts.alert_utils import airflow_failure_callback

RUN = 'cd "$PROJECT_DIR" && "$PIPELINE_PYTHON" -m '

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
        bash_command=RUN + "scripts.preprocess",
    )

    validate >> preprocess  # only clean the data if it passed validation
