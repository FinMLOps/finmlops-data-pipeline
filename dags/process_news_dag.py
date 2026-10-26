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
    # 00:30 UTC: the day is over and the midnight ingest runs have had time to finish
    schedule=CronDataIntervalTimetable("30 0 * * *", timezone="UTC"),
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
        # the day that just ended (the run at 2026-10-26 00:30 validates 2026-10-25)
        bash_command=RUN
        + "scripts.tfdv_validate {{ data_interval_start.strftime('%Y-%m-%d') }}",
        retries=0,  # anomalies won't go away on a retry
    )
    preprocess = BashOperator(
        task_id="preprocess",
        bash_command=RUN + "scripts.preprocess",
    )

    validate >> preprocess  # only clean the data if it passed validation
