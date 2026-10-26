"""Ingestion DAGs: one per news source, each fetching its own time window, a few hours late."""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

from scripts.alert_utils import airflow_failure_callback

SCHEDULES = {
    "finnhub": "@hourly",  # runs every hr
    "marketaux": "0 */6 * * *",  # runs every 6 hrs
}
# The APIs make articles available hours after they're published, so each run fetches
# the window that ended LAG_HOURS ago (the 12:00 Finnhub run fetches 05:00-06:00).
# Fetching the window that just ended got ~8% of the articles (26 vs 338 on 26 Oct).
LAG_HOURS = 6
SINCE = "{{ (data_interval_start - macros.timedelta(hours=" + str(LAG_HOURS) + ")).isoformat() }}"
UNTIL = "{{ (data_interval_end - macros.timedelta(hours=" + str(LAG_HOURS) + ")).isoformat() }}"

for source, cron in SCHEDULES.items():
    with DAG(
        dag_id=f"ingest_{source}",
        # gives each run a [start, end) window to fetch; Airflow 3's default cron timetable doesn't
        schedule=CronDataIntervalTimetable(cron, timezone="UTC"),
        start_date=pendulum.datetime(2026, 10, 26, tz="UTC"),
        catchup=True,  # after the laptop sleeps, run the windows it missed
        max_active_runs=1,
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "on_failure_callback": airflow_failure_callback,  # email once retries run out
        },
        tags=["ingest", source],
    ):
        BashOperator(
            task_id="ingest",
            bash_command=(
                'cd "$PROJECT_DIR" && "$PIPELINE_PYTHON" -m scripts.ingest --source '
                + source
                + " --since " + SINCE
                + " --until " + UNTIL
            ),
        )
