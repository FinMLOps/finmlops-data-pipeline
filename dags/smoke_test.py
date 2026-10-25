"""Smoke test: Airflow can run the pipeline's Python and see the project. Removed once the real DAGs exist."""

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="smoke_test",
    schedule=None,  # only runs when triggered by hand
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    tags=["test"],
):
    BashOperator(
        task_id="check_pipeline_env",
        bash_command=(
            'cd "$PROJECT_DIR" && "$PIPELINE_PYTHON" -c '
            '"import scripts.ingest, scripts.preprocess, scripts.tfdv_validate; print(\'pipeline env ok\')"'
        ),
    )
