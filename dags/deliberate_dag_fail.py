from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from scripts.alert_utils import airflow_failure_callback

with DAG(
    dag_id="alert_test",
    schedule=None,  # only runs when triggered by hand
    default_args={"on_failure_callback": airflow_failure_callback},
):
    BashOperator(
        task_id="deliberate_failure",
        bash_command='echo "simulating a broken task"; exit 1',
    )

