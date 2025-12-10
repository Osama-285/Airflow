from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "retries": 0,
}

# DAG definition
with DAG(
    dag_id="02_no_slots",
    description="DAG to reproduce 'No worker slots available' issue in CeleryExecutor.",
    start_date=datetime(2025, 12, 12),
    schedule="@once",
    catchup=False,
    default_args=default_args,
) as dag:

    # Create 20 long-running tasks
    for i in range(20):
        BashOperator(
            task_id=f"long_task_{i}",
            bash_command="sleep 300",  # Simulates heavy tasks
        )
