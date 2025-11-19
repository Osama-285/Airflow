from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='run_flink_py_job',
    default_args=default_args,
    description='Run a PyFlink job from Airflow using network mode',
    schedule=None,  # manual trigger
    catchup=False,
    tags=['flink', 'pyflink'],
) as dag:

    run_flink_job = BashOperator(
        task_id='submit_flink_job',
        bash_command='flink run -py /opt/flink/jobs/sla2.py -m flink-jobmanager:8087'
    )

    run_flink_job
