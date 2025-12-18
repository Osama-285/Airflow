from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

with DAG(
    dag_id="run_flink_job",
    start_date=datetime(2025, 12, 18),
    schedule=None,
    catchup=False
) as dag:

    submit_flink_job = DockerOperator(
        task_id="submit_flink_job",
        image="flink-jobmanager",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="main-bridge",
        mount_tmp_dir=False,
        command="""
        flink run \
          -py \
          /opt/flink/jobs/fraud_detection_2.0.py
        """,
        tty=True,
        do_xcom_push=False,
    )

    submit_flink_job
