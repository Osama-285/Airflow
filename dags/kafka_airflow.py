from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Consumer
import json
import pandas as pd
import time
import os


def read_kafka_and_write_csv(**context):
    topic = "transactions"
    output_dir = "/opt/airflow/output"

    os.makedirs(output_dir, exist_ok=True)

    filename = f"{output_dir}/transactions_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"

    consumer_cfg = {
        "bootstrap.servers": "broker2:29094",
        "group.id": "airflow-consumer",
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(consumer_cfg)
    consumer.subscribe([topic])

    end_time = time.time() + 120
    records = []

    print(f"Collecting Kafka messages for 2 minutes... writing to {filename}")

    while time.time() < end_time:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            print("Kafka error:", msg.error())
            continue

        try:
            obj = json.loads(msg.value().decode("utf-8"))
            records.append(obj)
        except Exception as e:
            print("Error parsing message:", e)

    consumer.close()

    if records:
        df = pd.DataFrame(records)
        df.to_csv(filename, index=False)
        print(f"Saved {len(records)} messages to: {filename}")
    else:
        print("No messages received during this window.")


default_args = {
    "owner": "osama",
    "start_date": datetime(2025, 12, 3),
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="kafka_to_csv_pandas_every_2_minutes",
    default_args=default_args,
    schedule="*/2 * * * *",  # UPDATED
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="read_kafka_and_write_csv",
        python_callable=read_kafka_and_write_csv,
    )
