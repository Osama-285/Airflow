from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from confluent_kafka import Consumer, KafkaException


def print_kafka_messages(**context):
    topic = "tansactions"  # fixed typo

    consumer_cfg = {
        'bootstrap.servers': "broker2:29094",
        'group.id': 'test004',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_cfg)

    # ✅ Connection check
    try:
        metadata = consumer.list_topics(timeout=5.0)
        print(f"✅ Connected to Kafka broker: {metadata.brokers}")
    except KafkaException as e:
        print("❌ Cannot connect to Kafka broker:", e)
        consumer.close()
        return

    consumer.subscribe([topic])
    print("📡 Polling Kafka for 2 minutes...")

    end_time = datetime.utcnow().timestamp() + 120  # 2 minutes

    while datetime.utcnow().timestamp() < end_time:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("❌ Kafka error:", msg.error())
            continue

        try:
            value = msg.value().decode("utf-8")
            print(f"➡️ Received message: {value}")
        except Exception as e:
            print("❌ Decode error:", e)

    consumer.close()
    print("✔️ Finished polling Kafka.")


default_args = {
    "owner": "osama",
    "start_date": datetime(2025, 12, 12),
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="kafka_print_messages_manual",
    default_args=default_args,
    schedule=None,   # MANUAL TRIGGER ONLY
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="print_kafka_messages_task",
        python_callable=print_kafka_messages,
    )
