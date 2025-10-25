from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, KafkaException
import os
import json

# ----------- CONFIG -----------
KAFKA_BOOTSTRAP = "broker:29094"   # container name or alias on flink-net
TOPIC_NAME = "shipEvent"
CHUNK_SIZE = 500
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)
# ------------------------------

def consume_ship_events(**context):
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': 'airflow-consumer-group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_NAME])

    print(f"Connected to Kafka topic '{TOPIC_NAME}' at {KAFKA_BOOTSTRAP}")

    buffer = []
    file_index = 1
    total_messages = 0

    try:
        while True:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                print("Waiting for messages...")
                continue
            if msg.error():
                raise KafkaException(msg.error())

            message_value = msg.value().decode('utf-8')
            buffer.append(message_value)
            total_messages += 1

            if len(buffer) == CHUNK_SIZE:
                output_file = os.path.join(OUTPUT_DIR, f"ship_events_chunk_{file_index}.txt")

                with open(output_file, "w", encoding="utf-8") as f:
                    for event in buffer:
                        f.write(event + "\n")

                print(f"✅ Wrote {CHUNK_SIZE} messages to {output_file}")
                buffer.clear()
                file_index += 1

    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        # write remaining messages if any
        if buffer:
            output_file = os.path.join(OUTPUT_DIR, f"ship_events_chunk_{file_index}.txt")
            with open(output_file, "w", encoding="utf-8") as f:
                for event in buffer:
                    f.write(event + "\n")
            print(f"✅ Wrote remaining {len(buffer)} messages to {output_file}")
        consumer.close()
        print(f"✅ Total messages consumed: {total_messages}")

default_args = {
    'owner': 'osama',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 24),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'kafka_ship_event_consumer',
    default_args=default_args,
    description='Consume Kafka topic shipEvent in 500-message chunks and write to output files',
    schedule=None,
    catchup=False
)

consume_task = PythonOperator(
    task_id='consume_ship_event_chunks',
    python_callable=consume_ship_events,
    dag=dag,
)
