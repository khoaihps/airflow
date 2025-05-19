from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def produce_message(**context):
    import os
    import boto3
    import json
    import pandas as pd
    from io import BytesIO
    from botocore import UNSIGNED
    from botocore.config import Config
    from kafka import KafkaProducer

    date = context["ds"]
    s3_path = f"aws-public-blockchain/v1.0/btc/transactions/date={date}/"
    local_cache_file = "/opt/airflow/processed_files.txt"

    producer = KafkaProducer(
        bootstrap_servers='127.1.27.1:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    bucket = "aws-public-blockchain"

    if not os.path.exists(local_cache_file):
        open(local_cache_file, 'w').close()

    with open(local_cache_file, 'r+') as f:
        processed = set(line.strip() for line in f.readlines())

        result = s3.list_objects_v2(Bucket=bucket, Prefix=s3_path)
        if 'Contents' not in result:
            return

        for obj in result['Contents']:
            key = obj['Key']
            if not key.endswith('.parquet') or key in processed:
                continue

            response = s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(BytesIO(response['Body'].read()))

            for _, row in df.iterrows():
                tx_hash = row.get("hash")
                block_number = row.get("block_number")
                timestamp = row.get("block_timestamp")
                inputs = row.get("inputs", [])
                outputs = row.get("outputs", [])

                input_address = None
                if isinstance(inputs, list) and len(inputs) > 1:
                    input_address = inputs[1].get("address")

                for out in outputs:
                    output_address = out.get("address")
                    value = out.get("value")

                    message = {
                        "hash": tx_hash,
                        "block_number": block_number,
                        "timestamp": timestamp,
                        "input_address": input_address,
                        "output_address": output_address,
                        "value": value
                    }

                    producer.send("btc_transactions", value=message)

            f.write(f"{key}\n")
            f.flush()

    producer.flush()

# DAG Definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="btc_to_kafka",
    default_args=default_args,
    description="Stream BTC tx data from S3 to Kafka",
    schedule_interval="*/10 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["btc", "kafka"],
) as dag:

    stream_tx_to_kafka = PythonOperator(
        task_id="stream_tx_to_kafka",
        python_callable=produce_message,
        provide_context=True,
    )
