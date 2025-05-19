from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import os
import boto3
import pandas as pd
import numpy as np
import logging
from io import BytesIO
from botocore import UNSIGNED
from botocore.config import Config

from common.kafka_producer import DataKafkaProducer

def produce_message(ds, **context):
    logging.info(f"Execution date (ds): {ds}")

    s3_path = f"v1.0/btc/transactions/date={ds}/"
    local_cache_file = "/opt/airflow/processed_files.txt"

    producer = DataKafkaProducer(topic="btc_transactions")

    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    bucket = "aws-public-blockchain"

    if not os.path.exists(local_cache_file):
        open(local_cache_file, 'w').close()

    with open(local_cache_file, 'r+') as f:
        processed = set(line.strip() for line in f.readlines())
        logging.info(f"Found {len(processed)} processed files in cache.")

        result = s3.list_objects_v2(Bucket=bucket, Prefix=s3_path)
        if 'Contents' not in result:
            logging.warning(f"No files found at prefix: {s3_path}")
            return

        files_processed = 0
        for obj in result['Contents']:
            key = obj['Key']
            if not key.endswith('.parquet') or key in processed:
                logging.debug(f"Skipping already processed or invalid file: {key}")
                continue

            logging.info(f"Processing file: {key}")
            try:
                response = s3.get_object(Bucket=bucket, Key=key)
                df = pd.read_parquet(BytesIO(response['Body'].read()))
                logging.info(f"Read {len(df)} rows from {key}")

                msg_count = 0
                for _, row in df.iterrows():
                    tx_hash = row.get("hash")
                    block_number = row.get("block_number")
                    raw_ts = row.get("block_timestamp")
                    timestamp = raw_ts.isoformat() if pd.notnull(raw_ts) else None
                    inputs = row.get("inputs", [])
                    outputs = row.get("outputs", [])

                    if isinstance(inputs, np.ndarray) and inputs.size > 0:
                        inputs_list = inputs.tolist()
                        input_address = inputs_list[0].get("address")
                    else:
                        continue

                    for out in outputs:
                        output_address = out.get("address")
                        value = out.get("value")

                        if not (input_address and output_address):
                            continue
                            
                        message = {
                            "hash": tx_hash,
                            "block_number": block_number,
                            "timestamp": timestamp,
                            "input_address": input_address,
                            "output_address": output_address,
                            "value": value
                        }

                        producer.send(message)
                        msg_count += 1

                logging.info(f"Sent {msg_count} messages to Kafka from {key}")
                f.write(f"{key}\n")
                f.flush()
                files_processed += 1

            except Exception as e:
                logging.error(f"Error processing {key}: {str(e)}")

    producer.flush()
    logging.info(f"Completed streaming. Total new files processed: {files_processed}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="btc_tx_to_kafka",
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
