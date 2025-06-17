from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import logging

from common.db import PostgresDB

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_ohlc_data_and_insert():
    url = "https://api4.binance.com/api/v3/klines"
    params = {
        "symbol": "BTCUSDT",
        "interval": "1d",
        "limit": 1
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    if not data or not data[0]:
        raise ValueError("No OHLC data returned")

    ohlc = data[0]
    ts = int(ohlc[0] // 1000)  # ms → seconds

    db = PostgresDB()
    insert_query = f"""
        INSERT INTO ohlc_1d (timestamp, open, high, low, close, volume)
        VALUES ({ts}, {float(ohlc[1])}, {float(ohlc[2])},
                {float(ohlc[3])}, {float(ohlc[4])}, {float(ohlc[5])})
        ON CONFLICT (timestamp) DO NOTHING;
    """
    db.execute(insert_query)
    logging.info("Inserted OHLC 1d data for timestamp: %s", ts)

with DAG(
    dag_id='ingest_ohlc_1d',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='1 0 * * *',  # 00:01 UTC mỗi ngày
    catchup=False,
    tags=['ohlc', 'binance'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_and_insert_ohlc',
        python_callable=fetch_ohlc_data_and_insert
    )

    fetch_task
