from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import time
from common.db import PostgresDB

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def compute_btc_monthly_return():
    db = PostgresDB()
    now_ts = int(time.time() * 1000)

    current_row = db.execute("""
        SELECT timestamp, close
        FROM ohlc_1d
        WHERE timestamp < %s
        ORDER BY timestamp DESC
        LIMIT 1
    """ % now_ts)

    if not current_row:
        raise ValueError("No OHLC data before now")

    current_ts = current_row[0]['timestamp']
    close_now = current_row[0]['close']

    ts_dt = datetime.utcfromtimestamp(current_ts / 1000)
    ts_month_start = datetime(ts_dt.year, ts_dt.month, 1)
    ts_month_start_ms = int(ts_month_start.timestamp() * 1000)

    prev_row = db.execute("""
        SELECT timestamp, close
        FROM ohlc_1d
        WHERE timestamp < %s
        ORDER BY timestamp DESC
        LIMIT 1
    """ % ts_month_start_ms)

    if not prev_row:
        raise ValueError("No OHLC data before month start")

    close_prev = prev_row[0]['close']

    return_percent = round(((close_now - close_prev) / close_prev) * 100, 2)
    year = ts_dt.year
    month = ts_dt.month

    insert_query = f"""
        INSERT INTO btc_monthly_returns (year, month, return_percent)
        VALUES ({year}, {month}, {return_percent})
        ON CONFLICT (year, month)
        DO UPDATE SET return_percent = EXCLUDED.return_percent;
    """
    db.execute(insert_query)

    logging.info(f"[BTC Monthly Return] {year}-{month}: {return_percent:.2f}%")

with DAG(
    dag_id="btc_monthly_return_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="59 23 * * *",
    default_args=default_args,
    catchup=False,
    tags=["btc", "return", "ohlc"]
) as dag:
    calculate_return = PythonOperator(
        task_id="compute_btc_monthly_return",
        python_callable=compute_btc_monthly_return,
    )

    calculate_return
