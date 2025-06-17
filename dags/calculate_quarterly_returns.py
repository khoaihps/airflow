from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import time
from common.db import PostgresDB

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def get_quarter_start(dt: datetime) -> datetime:
    quarter = (dt.month - 1) // 3 + 1
    return datetime(dt.year, 3 * (quarter - 1) + 1, 1)

def compute_btc_quarterly_return():
    db = PostgresDB()
    now_ts = int(time.time() * 1000)

    current_row = db.execute(f"""
        SELECT timestamp, close
        FROM ohlc_1d
        WHERE timestamp < {now_ts}
        ORDER BY timestamp DESC
        LIMIT 1
    """)

    if not current_row:
        raise ValueError("No OHLC data before now")

    current_ts = current_row[0]['timestamp']
    close_now = current_row[0]['close']
    ts_dt = datetime.utcfromtimestamp(current_ts / 1000)

    ts_quarter_start = get_quarter_start(ts_dt)
    ts_quarter_start_ms = int(ts_quarter_start.timestamp() * 1000)

    prev_row = db.execute(f"""
        SELECT timestamp, close
        FROM ohlc_1d
        WHERE timestamp < {ts_quarter_start_ms}
        ORDER BY timestamp DESC
        LIMIT 1
    """)

    if not prev_row:
        raise ValueError("No OHLC data before quarter start")

    close_prev = prev_row[0]['close']

    return_percent = round(((close_now - close_prev) / close_prev) * 100, 2)
    year = ts_dt.year
    quarter = f"Q{(ts_dt.month - 1) // 3 + 1}"

    insert_query = f"""
        INSERT INTO btc_quarterly_returns (year, quarter, return_percent)
        VALUES ({year}, '{quarter}', {return_percent})
        ON CONFLICT (year, quarter)
        DO UPDATE SET return_percent = EXCLUDED.return_percent;
    """
    db.execute(insert_query)

    logging.info(f"[BTC Quarterly Return] {year}-{quarter}: {return_percent:.2f}%")

with DAG(
    dag_id="btc_quarterly_return_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="59 23 * * *",
    default_args=default_args,
    catchup=False,
    tags=["btc", "return", "ohlc", "quarter"]
) as dag:
    calculate_return = PythonOperator(
        task_id="compute_btc_quarterly_return",
        python_callable=compute_btc_quarterly_return,
    )

    calculate_return
