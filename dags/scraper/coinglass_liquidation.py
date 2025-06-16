from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
import logging

from scraper.coinglass_scraper import CoinglassScraper
from common.db import PostgresDB

URL = "https://www.coinglass.com/LiquidationData"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

def scrape_and_save_liquidation():
    scraper = CoinglassScraper(URL)
    data = scraper.scrape()

    if not data:
        logging.warning("❌ No data scraped.")
        return

    db = PostgresDB()
    now = int(time.time())

    for type_, value in data.items():
        query = f"""
        INSERT INTO liquidation_1h (timestamp, type, value)
        VALUES ({now}, '{type_}', {value});
        """
        db.execute(query)
        logging.info(f"✅ Inserted {type_} → {value} at {now}")

with DAG(
    dag_id="scrape_coinglass_liquidation",
    schedule_interval="0 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["scraper", "coinglass"],
) as dag:

    run_scraper = PythonOperator(
        task_id="scrape_and_store_liquidation",
        python_callable=scrape_and_save_liquidation,
    )
