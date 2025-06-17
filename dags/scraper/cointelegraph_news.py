from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from common.kafka_producer import DataKafkaProducer
from scraper.cointelegraph_scraper import CointelegraphScraper

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_scraper():
    scraper = CointelegraphScraper()
    articles = scraper.run(max_articles=5)

    producer = DataKafkaProducer(bootstrap_servers="kafka:9092", topic="news")
    for a in articles:
        producer.send(a)

    producer.flush()

with DAG(
    dag_id="cointelegraph_scraper_dag",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/17 * * * *",
    catchup=False,
    tags=["cointelegraph", "scraper"],
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_cointelegraph_news",
        python_callable=run_scraper,
    )

    scrape_task
