import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import json
from kafka import KafkaConsumer
from typing import List, Dict

from scraper.openai_summarizer import OpenAISummarizer
from common.db import PostgresDB


def summarize_and_store_articles():
    consumer = KafkaConsumer(
        "news",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="news_summarizer",
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    now = int(datetime.now(timezone.utc).timestamp())
    recent_articles: List[Dict] = []

    for msg in consumer:
        data = msg.value
        try:
            print((now - data["timestamp"]))
            if (now - data["timestamp"]) <= (12 * 3600):
                recent_articles.append(data)
        except Exception as e:
            continue

    consumer.close()

    recent_articles = sorted(recent_articles, key=lambda x: x["timestamp"], reverse=True)

    summarizer = OpenAISummarizer()
    summarized_articles = []
    for article in recent_articles:
        summary = summarizer.summarize(article["content"])
        if summary:
            article["summary"] = summary
            ts = article["timestamp"]
            article["timestamp"] = datetime.fromtimestamp(ts, tz=timezone.utc)
            summarized_articles.append(article)
            logging.info(f"Summarized article url: {article["url"]}")
            if len(summarized_articles) >= 10:
                break

    if summarized_articles:
        db = PostgresDB()
        db.insert_news(summarized_articles)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="news_summary_pipeline",
    default_args=default_args,
    schedule_interval="0 * * * *",
    catchup=False,
    tags=["news", "openai", "summary"],
) as dag:

    run_summary_pipeline = PythonOperator(
        task_id="summarize_and_store_news",
        python_callable=summarize_and_store_articles,
    )

    run_summary_pipeline
