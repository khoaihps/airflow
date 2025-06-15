from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import List, Dict, Optional
import os

class PostgresDB:
    def __init__(self):
        self.url = f"postgresql+psycopg2://postgres:{os.getenv("POSTGRES_PASSWORD")}@postgresql:5432/public"
        self.engine: Engine = create_engine(self.url)

    def execute(self, query: str) -> Optional[List[Dict]]:
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            try:
                return [dict(row._mapping) for row in result]
            except Exception:
                return None

    def execute_many(self, query: str, values: List[Dict]):
        with self.engine.connect() as conn:
            conn.execute(text(query), values)

    def insert_news(self, articles: List[Dict]):
        insert_query = """
        INSERT INTO news (url, title, content, timestamp, summary)
        VALUES (:url, :title, :content, :timestamp, :summary)
        ON CONFLICT (url) DO UPDATE
        SET title = EXCLUDED.title,
            content = EXCLUDED.content,
            timestamp = EXCLUDED.timestamp,
            summary = EXCLUDED.summary
        """
        records = [
            {
                "url": a["url"],
                "title": a["title"],
                "content": a["content"],
                "timestamp": a["timestamp"],
                "summary": a["summary"],
            }
            for a in articles
        ]
        self.execute_many(insert_query, records)