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
            conn.commit()
            try:
                return [dict(row._mapping) for row in result]
            except Exception:
                return None
