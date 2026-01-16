import sqlite3
from typing import Optional
import os
from pathlib import Path 

# import get current project's directory utility
from app.utils.get_project_dir import get_current_project_dir

PROJECT_DIR = Path(get_current_project_dir()).parent
DATABASE_DIR = os.path.join(PROJECT_DIR , "ingestion_state_data", "ingestion_state.db")

class IngestionStateStore:
    def __init__(self, db_path=DATABASE_DIR):
        # Ensure parent directory exists
        """
        This will create the directory if it does not exists
        """
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init()

    def _init(self):
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_state (
            ingestion_id TEXT PRIMARY KEY,
            last_chunk INTEGER,
            total_records INTEGER,
            status TEXT
        )
        """)
        self.conn.commit()

    def get_last_chunk(self, ingestion_id: str) -> int:
        cur = self.conn.execute(
            "SELECT last_chunk FROM ingestion_state WHERE ingestion_id=?",
            (ingestion_id,)
        )
        row = cur.fetchone()
        return row[0] if row else -1
    
    def get_total_records(self, ingestion_id):
        cur = self.conn.execute(
            "SELECT total_records FROM ingestion_state WHERE ingestion_id=?",
            (ingestion_id,)
        )
        row = cur.fetchone()
        return row[0] if row else 0

    def update_chunk(self, ingestion_id, chunk_number, total_records):
        self.conn.execute("""
        INSERT INTO ingestion_state (ingestion_id, last_chunk, total_records, status)
        VALUES (?, ?, ?, 'IN_PROGRESS')
        ON CONFLICT(ingestion_id)
        DO UPDATE SET
            last_chunk=excluded.last_chunk,
            total_records=excluded.total_records
        """, (ingestion_id, chunk_number, total_records))
        self.conn.commit()

    def mark_completed(self, ingestion_id: str):
        self.conn.execute(
            "UPDATE ingestion_state SET status='COMPLETED' WHERE ingestion_id=?",
            (ingestion_id,)
        )
        self.conn.commit()
