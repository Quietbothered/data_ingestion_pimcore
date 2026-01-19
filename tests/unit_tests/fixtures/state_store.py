import pytest
from app.services.ingestion_state_store import IngestionStateStore

class StateStoreFixture:
    def __init__(self, db_path):
        self.store = IngestionStateStore(db_path=str(db_path))

    def ack_chunk(self, ingestion_id, chunk, total):
        self.store.update_chunk(ingestion_id, chunk, total)

    def mark_completed(self, ingestion_id):
        self.store.mark_completed(ingestion_id)

    def last_chunk(self, ingestion_id):
        return self.store.get_last_chunk(ingestion_id)

@pytest.fixture
def state_store(tmp_path):
    db_path = tmp_path / "ingestion.db"
    return StateStoreFixture(db_path)
