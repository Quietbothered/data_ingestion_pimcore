import pytest 
@pytest.fixture
def ingestion_service(state_store):
    from app.services.json_reader import JsonIngestionService

    service = JsonIngestionService()
    service.state_store = state_store.store
    return service
