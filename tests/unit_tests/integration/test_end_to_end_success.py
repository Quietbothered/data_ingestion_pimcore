import pytest
from httpx import AsyncClient
from app.main import app
from tests.conftest import test_info_logger

@pytest.mark.asyncio
async def test_end_to_end_success(test_timer):
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.post(
            "/api/ingest",
            json={
            "file_path": "/home/aditya/github/data_ingestion_pimcore/tests/test_data/PIM_PRODIDSKU_20251222183200000_001.json",
            "file_type": "json",
            "callback_url": "http://127.0.0.1:9000/callback",
            "chunk_size_by_records": 10,
            "chunk_size_by_memory": 0,
            "re_ingestion":False
            }
        )

    elapsed = test_timer()
    test_info_logger.info(
        f"END_TO_END_SUCCESS | status={response.status_code} | time={elapsed:.2f}s"
    )

    assert response.status_code == 200
    assert response.json()["status"] == "STARTED"
