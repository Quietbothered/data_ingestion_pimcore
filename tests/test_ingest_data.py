import pytest
import requests


def test_ingestion_api_smoke():
    # This assumes your app is already running locally
    response = requests.post(
        "http://localhost:8000/api/ingest",
        json={
            "file_type": "excel",
            "file_path": "tests/resources/sample.xlsx",
            "chunk_size_by_records": 10,
            "callback_url": "http://localhost:9000/callback"
        }
    )

    assert response.status_code == 200
    assert "ingestion_id" in response.json()
