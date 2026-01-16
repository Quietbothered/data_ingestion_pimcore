import pytest
from httpx import AsyncClient
from fastapi import status
from unittest.mock import patch, MagicMock

from app.main import app
from app.utils.error_messages import ErrorMessages
from tests.conftest import (
    test_info_logger,
    test_error_logger,
    test_debug_logger
)

# ------------------------------------------------------------------
# Helper: Valid base request
# ------------------------------------------------------------------
VALID_REQUEST = {
    "file_path": "tests/data/sample.json",
    "file_type": "json",
    "callback_url": "http://localhost:8001/callback",
    "chunk_size_by_records": 10
}

# ------------------------------------------------------------------
# Missing file_path
# ------------------------------------------------------------------
@pytest.mark.asyncio
async def test_missing_file_path(test_timer):
    payload = VALID_REQUEST.copy()
    payload.pop("file_path")

    async with AsyncClient(app=app, base_url="http://test") as client:
        resp = await client.post("/api/ingest", json=payload)

    elapsed = test_timer()

    test_error_logger.error(
        f"test_missing_file_path | status={resp.status_code} | time={elapsed:.4f}s"
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert ErrorMessages.FILE_URL_IS_NONE.value in resp.text


# ------------------------------------------------------------------
# Missing callback_url
# ------------------------------------------------------------------
@pytest.mark.asyncio
async def test_missing_callback_url(test_timer):
    payload = VALID_REQUEST.copy()
    payload.pop("callback_url")

    async with AsyncClient(app=app, base_url="http://test") as client:
        resp = await client.post("/api/ingest", json=payload)

    elapsed = test_timer()
    test_error_logger.error(
        f"test_missing_callback_url | status={resp.status_code} | time={elapsed:.4f}s"
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert ErrorMessages.CALL_BACK_URL_IS_NONE.value in resp.text


# ------------------------------------------------------------------
# Neither chunk size provided
# ------------------------------------------------------------------
@pytest.mark.asyncio
async def test_no_chunk_size_provided(test_timer):
    payload = VALID_REQUEST.copy()
    payload.pop("chunk_size_by_records")

    async with AsyncClient(app=app, base_url="http://test") as client:
        resp = await client.post("/api/ingest", json=payload)

    elapsed = test_timer()
    test_error_logger.error(
        f"test_no_chunk_size_provided | status={resp.status_code} | time={elapsed:.4f}s"
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert ErrorMessages.NEITHER_CHUNK_SIZE_PROVIDED.value in resp.text


# ------------------------------------------------------------------
# Both chunk sizes provided (conflict)
# ------------------------------------------------------------------
@pytest.mark.asyncio
async def test_both_chunk_sizes_provided(test_timer):
    payload = VALID_REQUEST.copy()
    payload["chunk_size_by_memory"] = 1024

    async with AsyncClient(app=app, base_url="http://test") as client:
        resp = await client.post("/api/ingest", json=payload)

    elapsed = test_timer()
    test_error_logger.error(
        f"test_both_chunk_sizes_provided | status={resp.status_code} | time={elapsed:.4f}s"
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert ErrorMessages.BOTH_CHUNK_SIZES_PROVIDED.value in resp.text


# ------------------------------------------------------------------
# Unsupported file type (controller-level failure)
# ------------------------------------------------------------------
@pytest.mark.asyncio
async def test_unsupported_file_type(test_timer):
    payload = VALID_REQUEST.copy()
    payload["file_type"] = "xml"

    async with AsyncClient(app=app, base_url="http://test") as client:
        resp = await client.post("/api/ingest", json=payload)

    elapsed = test_timer()
    test_error_logger.error(
        f"test_unsupported_file_type | status={resp.status_code} | time={elapsed:.4f}s"
    )

    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert ErrorMessages.INVALID_FILE_TYPE.value in resp.text


# ------------------------------------------------------------------
# Background task failure (JSON ingestion throws)
# ------------------------------------------------------------------
@pytest.mark.asyncio
async def test_background_task_failure(test_timer):
    payload = VALID_REQUEST.copy()

    with patch(
        "app.services.json_reader.JsonIngestionService.stream_and_push",
        side_effect=Exception("Simulated ingestion failure")
    ):
        async with AsyncClient(app=app, base_url="http://test") as client:
            resp = await client.post("/api/ingest", json=payload)

    elapsed = test_timer()

    test_error_logger.error(
        f"test_background_task_failure | status={resp.status_code} | time={elapsed:.4f}s"
    )

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["status"] == "STARTED"


# ------------------------------------------------------------------
# State store DB failure
# ------------------------------------------------------------------
@pytest.mark.asyncio
async def test_state_store_failure(test_timer):
    payload = VALID_REQUEST.copy()

    with patch(
        "app.services.ingestion_state_store.sqlite3.connect",
        side_effect=Exception("DB unavailable")
    ):
        async with AsyncClient(app=app, base_url="http://test") as client:
            resp = await client.post("/api/ingest", json=payload)

    elapsed = test_timer()
    test_error_logger.error(
        f"test_state_store_failure | status={resp.status_code} | time={elapsed:.4f}s"
    )

    assert resp.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR


# ------------------------------------------------------------------
# Unexpected controller exception → 500
# ------------------------------------------------------------------
@pytest.mark.asyncio
async def test_unhandled_controller_exception(test_timer):
    payload = VALID_REQUEST.copy()

    with patch(
        "app.controllers.ingestion_controllers.GenerateFileAndIngestionID.generate_file_id",
        side_effect=Exception("Unexpected crash")
    ):
        async with AsyncClient(app=app, base_url="http://test") as client:
            resp = await client.post("/api/ingest", json=payload)

    elapsed = test_timer()
    test_error_logger.error(
        f"test_unhandled_controller_exception | status={resp.status_code} | time={elapsed:.4f}s"
    )

    assert resp.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
