import pytest
import os
import shutil
import tempfile
import asyncio
from unittest.mock import patch, AsyncMock

from app.services.ingestion_state_store import IngestionStateStore
from app.services.json_reader import JsonIngestionService
from tests.conftest import (
    test_info_logger,
    test_error_logger,
    test_debug_logger
)

# ------------------------------------------------------------------
# Helper fixtures
# ------------------------------------------------------------------

@pytest.fixture
def temp_db_dir():
    tmp = tempfile.mkdtemp()
    yield tmp
    shutil.rmtree(tmp)


@pytest.fixture
def mock_request():
    class MockRequest:
        file_path = "tests/data/sample.json"
        file_type = "json"
        callback_url = "http://pim-core/callback"
        chunk_size_by_records = 2
        chunk_size_by_memory = None
        re_ingestion = False

    return MockRequest()


# ------------------------------------------------------------------
# Resume from last ACKed chunk
# ------------------------------------------------------------------
@pytest.mark.asyncio
async def test_resume_from_last_chunk(temp_db_dir, mock_request, test_timer):
    db_path = os.path.join(temp_db_dir, "state.db")
    store = IngestionStateStore(db_path=db_path)

    ingestion_id = "ing-123"

    # Simulate ACKed chunks 0,1
    store.update_chunk(ingestion_id, 0, 2)
    store.update_chunk(ingestion_id, 1, 4)

    service = JsonIngestionService()
    service.state_store = store

    with patch.object(
        service,
        "_send_chunk",
        new=AsyncMock()
    ) as mock_send:
        await service.stream_and_push(ingestion_id, mock_request)

    elapsed = test_timer()
    test_info_logger.info(
        f"test_resume_from_last_chunk | resumed_from=2 | time={elapsed:.4f}s"
    )

    # Ensure chunk 0 & 1 NOT resent
    sent_chunks = [
        call.args[3] for call in mock_send.call_args_list
    ]
    assert all(chunk > 1 for chunk in sent_chunks)


# ------------------------------------------------------------------
# Resume skips already ACKed chunks
# ------------------------------------------------------------------
@pytest.mark.asyncio
async def test_resume_skips_acked_chunks(temp_db_dir, mock_request, test_timer):
    db_path = os.path.join(temp_db_dir, "state.db")
    store = IngestionStateStore(db_path=db_path)

    ingestion_id = "ing-456"

    store.update_chunk(ingestion_id, 0, 2)
    store.update_chunk(ingestion_id, 1, 4)
    store.update_chunk(ingestion_id, 2, 6)

    service = JsonIngestionService()
    service.state_store = store

    with patch.object(
        service,
        "_send_chunk",
        new=AsyncMock()
    ) as mock_send:
        await service.stream_and_push(ingestion_id, mock_request)

    elapsed = test_timer()
    test_info_logger.info(
        f"test_resume_skips_acked_chunks | last_acked=2 | time={elapsed:.4f}s"
    )

    for call in mock_send.call_args_list:
        assert call.args[3] > 2


# ------------------------------------------------------------------
# Container restart (new service instance)
# ------------------------------------------------------------------
@pytest.mark.asyncio
async def test_resume_after_container_restart(temp_db_dir, mock_request, test_timer):
    db_path = os.path.join(temp_db_dir, "state.db")
    ingestion_id = "ing-restart"

    # First "container"
    store1 = IngestionStateStore(db_path=db_path)
    store1.update_chunk(ingestion_id, 0, 2)
    store1.update_chunk(ingestion_id, 1, 4)

    # Second "container"
    store2 = IngestionStateStore(db_path=db_path)

    service = JsonIngestionService()
    service.state_store = store2

    with patch.object(
        service,
        "_send_chunk",
        new=AsyncMock()
    ) as mock_send:
        await service.stream_and_push(ingestion_id, mock_request)

    elapsed = test_timer()
    test_info_logger.info(
        f"test_resume_after_container_restart | resumed=True | time={elapsed:.4f}s"
    )

    sent_chunks = [call.args[3] for call in mock_send.call_args_list]
    assert min(sent_chunks) >= 2


# ------------------------------------------------------------------
# Restart mid-ingestion (partial progress)
# ------------------------------------------------------------------
@pytest.mark.asyncio
async def test_restart_mid_ingestion(temp_db_dir, mock_request, test_timer):
    db_path = os.path.join(temp_db_dir, "state.db")
    ingestion_id = "ing-mid"

    store = IngestionStateStore(db_path=db_path)
    store.update_chunk(ingestion_id, 0, 2)

    service = JsonIngestionService()
    service.state_store = store

    with patch.object(
        service,
        "_send_chunk",
        new=AsyncMock()
    ) as mock_send:
        await service.stream_and_push(ingestion_id, mock_request)

    elapsed = test_timer()
    test_info_logger.info(
        f"test_restart_mid_ingestion | resumed_from=1 | time={elapsed:.4f}s"
    )

    for call in mock_send.call_args_list:
        assert call.args[3] > 0


# ------------------------------------------------------------------
# Resume + completion marks ingestion COMPLETED
# ------------------------------------------------------------------
@pytest.mark.asyncio
async def test_resume_and_complete(temp_db_dir, mock_request, test_timer):
    db_path = os.path.join(temp_db_dir, "state.db")
    ingestion_id = "ing-complete"

    store = IngestionStateStore(db_path=db_path)
    store.update_chunk(ingestion_id, 0, 2)
    store.update_chunk(ingestion_id, 1, 4)

    service = JsonIngestionService()
    service.state_store = store

    with patch.object(
        service,
        "_send_chunk",
        new=AsyncMock()
    ):
        with patch(
            "httpx.AsyncClient.post",
            new=AsyncMock(return_value=AsyncMock(json=lambda: {"ack": True}))
        ):
            await service.stream_and_push(ingestion_id, mock_request)

    elapsed = test_timer()
    test_info_logger.info(
        f"test_resume_and_complete | status=COMPLETED | time={elapsed:.4f}s"
    )

    cur = store.conn.execute(
        "SELECT status FROM ingestion_state WHERE ingestion_id=?",
        (ingestion_id,)
    )
    status = cur.fetchone()[0]
    assert status == "COMPLETED"
