import pytest
import hashlib
import orjson

from services.chunk_data_integrity_validator import ChunkValidator
from utility.error_messages import ErrorMessages
from tests.conftest import (
    test_info_logger,
    test_error_logger,
    test_debug_logger
)

# ------------------------------------------------------------------
# Helper utilities
# ------------------------------------------------------------------

def checksum(records):
    return hashlib.sha256(
        orjson.dumps(records, option=orjson.OPT_SORT_KEYS)
    ).hexdigest()


INGESTION_ID = "ingestion-123"

# ------------------------------------------------------------------
# Out-of-order chunk
# ------------------------------------------------------------------
def test_out_of_order_chunk(test_timer):
    validator = ChunkValidator()

    records = [{"id": 1}]
    ck = checksum(records)

    # Send chunk 0 first (OK)
    ack, err = validator.validate(
        ingestion_id=INGESTION_ID,
        chunk_id=f"{INGESTION_ID}:0",
        chunk_number=0,
        records=records,
        checksum=ck
    )
    assert ack is True

    # Send chunk 2 (skip chunk 1 → ERROR)
    ack, err = validator.validate(
        ingestion_id=INGESTION_ID,
        chunk_id=f"{INGESTION_ID}:2",
        chunk_number=2,
        records=records,
        checksum=ck
    )

    elapsed = test_timer()
    test_error_logger.error(
        f"test_out_of_order_chunk | ack={ack} | error={err} | time={elapsed:.4f}s"
    )

    assert ack is False
    assert err == ErrorMessages.OUT_OF_ORDER_CHUNK.value


# ------------------------------------------------------------------
# Checksum mismatch
# ------------------------------------------------------------------
def test_checksum_mismatch(test_timer):
    validator = ChunkValidator()

    records = [{"id": 1}]
    wrong_checksum = "deadbeef"

    ack, err = validator.validate(
        ingestion_id=INGESTION_ID,
        chunk_id=f"{INGESTION_ID}:0",
        chunk_number=0,
        records=records,
        checksum=wrong_checksum
    )

    elapsed = test_timer()
    test_error_logger.error(
        f"test_checksum_mismatch | ack={ack} | error={err} | time={elapsed:.4f}s"
    )

    assert ack is False
    assert err == ErrorMessages.CHECKSUM_MISMATCH.value


# ------------------------------------------------------------------
# Duplicate chunk (idempotency)
# ------------------------------------------------------------------
def test_duplicate_chunk(test_timer):
    validator = ChunkValidator()

    records = [{"id": 1}]
    ck = checksum(records)
    chunk_id = f"{INGESTION_ID}:0"

    # First time → normal processing
    ack1, err1 = validator.validate(
        INGESTION_ID, chunk_id, 0, records, ck
    )

    # Duplicate → should be ACKed silently
    ack2, err2 = validator.validate(
        INGESTION_ID, chunk_id, 0, records, ck
    )

    elapsed = test_timer()
    test_info_logger.info(
        f"test_duplicate_chunk | first_ack={ack1} | duplicate_ack={ack2} | time={elapsed:.4f}s"
    )

    assert ack1 is True
    assert ack2 is True
    assert err2 is None


# ------------------------------------------------------------------
# Empty chunk
# ------------------------------------------------------------------
def test_empty_chunk(test_timer):
    validator = ChunkValidator()

    records = []
    ck = checksum(records)

    ack, err = validator.validate(
        ingestion_id=INGESTION_ID,
        chunk_id=f"{INGESTION_ID}:0",
        chunk_number=0,
        records=records,
        checksum=ck
    )

    elapsed = test_timer()
    test_error_logger.error(
        f"test_empty_chunk | ack={ack} | error={err} | time={elapsed:.4f}s"
    )

    assert ack is False
    assert err == ErrorMessages.EMPTY_CHUNK.value


# ------------------------------------------------------------------
# Mixed ingestion IDs (parallel safety)
# ------------------------------------------------------------------
def test_parallel_ingestion_ids(test_timer):
    validator = ChunkValidator()

    records = [{"id": 1}]
    ck = checksum(records)

    ack1, err1 = validator.validate(
        "ing-1", "ing-1:0", 0, records, ck
    )
    ack2, err2 = validator.validate(
        "ing-2", "ing-2:0", 0, records, ck
    )

    elapsed = test_timer()
    test_info_logger.info(
        f"test_parallel_ingestion_ids | ack1={ack1} | ack2={ack2} | time={elapsed:.4f}s"
    )

    assert ack1 is True
    assert ack2 is True


# ------------------------------------------------------------------
# Restart simulation (state loss)
# ------------------------------------------------------------------
def test_restart_state_loss(test_timer):
    validator = ChunkValidator()

    records = [{"id": 1}]
    ck = checksum(records)

    validator.validate(
        INGESTION_ID, f"{INGESTION_ID}:0", 0, records, ck
    )

    # Simulate restart (new instance)
    validator = ChunkValidator()

    # Chunk 1 should FAIL because chunk 0 state is lost
    ack, err = validator.validate(
        INGESTION_ID, f"{INGESTION_ID}:1", 1, records, ck
    )

    elapsed = test_timer()
    test_error_logger.error(
        f"test_restart_state_loss | ack={ack} | error={err} | time={elapsed:.4f}s"
    )

    assert ack is False
    assert err == ErrorMessages.OUT_OF_ORDER_CHUNK.value


# ------------------------------------------------------------------
# Completion ACK should not break state
# ------------------------------------------------------------------
def test_completion_does_not_break_state(test_timer):
    validator = ChunkValidator()

    records = [{"id": 1}]
    ck = checksum(records)

    for i in range(3):
        ack, err = validator.validate(
            INGESTION_ID, f"{INGESTION_ID}:{i}", i, records, ck
        )
        assert ack is True

    elapsed = test_timer()
    test_info_logger.info(
        f"test_completion_does_not_break_state | final_chunk=2 | time={elapsed:.4f}s"
    )
