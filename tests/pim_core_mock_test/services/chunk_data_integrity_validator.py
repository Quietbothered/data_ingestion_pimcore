"""
The code below will validate if the data sent by the fast-api microservice is the exactly the same or not 
"""
import hashlib
import orjson
from typing import Dict, Any, Set

# import error messages
from utility.error_messages import ErrorMessages

from utility.json_decimal_encoder import orjson_default

CANONICAL_OPTS = orjson.OPT_SORT_KEYS

class ChunkValidator:
    def __init__(self):
        self.processed_chunks: Set[str] = set()
        self.last_chunk_number: Dict[str, int] = {}

    def canonical_dumps(self,obj) -> bytes:
        return orjson.dumps(
            obj,
            option=CANONICAL_OPTS,
            default=orjson_default
        )

    def validate(
        self,
        ingestion_id: str,
        chunk_id: str,
        chunk_number: int,
        records,
        checksum: str
    ) -> tuple[bool, str | None]:
        """
        Returns (ack, error_message)
        """

        # Duplicate detection (idempotency)
        if chunk_id in self.processed_chunks:
            return True, None

        # Out-of-order detection
        last = self.last_chunk_number.get(ingestion_id, -1)
        if chunk_number != last + 1:
            return False, ErrorMessages.OUT_OF_ORDER_CHUNK.value

        # Checksum validation
        calculated = hashlib.sha256(
            self.canonical_dumps(records)
        ).hexdigest()

        if calculated != checksum:
            return False, ErrorMessages.CHECKSUM_MISMATCH.value

        # Mark as processed
        self.processed_chunks.add(chunk_id)
        self.last_chunk_number[ingestion_id] = chunk_number

        return True, None
