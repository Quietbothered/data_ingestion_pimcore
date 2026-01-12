"""
This file is responsible for making sure that pim-core callback url gets the exactly the same chunk with zero data loss
[PREVENTS]
- Partial trasmission of chunks over the network
- Proxy/body truncation 
- Corrupted JSON
- Duplicate chunk retry
- Out-of-order chunk
"""
import hashlib
import orjson
from typing import List, Dict, Any

# ort json parser
from app.utils.json_decimal_encoder import orjson_default

# import logging utility
from app.utils.logger import LoggerFactory

# initialize logging utility
info_logger = LoggerFactory.get_info_logger()
error_logger = LoggerFactory.get_error_logger()
debug_logger = LoggerFactory.get_debug_logger()

CANONICAL_OPTS = orjson.OPT_SORT_KEYS

class ChunkIntegrityManager:
    @staticmethod
    def canonical_dumps(obj) -> bytes:
        dump = orjson.dumps(
            obj,
            option=CANONICAL_OPTS,
            default=orjson_default
        )
        debug_logger.debug(f"ChunkIntegrityManager.canonical_dumps | dump = {dump}")
        return dump 
    
    @staticmethod
    def compute_checksum(records: List[Dict[str, Any]]) -> str:
        """
        Computes deterministic checksum for a chunk.
        """
        payload_bytes = ChunkIntegrityManager.canonical_dumps(records)
        checksum = hashlib.sha256(payload_bytes).hexdigest()
        debug_logger.debug(f"ChunkIntegrityManager.compute_checksum | chunk checksum value = {checksum}")
        return checksum

    @staticmethod
    def build_chunk_id(ingestion_id: str, chunk_number: int) -> str:
        """
        Unique identity for a chunk.
        """
        debug_logger.debug(f"ChunkIntegrityManager.build_chunk_id | generated chunk_id = {ingestion_id}:{chunk_number}")
        return f"{ingestion_id}:{chunk_number}"
