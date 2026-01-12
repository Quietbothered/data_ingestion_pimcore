import ijson
import fsspec

# ort json parser
from app.utils.json_decimal_encoder import orjson_default

# import data integrity manager
from app.services.data_integrity_manager import ChunkIntegrityManager

# import error messages
from app.utils.error_messages import ErrorMessages

import httpx
import orjson

# import logging utility
from app.utils.logger import LoggerFactory

# initialize logging utility
info_logger = LoggerFactory.get_info_logger()
error_logger = LoggerFactory.get_error_logger()
debug_logger = LoggerFactory.get_debug_logger()

class JsonIngestionService:
    """
    This method will read json using read stream method
    """
    async def stream_and_push(self, ingestion_id: str, request):
        fs, _, paths = fsspec.get_fs_token_paths(request.file_path)
        debug_logger.debug(f"JsonIngestionService.stream_and_push | file_system={fs} | paths = {paths}")

        chunk = []
        chunk_bytes = 0
        chunk_number = 0
        total_records = 0

        async with httpx.AsyncClient(timeout=60) as client:
            for base_path in paths:
                files = (
                    fs.glob(f"{base_path.rstrip('/')}/**/*.json")
                    if fs.isdir(base_path)
                    else [base_path]
                )

                for file in files:
                    debug_logger.debug(f"JsonIngestionService.stream_and_push | Processing file = {file}")
                    with fs.open(file, "rb") as f:
                        for record in ijson.items(f, "item"):
                            record_bytes = len(orjson.dumps(record, default=orjson_default)

)

                            if self._should_flush(
                                request,
                                chunk,
                                chunk_bytes,
                                record_bytes
                            ):
                                await self._send_chunk(
                                    client,
                                    request.callback_url,
                                    ingestion_id,
                                    chunk_number,
                                    chunk,
                                    False
                                )
                                chunk_number += 1
                                chunk.clear()
                                chunk_bytes = 0

                            chunk.append(record)
                            chunk_bytes += record_bytes
                            total_records += 1

            # Final chunk
            if chunk:
                debug_logger.debug(f"JsonIngestionService.stream_and_push | Processing chunks | ingestion_id = {ingestion_id} | chunk_number = {chunk_number}")
                await self._send_chunk(
                    client,
                    request.callback_url,
                    ingestion_id,
                    chunk_number,
                    chunk,
                    True
                )

            # Completion event
            debug_logger.debug(f"JsonIngestionService.stream_and_push | Processed and completed all the chunks | ingestion_id = {ingestion_id} | chunk_number = {chunk_number} | total_records = {total_records} | status = COMPLETED")
            await client.post(
                request.callback_url,
                json={
                    "ingestion_id": ingestion_id,
                    "status": "COMPLETED",
                    "chunk_number":chunk_number,
                    "total_records": total_records
                }
            )

    def _should_flush(self, request, chunk, chunk_bytes, next_record_bytes):
        should_flush = (
            len(chunk) >= request.chunk_size_by_records
            if request.chunk_size_by_records
            else (chunk_bytes + next_record_bytes) > request.chunk_size_by_memory
        )
        debug_logger.debug(f"JsonIngestionService._should_flush | should_flush={should_flush}")
        return should_flush

    async def _send_chunk(
        self,
        client,
        url,
        ingestion_id,
        chunk_number,
        records,
        is_last
    ):
        # Data integrity check related logic (checksum mechanism)
        checksum = ChunkIntegrityManager.compute_checksum(records)
        chunk_id = ChunkIntegrityManager.build_chunk_id(
            ingestion_id, chunk_number
        )


        payload = {
            "ingestion_id": ingestion_id,
            "chunk_number": chunk_number,
            "chunk_id":chunk_id,
            "checksum":checksum,
            "records": records,
            "is_last": is_last
        }

        for attempt in range(3):
            try:
                resp = await client.post(
                    url,
                    content=orjson.dumps(payload, default=orjson_default),
                    headers={"Content-Type": "application/json"}
                )

                # Re-write the logic using ACK validation for fault tolerant system and re-tries
                # resp.raise_for_status()
                # ack_response = resp.json()
                # ack = ack_response.get("ack")

                # Added checksum mechanism to make sure chunk wise data ingegrity along with ack validation for fault tolerant system and re-tries
                ack_response = resp.json()
                debug_logger.debug(f"JsonIngestionService._send_chunk | response from pim core callback url ={ack_response}")
                ack = ack_response.get("ack")

                # raise exception when the chunk is rejected due to errors 
                if ack is not True:
                    if ack_response.get("error") == ErrorMessages.OUT_OF_ORDER_CHUNK.value:
                        # Write the logic to handle the case when we get the chunk out of order error 
                        error_logger.error(f"JsonIngestionService._send_chunk | Chunk {chunk_number} rejected: {ack_response.get("error")}")
                        raise Exception(
                                f"Chunk {chunk_number} rejected: {ack_response.get("error")}"
                        )
                    error_logger.error(f"JsonIngestionService._send_chunk | Chunk {chunk_number} rejected: {ack_response.get("error")}") 
                    raise Exception(
                        f"Chunk {chunk_number} rejected: {ack_response.get("error")}"
                )
                return
            except Exception as e:
                error_logger.error(f"JsonIngestionService._send_chunk | Retry {attempt + 1} for chunk {chunk_number}: {e}")
                if attempt == 2:
                    raise


