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

# import the utility to store the state of data ingestion process
from app.services.ingestion_state_store import IngestionStateStore

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
    def __init__(self):
        self.state_store = IngestionStateStore()
        self.total_records = 0

    async def stream_and_push(self, ingestion_id: str, request):   
        # Adding resume data stream support after container re-starts
        # Save the last cunk in the database
        last_chunk = self.state_store.get_last_chunk(ingestion_id)
        chunk_number = last_chunk + 1

        fs, _, paths = fsspec.get_fs_token_paths(request.file_path)
        debug_logger.debug(f"JsonIngestionService.stream_and_push | file_system={fs} | paths = {paths}")

        chunk = []
        chunk_bytes = 0
        # chunk_number = 0
        # self.total_records = 0

        # Resume total_records from persisted state
        """
        In case where the database doesn't have the total_records saved in it then it will return zero hence reseting the total_records properly as I have intended to be.
        """
        self.total_records = self.state_store.get_total_records(ingestion_id)

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
                                # await self._send_chunk(
                                #     client,
                                #     request.callback_url,
                                #     ingestion_id,
                                #     chunk_number,
                                #     chunk,
                                #     False
                                # )
                                # chunk_number += 1
                                # chunk.clear()
                                # chunk_bytes = 0

                                # 🔴 SKIP already ACKed chunks
                                debug_logger.debug(f"JsonIngestionService.stream_and_push| Operation : if self._should_flush | Only send chunks that are not ACKed by pim-core : {chunk_number > last_chunk}")
                                if chunk_number > last_chunk:
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
                            self.total_records += 1

            # Final chunk
            if chunk:
                debug_logger.debug(f"JsonIngestionService.stream_and_push | Processing chunks | ingestion_id = {ingestion_id} | chunk_number = {chunk_number}")
                # await self._send_chunk(
                #     client,
                #     request.callback_url,
                #     ingestion_id,
                #     chunk_number,
                #     chunk,
                #     True
                # )
                debug_logger.debug(f"JsonIngestionService.stream_and_push| Operation : if chunk = {chunk} | Only send chunks that are not ACKed by pim-core : {chunk_number > last_chunk}")
                if chunk_number > last_chunk:
                    await self._send_chunk(
                        client,
                        request.callback_url,
                        ingestion_id,
                        chunk_number,
                        chunk,
                        True
                    )

            # Completion event
            debug_logger.debug(f"JsonIngestionService.stream_and_push | Processed and completed all the chunks | ingestion_id = {ingestion_id} | chunk_number = {chunk_number} | total_records = {self.total_records} | status = COMPLETED")

            resp = await client.post(
                request.callback_url,
                json={
                    "ingestion_id": ingestion_id,
                    "status": "COMPLETED",
                    "chunk_number":chunk_number,
                    "total_records": self.total_records
                }
            )
            ack_response = resp.json()
            debug_logger.debug(f"JsonIngestionService._send_chunk | COMPLETION EVENT | response from pim core callback url ={ack_response}")
            ack = ack_response.get("ack")
            # Mark the chunk being commit by pim-core into the database hence the ingestion is complete.
            if ack:
                self.state_store.mark_completed(ingestion_id)

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
                        error_logger.error(f"JsonIngestionService._send_chunk | Chunk {chunk_number} rejected: {ack_response.get('error')}")
                        raise Exception(
                                f"Chunk {chunk_number} rejected: {ack_response.get('error')}"
                        )
                    error_logger.error(f"JsonIngestionService._send_chunk | Chunk {chunk_number} rejected: {ack_response.get('error')}") 
                    raise Exception(
                        f"Chunk {chunk_number} rejected: {ack_response.get('error')}"
                )

                # Persist progress ONLY after ACK
                self.state_store.update_chunk(ingestion_id, chunk_number, self.total_records)
                return
            except Exception as e:
                error_logger.error(f"JsonIngestionService._send_chunk | Retry {attempt + 1} for chunk {chunk_number}: {e}")
                if attempt == 2:
                    raise


