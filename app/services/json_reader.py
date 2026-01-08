import pandas as pd
import ijson
import fsspec
from typing import List, Dict, Tuple, Optional

from app.utils.json_decimal_encoder import orjson_default

import httpx
import orjson

class JsonIngestionService:
    """
    This method will read json using read stream method
    """
    async def stream_and_push(self, ingestion_id: str, request):
        fs, _, paths = fsspec.get_fs_token_paths(request.file_path)

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
                await self._send_chunk(
                    client,
                    request.callback_url,
                    ingestion_id,
                    chunk_number,
                    chunk,
                    True
                )

            # Completion event
            await client.post(
                request.callback_url,
                json={
                    "ingestion_id": ingestion_id,
                    "status": "COMPLETED",
                    "total_records": total_records
                }
            )

    def _should_flush(self, request, chunk, chunk_bytes, next_record_bytes):
        if request.chunk_size_by_records:
            return len(chunk) >= request.chunk_size_by_records
        return (chunk_bytes + next_record_bytes) > request.chunk_size_by_memory

    async def _send_chunk(
        self,
        client,
        url,
        ingestion_id,
        chunk_number,
        records,
        is_last
    ):
        payload = {
            "ingestion_id": ingestion_id,
            "chunk_number": chunk_number,
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
                resp.raise_for_status()
                return
            except Exception:
                if attempt == 2:
                    raise


