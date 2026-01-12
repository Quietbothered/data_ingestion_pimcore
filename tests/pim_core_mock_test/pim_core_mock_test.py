"""
This file is written to test the microservice by simulating behaviour of pim-core cron
for data ingestion using json files
"""

from fastapi import FastAPI, Request, status, HTTPException
from fastapi.responses import JSONResponse

# import response model
from schemas.response_model import PimCoreCallBackResponse

# import chunk data integrity validator
from services.chunk_data_integrity_validator import ChunkValidator

# import error message from utils
from utility.error_messages import ErrorMessages

app = FastAPI()

# chunk validator
chunk_validator = ChunkValidator()

# new code with ACK/NACK implementation to make the ingestion pipeline resilient to failures
total_records_recieved = 0
@app.post("/callback")
async def receive_chunk(request: Request) -> PimCoreCallBackResponse:
    global total_records_recieved
    payload = await request.json()

    if payload.get("status") == "COMPLETED":
        total_records_recieved = 0
        print(">>>>INGESTION COMPLETED<<<<")
        print(f"Ingestion: {payload.get("ingestion_id")}")
        print(f"Total records: {payload.get("total_records")}")
        print(f"Last chunk: {payload.get("chunk_number")}")
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "status":payload.get("status"),
                "ack": True,
                "ingestion_id": payload.get("ingestion_id"),
                "chunk_number": payload.get("chunk_number")
            }
        )

    # validate chunks recieved from the fast-api microservice
    ack, chunk_validity_error = chunk_validator.validate(
        ingestion_id=payload.get("ingestion_id"),
        chunk_id=payload.get("chunk_id"),
        chunk_number=payload.get("chunk_number"),
        records=payload.get("records", []),
        checksum=payload.get("checksum"),
    )

    ingestion_id = payload.get("ingestion_id")
    chunk_number = payload.get("chunk_number")
    records = payload.get("records", [])

    print(">>>>> RECEIVED CHUNK <<<<<")
    total_records_recieved = total_records_recieved + len(records)
    print(f"Ingestion: {ingestion_id}, Chunk: {chunk_number}, Records: {len(records)}, Total_records_recieved : {total_records_recieved}")

    # Simulate validation / processing
    if not records:
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=PimCoreCallBackResponse(
                ack=False,
                ingestion_id=ingestion_id,
                chunk_number=chunk_number,
                error=ErrorMessages.EMPTY_CHUNK.value
            ).model_dump()
        )
    if chunk_validity_error:
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=PimCoreCallBackResponse(
                ack=False,
                ingestion_id=ingestion_id,
                chunk_number=chunk_number,
                error=chunk_validity_error
            ).model_dump()
        )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=PimCoreCallBackResponse(
            ack=True,
            ingestion_id=ingestion_id,
            chunk_number=chunk_number
        ).model_dump()
    )



"""
/home/aditya/github/data_ingestion_pimcore/tests/test_data/PIM_PRODIDSKU_20251222183200000_001.json


http://127.0.0.1:9000/callback
"""