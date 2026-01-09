"""
This file is written to test the microservice by simulating behaviour of pim-core cron
for data ingestion using json files
"""

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

# import response model
from schemas.response_model import PimCoreCallBackResponse, InnerResponseContent

app = FastAPI()

# old_code [working]
# received_chunks = []

# @app.post("/callback")
# async def receive_chunk(request: Request):
#     payload = await request.json()
#     received_chunks.append(payload)

#     print(">>>>> RECEIVED CHUNK <<<<<")
#     print(payload)

#     return {"status": "OK"}

# @app.get("/received")
# def get_received():
#     return {
#         "total_chunks": len(received_chunks),
#         "chunks": received_chunks
#     }

# new code with ACK/NACK implementation to make the ingestion pipeline resilient to failures
total_records_recieved = 0
@app.post("/callback")
async def receive_chunk(request: Request) -> PimCoreCallBackResponse:
    global total_records_recieved
    payload = await request.json()

    ingestion_id = payload.get("ingestion_id")
    chunk_number = payload.get("chunk_number")
    records = payload.get("records", [])

    print(">>>>> RECEIVED CHUNK <<<<<")
    total_records_recieved = total_records_recieved + len(records)
    print(f"Ingestion: {ingestion_id}, Chunk: {chunk_number}, Records: {len(records)}, Total_records_recieved : {total_records_recieved}")

    # Simulate validation / processing
    if not records:
        return PimCoreCallBackResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=InnerResponseContent(
                ack=False,
                ingestion_id=ingestion_id,
                chunk_number=chunk_number,
                error="Empty chunk"
            ).model_dump()
        )

    return PimCoreCallBackResponse(
        status_code=status.HTTP_200_OK,
            content=InnerResponseContent(
                ack=True,
                ingestion_id=ingestion_id,
                chunk_number=chunk_number,
                error="Empty chunk"
            ).model_dump()
    )



"""
/home/aditya/github/data_ingestion_pimcore/tests/test_data/PIM_PRODIDSKU_20251222183200000_001.json


http://127.0.0.1:9000/callback
"""