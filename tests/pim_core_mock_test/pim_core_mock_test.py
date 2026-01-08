"""
This file is written to test the microservice by simulating behaviour of pim-core cron
for data ingestion using json files
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

received_chunks = []

@app.post("/callback")
async def receive_chunk(request: Request):
    payload = await request.json()
    received_chunks.append(payload)

    print(">>>>> RECEIVED CHUNK <<<<<")
    print(payload)

    return {"status": "OK"}

@app.get("/received")
def get_received():
    return {
        "total_chunks": len(received_chunks),
        "chunks": received_chunks
    }



"""
/home/aditya/github/data_ingestion_pimcore/tests/test_data/PIM_PRODIDSKU_20251222183200000_001.json


http://127.0.0.1:9000/callback
"""