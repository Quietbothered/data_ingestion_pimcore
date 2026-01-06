from fastapi import FastAPI,status

# custom routes
from app.api.ingest_data import router as ingest_data_router

app = FastAPI(title = "Data Ingestion Service")

# include custome routes here
app.include_router(ingest_data_router, prefix="/api")

@app.get("/health",status_code = status.HTTP_200_OK)
def health():
    return {
        "status":"ok"
    }