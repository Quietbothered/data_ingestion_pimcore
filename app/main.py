from fastapi import FastAPI,status, Request, HTTPException
from fastapi.responses import JSONResponse

# custom routes
from app.api.ingest_data import router as ingest_data_router

app = FastAPI(title = "Data Ingestion Service")

# include custome routes here
# ingest_data router
app.include_router(ingest_data_router, prefix="/api")

# Global error exception response handler
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": exc.status_code,
            "error": exc.detail
        }
    )

# Test api
@app.get("/health",status_code = status.HTTP_200_OK)
def health():
    return {
        "status": status.HTTP_200_OK,
        "message":"success check ok!"
    }