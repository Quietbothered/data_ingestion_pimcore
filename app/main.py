from fastapi import FastAPI,status, Request, HTTPException
from fastapi.responses import JSONResponse

# custom routes
from app.api.ingest_data import router as ingest_data_router

# import logging utility
from app.utils.logger import LoggerFactory

# import logger info messages
from app.utils.logger_info_messages import LoggerInfoMessages

# initialize logging utility
info_logger = LoggerFactory.get_info_logger()
error_logger = LoggerFactory.get_error_logger()
debug_logger = LoggerFactory.get_debug_logger()

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
    info_logger.info(f"api_hit : /api/health : {LoggerInfoMessages.API_HIT_SUCCESS.value}")
    return {
        "status": status.HTTP_200_OK,
        "message":"success check ok!"
    }