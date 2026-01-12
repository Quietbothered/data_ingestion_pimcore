# import fast api related libraries and packages
from fastapi import APIRouter, Depends, BackgroundTasks

# import request response model
from app.schemas.request_model import IngestionRequest
from app.schemas.response_model import IngestStartResponse

# import controllers
from app.controllers.ingestion_controllers import IngestionController

# import logging utility
from app.utils.logger import LoggerFactory

# import info logger messages
from app.utils.logger_info_messages import LoggerInfoMessages

router = APIRouter(tags=["Ingestion"])

# initialize logging utility
info_logger = LoggerFactory.get_info_logger()
error_logger = LoggerFactory.get_error_logger()
debug_logger = LoggerFactory.get_debug_logger()

def get_ingestion_controller():
    return IngestionController()

@router.post("/ingest", response_model=IngestStartResponse)
def ingest_data(
    request: IngestionRequest,
    bg: BackgroundTasks,
    controller: IngestionController = Depends(get_ingestion_controller)
):
    info_logger.info(f"api_hit : /api/ingest : {LoggerInfoMessages.API_HIT_SUCCESS.value}")
    return controller.ingest(request,bg)