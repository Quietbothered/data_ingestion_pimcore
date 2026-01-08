# import fast api related libraries and packages
from fastapi import APIRouter, Depends, BackgroundTasks

# import request response model
from app.schemas.request_model import IngestionRequest
from app.schemas.response_model import IngestStartResponse

# import controllers
from app.controllers.ingestion_controllers import IngestionController

router = APIRouter(tags=["Ingestion"])

def get_ingestion_controller():
    return IngestionController()

@router.post("/ingest", response_model=IngestStartResponse)
def ingest_data(
    request: IngestionRequest,
    bg: BackgroundTasks,
    controller: IngestionController = Depends(get_ingestion_controller)
):
    return controller.ingest(request,bg)