# import fast api related libraries and packages
from fastapi import APIRouter, Depends

# import request response model
from app.schemas.request_model import IngestionRequest
from app.schemas.response_model import IngestResponse

# import common error messages
from app.utils.error_messages import ErrorMessages

# import services
from app.services.json_reader import JsonIngestionService

# import controllers
from app.controllers.ingestion_controllers import IngestionController

router = APIRouter(tags=["Ingestion"])

def get_ingestion_controller():
    return IngestionController()

@router.post("/ingest", response_model=IngestResponse)
def ingest_data(
    request: IngestionRequest,
    controller: IngestionController = Depends(get_ingestion_controller)
):
    return controller.ingest(request)