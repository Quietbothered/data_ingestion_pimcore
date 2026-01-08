# import fast-api related libraries and packages
from fastapi import HTTPException, status, BackgroundTasks
import uuid

# import request response model
from app.schemas.request_model import IngestionRequest
from app.schemas.response_model import IngestStartResponse

# import error messages
from app.utils.error_messages import ErrorMessages

# import services here
from app.services.json_reader import JsonIngestionService
from app.services.memory_monitoring import DataFrameMemoryService

class IngestionController:
    def __init__(self):
        self.streamer = JsonIngestionService()

    def ingest(self, request, bg: BackgroundTasks) -> IngestStartResponse:
        ingestion_id = str(uuid.uuid4())
        try:
            bg.add_task(
                self.streamer.stream_and_push,
                ingestion_id,
                request
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(e)
            )

        return IngestStartResponse(
            status="STARTED",
            ingestion_id=ingestion_id
        )
