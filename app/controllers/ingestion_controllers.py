from fastapi import HTTPException, status, BackgroundTasks
import uuid

from app.schemas.response_model import IngestStartResponse
from app.utils.error_messages import ErrorMessages
from app.services.json_reader import JsonIngestionService
from app.services.excel_reader import ExcelIngestionService


class IngestionController:
    def __init__(self):
        self.json_streamer = JsonIngestionService()
        self.excel_streamer = ExcelIngestionService()

    def ingest(self, request, bg: BackgroundTasks) -> IngestStartResponse:
        ingestion_id = str(uuid.uuid4())

        try:
            if request.file_type.lower() == "json":
                bg.add_task(
                    self.json_streamer.stream_and_push,
                    ingestion_id,
                    request
                )

            elif request.file_type.lower() == "excel":
                bg.add_task(
                    self.excel_streamer.stream_and_push,
                    ingestion_id,
                    request
                )

            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=ErrorMessages.INVALID_FILE_TYPE.value
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
