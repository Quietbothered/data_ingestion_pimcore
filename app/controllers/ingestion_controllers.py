from fastapi import HTTPException, status, BackgroundTasks
import uuid

from app.schemas.response_model import IngestStartResponse
from app.utils.error_messages import ErrorMessages
from app.services.json_reader import JsonIngestionService
from app.services.excel_reader import ExcelIngestionService

# import logging utility
from app.utils.logger import LoggerFactory

# import info logger messages
from app.utils.logger_info_messages import LoggerInfoMessages

# import utility to generate ingestion id
from app.utils.generate_ingestion_id import GenerateFileAndIngestionID
import time

# initialize logging utility
info_logger = LoggerFactory.get_info_logger()
error_logger = LoggerFactory.get_error_logger()
debug_logger = LoggerFactory.get_debug_logger()

class IngestionController:
    def __init__(self):
        self.json_streamer = JsonIngestionService()
        self.excel_streamer = ExcelIngestionService()
        self.ingesttion_and_file_id_generator = GenerateFileAndIngestionID()

    def ingest(self, request, bg: BackgroundTasks) -> IngestStartResponse:
        file_id = self.ingesttion_and_file_id_generator.generate_file_id(request.file_path, request.file_type)

        # If pim-core requests for data re-ingestion from the same file from this mciro-service
        if request.re_ingestion:
            # New execution
            version = str(int(time.time() * 1000))
        else:
            # Resume semantic
            version = "resume"

        ingestion_id = self.ingesttion_and_file_id_generator.generate_ingestion_id(file_id, version)

        info_logger.info(f"IngestionController.ingest | This method will make decision based on what file type the client want to ingest data from and fire up the core logic of data ingestion based on the file type for either json files or for excel files.")
        try:
            if request.file_type.lower() == "json":
                info_logger.info(f"IngestionController.ingest | {LoggerInfoMessages.PROCESS_JSON_FILES.value}")
                bg.add_task(
                    self.json_streamer.stream_and_push,
                    ingestion_id,
                    request
                )

            elif request.file_type.lower() == "excel":
                info_logger.info(f"IngestionController.ingest | {LoggerInfoMessages.PROCESS_EXCEL_FILES.value}")
                bg.add_task(
                    self.excel_streamer.stream_and_push,
                    ingestion_id,
                    request
                )

            else:
                error_logger.error(f"IngestionController.ingest | {ErrorMessages.INVALID_FILE_TYPE.value}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=ErrorMessages.INVALID_FILE_TYPE.value
                )

        except Exception as e:
            error_logger.error(f"IngestionController.ingest | {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(e)
            )
        info_logger.info(f"IngestionController.ingest | status=STARTED , ingestion_id = {ingestion_id}")
        return IngestStartResponse(
            status="STARTED",
            ingestion_id=ingestion_id
        )
