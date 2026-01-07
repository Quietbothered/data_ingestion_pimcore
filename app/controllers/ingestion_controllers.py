from fastapi import HTTPException, status
from app.schemas.request_model import IngestionRequest
from app.schemas.response_model import IngestResponse
from app.utils.error_messages import ErrorMessages
from app.services.json_reader import JsonIngestionService


class IngestionController:
    def __init__(self):
        self.json_ingestion_service = JsonIngestionService()

    def ingest(self, request: IngestionRequest) -> IngestResponse:
        # Request parameter validation logic
        if not request.file_path:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorMessages.FILE_URL_IS_NONE.value
            )

        if not request.file_type:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorMessages.FILE_TYPE_IS_NONE.value
            )

        # Ingestion logic
        try:
            records = self.json_ingestion_service.read(request.file_path)

        # Exception handling 
        except FileNotFoundError as e:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=str(e)
            )
        # Invalid json file exception handling
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Invalid JSON file: {str(e)}"
            )

        except Exception:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorMessages.INTERNAL_SERVER_ERROR.value
            )

        # Pagination logic
        total_rows = len(records)
        start = (request.page - 1) * request.page_size
        end = start + request.page_size
        paginated_records = records[start:end]

        return IngestResponse(
            status=status.HTTP_200_OK,
            rows=len(paginated_records),
            total_rows=total_rows,
            page=request.page,
            page_size=request.page_size,
            data=paginated_records
        )
