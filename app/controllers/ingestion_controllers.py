# import fast-api related libraries and packages
from fastapi import HTTPException, status

# import request response model
from app.schemas.request_model import IngestionRequest
from app.schemas.response_model import IngestResponse

# import error messages
from app.utils.error_messages import ErrorMessages

# import services here
from app.services.json_reader import JsonIngestionService
from app.services.memory_monitoring import DataFrameMemoryService

class IngestionController:
    def __init__(self):
        self.json_ingestion_service = JsonIngestionService()
        self.memory_service = DataFrameMemoryService()

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
            # paginated read json data logic
            records, total_rows, page_dfs = self.json_ingestion_service.read_paginated(
                path=request.file_path,
                page=request.page,
                page_size=request.page_size
            )

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
        
        # Memory taken by the dataframe
        memory_mb = round(
            sum(
                self.memory_service.calculate_bytes(df)
                for df in page_dfs
            ) / (1024 * 1024),
            2  # round to 2 decimal places
        )
        memory_bytes = sum(
                self.memory_service.calculate_bytes(df)
                for df in page_dfs
            ) 

        return IngestResponse(
            status=status.HTTP_200_OK,
            rows=len(records),
            total_rows=total_rows,
            page=request.page,
            page_size=request.page_size,
            df_memory_usage_mb=memory_mb,
            df_memory_usage_bytes=memory_bytes,
            data=records
        )
