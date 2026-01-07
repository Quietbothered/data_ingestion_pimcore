from pydantic import BaseModel, Field, model_validator
from fastapi import HTTPException, status
from typing import List, Dict, Any, Optional
from app.utils.error_messages import ErrorMessages
class IngestionRequest(BaseModel):
    file_path : str = Field(default=None, description="Input file path or url")
    file_type : str = Field(default="json", description="Type of input file you want to ingest (JSON or EXCEL)")
    page: int = Field(default=1, ge=1, description="Page number (1-based)")
    chunk_size_by_records: Optional[int] = Field(default=None, ge=1, le=4000, description="Define your chunk size by number of records per chunk")
    chunk_size_by_memory: Optional[int] = Field(default=None,description = "Define your chunk size by memory taken by dataframe in bytes")
    
    @model_validator(mode="after")
    def validate_chunking_mode(self):
        if not self.file_path:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorMessages.FILE_URL_IS_NONE.value
            )
        
        if not self.file_type:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorMessages.FILE_TYPE_IS_NONE.value
            )
        
        if not self.chunk_size_by_records and not self.chunk_size_by_memory:
            raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=ErrorMessages.NEITHER_CHUNK_SIZE_PROVIDED.value
                )

        if self.chunk_size_by_records and self.chunk_size_by_memory:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorMessages.BOTH_CHUNK_SIZES_PROVIDED.value
            )

        return self