from pydantic import BaseModel, Field, model_validator
from fastapi import HTTPException, status
from typing import List, Dict, Any, Optional
from app.utils.error_messages import ErrorMessages
from app.utils.field_descriptions import RequestFieldDescriptions

# import logging utility
from app.utils.logger import LoggerFactory

# import info logger messages
from app.utils.logger_info_messages import LoggerInfoMessages

# initialize logging utility
info_logger = LoggerFactory.get_info_logger()
error_logger = LoggerFactory.get_error_logger()
debug_logger = LoggerFactory.get_debug_logger()

class IngestionRequest(BaseModel):
    file_path : str = Field(default=None, description=RequestFieldDescriptions.FILE_PATH.value)
    file_type : str = Field(default="json", description=RequestFieldDescriptions.FILE_TYPE.value)
    # page: int = Field(default=1, ge=1, description="Page number (1-based)") # remove this 
    callback_url : str = Field(default=None,description=RequestFieldDescriptions.CALLBACK_URL.value)
    chunk_size_by_records: Optional[int] = Field(default=None, ge=1, le=4000, description=RequestFieldDescriptions.CHUNK_SIZE_BY_RECORDS.value)
    # Do NOT exceed memory under ANY circumstances — even for the first row 
    chunk_size_by_memory: Optional[int] = Field(default=None,description = RequestFieldDescriptions.CHUNK_SIZE_BY_MEMORY.value)
    
    @model_validator(mode="after")
    def validate_chunking_mode(self):
        if not self.file_path:
            error_logger.error(f"IngestionRequest.validate_chunking_mode | error = {ErrorMessages.FILE_URL_IS_NONE.value}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorMessages.FILE_URL_IS_NONE.value
            )
        
        if not self.callback_url:
            error_logger.error(f"IngestionRequest.validate_chunking_mode | error = {ErrorMessages.CALL_BACK_URL_IS_NONE.value}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorMessages.CALL_BACK_URL_IS_NONE.value
            )
        
        if not self.file_type:
            error_logger.error(f"IngestionRequest.validate_chunking_mode | error = {ErrorMessages.FILE_TYPE_IS_NONE.value}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorMessages.FILE_TYPE_IS_NONE.value
            )
        
        if self.chunk_size_by_records is None and self.chunk_size_by_memory is None:
            error_logger.error(f"IngestionRequest.validate_chunking_mode | error = {ErrorMessages.NEITHER_CHUNK_SIZE_PROVIDED.value}")
            raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=ErrorMessages.NEITHER_CHUNK_SIZE_PROVIDED.value
                )

        if self.chunk_size_by_records and self.chunk_size_by_memory:
            error_logger.error(f"IngestionRequest.validate_chunking_mode | error = {ErrorMessages.BOTH_CHUNK_SIZES_PROVIDED.value}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorMessages.BOTH_CHUNK_SIZES_PROVIDED.value
            )

        return self