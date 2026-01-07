from pydantic import BaseModel, Field
from typing import List, Dict, Any

class IngestionRequest(BaseModel):
    file_path : str = Field(default=None, description="Input file path or url")
    file_type : str = Field(default="json", description="Type of input file you want to ingest (JSON or EXCEL)")
    page: int = Field(default=1, ge=1, description="Page number (1-based)")
    page_size: int = Field(default=10, ge=1, le=100, description="Items per page")