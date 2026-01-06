from pydantic import BaseModel, Field
from typing import List, Dict, Any

class IngestionRequest(BaseModel):
    file_path : str = Field(default=None, description="Input file path or url")
    file_type : str = Field(default="json", description="Type of input file")