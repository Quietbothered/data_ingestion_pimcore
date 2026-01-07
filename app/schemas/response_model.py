from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional 

class IngestResponse(BaseModel):
    status: int
    error: Optional[str] = None
    # rows in this page
    rows: int
    # total rows in file                     
    total_rows: int
    # page_number               
    page: int
    chunk_size_by_records: Optional[int] = None
    chunk_size_by_memory: Optional[int] = None
    # dataframe memory usage
    df_memory_usage_mb: float = Field(default_factory = 0.0)
    df_memory_usage_bytes : int = Field(default_factory = 0)
    data: List[Dict[str, Any]] = Field(default_factory=list)
