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
    page_size: int
    data: List[Dict[str, Any]] = Field(default_factory=list)
