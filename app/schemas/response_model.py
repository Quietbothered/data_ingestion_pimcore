from pydantic import BaseModel, Field
from typing import List, Dict, Any

class IngestResponse(BaseModel):
    rows: int
    data: List[Dict[str, Any]]