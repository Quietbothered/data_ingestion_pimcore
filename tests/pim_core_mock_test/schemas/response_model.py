from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class PimCoreCallBackResponse(BaseModel):
    ack: bool
    ingestion_id: str
    chunk_number : int 
    error : Optional[str] = None
    