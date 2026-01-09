from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class InnerResponseContent(BaseModel):
    ack: bool
    ingestion_id: str
    chunk_number : int 
    error : Optional[str] = None
    
class PimCoreCallBackResponse(BaseModel):
    status_code:int
    content:InnerResponseContent
    