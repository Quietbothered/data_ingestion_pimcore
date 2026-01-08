from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional 

class IngestStartResponse(BaseModel):
    status: str
    ingestion_id: str
