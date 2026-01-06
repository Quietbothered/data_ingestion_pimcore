from fastapi import APIRouter, HTTPException, status

# import request model
from app.schemas.request_model import IngestionRequest
# import response model
from app.schemas.response_model import IngestResponse

router = APIRouter(tags=["Ingestion"])

@router.post("/ingest",response_model=IngestResponse)
def ingest_data(request:IngestionRequest):
    try:
        records = [
            {
                "name":"Rollex",
                "roll_no":997
            },
            {
                "name":"Ballistic",
                "roll_no":1000
            },
            {
                "name":"Barricade",
                "roll_no":247
            }
        ]
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e))
    
    return IngestResponse(
        rows=len(records),
        data=records
    ) 