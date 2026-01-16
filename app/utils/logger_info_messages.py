from enum import Enum 

class LoggerInfoMessages(Enum):
    API_HIT_SUCCESS = "Success check ok!"
    PROCESS_JSON_FILES = "Processing JSON FILES"
    PROCESS_EXCEL_FILES = "Processing Excel FILES"
    
class ExcelInfoMessages(Enum):
    STREAM_START = "Excel ingestion started | ingestion_id={ingestion_id}"
    WORKBOOK_LOAD_START = "Excel workbook load started"
    WORKBOOK_LOADED = "Excel workbook loaded successfully"
    INGESTION_COMPLETED = "Excel ingestion completed | total_records={total_records}"
