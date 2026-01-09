from enum import Enum 
class RequestFieldDescriptions(Enum):
    # IngestionRequest model field descriptions
    FILE_PATH = "Input file path or url"
    FILE_TYPE = "Type of input file you want to ingest (JSON or EXCEL)"
    CALLBACK_URL = "Send data to pim-core using this call-back url"
    CHUNK_SIZE_BY_RECORDS = "Define your chunk size by number of records per chunk"
    CHUNK_SIZE_BY_MEMORY = "Define your chunk size by memory taken by dataframe in bytes"