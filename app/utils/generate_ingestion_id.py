import hashlib

# import logging utility
from app.utils.logger import LoggerFactory

# initialize logging utility
info_logger = LoggerFactory.get_info_logger()
error_logger = LoggerFactory.get_error_logger()
debug_logger = LoggerFactory.get_debug_logger()

def generate_ingestion_id(file_path: str, file_type: str) -> str:
    info_logger.info(f"generate_ingestion_id | generate predictable ingestion id based on file name")
    raw = f"{file_path}|{file_type}"
    return hashlib.sha256(raw.encode()).hexdigest()