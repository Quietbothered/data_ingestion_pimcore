from pathlib import Path

# import logging utility
from app.utils.logger import LoggerFactory

# import info logger messages
from app.utils.logger_info_messages import LoggerInfoMessages

# import utility to generate ingestion id
from app.utils.generate_ingestion_id import generate_ingestion_id

# initialize logging utility
info_logger = LoggerFactory.get_info_logger()
error_logger = LoggerFactory.get_error_logger()
debug_logger = LoggerFactory.get_debug_logger()


def get_current_project_dir():
    info_logger.info(f"get_current_project_dir | getting current project's directory")
    PROJECT_DIR = Path(__file__).resolve().parent
    debug_logger.debug(f"get_current_project_dir | PROJECT_DIR = {PROJECT_DIR}")
    return PROJECT_DIR