from enum import Enum 

class MicroServiceConfigurations(Enum):
    # ---------------------------------------------------------------------------------------------------------------------------------
    # LOGS RELATED CONFIGURATIONS
    # ---------------------------------------------------------------------------------------------------------------------------------
    LOG_STRUCTURE = {
            "error": "error.log",
            "info": "info.log",
            "debug": "debug.log",
        }
    ERROR_LOG_DIR = "/error/error.log"
    INFO_LOG_DIR = "/info/info.log"
    DEBUG_LOG_DIR = "/debug/debug.log"
    LOG_FILES_CONTENT_FORMATTER = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
    PROPOGATE_LOGS = False

    # ---------------------------------------------------------------------------------------------------------------------------------
    # DATABASE RELATED CONFIGURATIONS
    # ---------------------------------------------------------------------------------------------------------------------------------
    DB_FOLDER_NAME = "ingestion_state_data"
    DB_NAME = "ingestion_state.db"