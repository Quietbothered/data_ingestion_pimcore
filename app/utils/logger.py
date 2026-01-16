import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from app.utils.log_initializer import LogInitializer
from app.utils.logs_re_namer import numbered_log_namer

from app.utils.log_initializer import BASE_LOG_DIR

from app.core.config import MicroServiceConfigurations

class LoggerFactory:
    """
    Provides configured loggers for different log levels.
    """

    @staticmethod
    def _create_logger(
        name: str,
        log_file: Path,
        level: int
    ) -> logging.Logger:
        LogInitializer.initialize()

        logger = logging.getLogger(name)
        logger.setLevel(level)

        # Prevent duplicate handlers
        if logger.handlers:
            return logger

        handler = RotatingFileHandler(
            log_file,
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=10,
        )

        handler.namer = numbered_log_namer

        formatter = logging.Formatter(
            MicroServiceConfigurations.LOG_FILES_CONTENT_FORMATTER.value
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = MicroServiceConfigurations.PROPOGATE_LOGS.value

        return logger

    @classmethod
    def get_error_logger(cls) -> logging.Logger:
        LogInitializer.initialize()
        return cls._create_logger(
            name="error_logger",
            log_file=Path(f"{BASE_LOG_DIR}{MicroServiceConfigurations.ERROR_LOG_DIR.value}"),
            level=logging.ERROR
        )

    @classmethod
    def get_info_logger(cls) -> logging.Logger:
        LogInitializer.initialize()
        return cls._create_logger(
            name="info_logger",
            log_file=Path(f"{BASE_LOG_DIR}{MicroServiceConfigurations.INFO_LOG_DIR.value}"),
            level=logging.INFO
        )

    @classmethod
    def get_debug_logger(cls) -> logging.Logger:
        LogInitializer.initialize()
        return cls._create_logger(
            name="debug_logger",
            log_file=Path(f"{BASE_LOG_DIR}{MicroServiceConfigurations.DEBUG_LOG_DIR.value}"),
            level=logging.DEBUG
        )
