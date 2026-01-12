import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from app.utils.log_initializer import LogInitializer
from app.utils.logs_re_namer import numbered_log_namer

from app.utils.log_initializer import BASE_LOG_DIR

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
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False

        return logger

    @classmethod
    def get_error_logger(cls) -> logging.Logger:
        LogInitializer.initialize()
        return cls._create_logger(
            name="error_logger",
            log_file=Path(f"{BASE_LOG_DIR}/error/error.log"),
            level=logging.ERROR
        )

    @classmethod
    def get_info_logger(cls) -> logging.Logger:
        LogInitializer.initialize()
        return cls._create_logger(
            name="info_logger",
            log_file=Path(f"{BASE_LOG_DIR}/info/info.log"),
            level=logging.INFO
        )

    @classmethod
    def get_debug_logger(cls) -> logging.Logger:
        LogInitializer.initialize()
        return cls._create_logger(
            name="debug_logger",
            log_file=Path(f"{BASE_LOG_DIR}/debug/debug.log"),
            level=logging.DEBUG
        )
