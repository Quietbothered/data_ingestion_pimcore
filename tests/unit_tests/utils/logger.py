import logging
import time
import pytest
from pathlib import Path


class TestLoggingConfig:
    LOG_DIR = Path(__file__).resolve().parents[2] / "unit_tests" / "logs"

    @classmethod
    def ensure_log_dir(cls):
        cls.LOG_DIR.mkdir(parents=True, exist_ok=True)

    @classmethod
    def setup_logger(cls, name: str, file_name: str) -> logging.Logger:
        cls.ensure_log_dir()

        logger = logging.getLogger(name)

        if logger.handlers:
            return logger

        logger.setLevel(logging.DEBUG)

        handler = logging.FileHandler(cls.LOG_DIR / file_name)
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger


# ------------------------
# Logger instances
# ------------------------
_test_info_logger = TestLoggingConfig.setup_logger("test_info", "test_info.log")
_test_error_logger = TestLoggingConfig.setup_logger("test_error", "test_error.log")
_test_debug_logger = TestLoggingConfig.setup_logger("test_debug", "test_debug.log")


# ------------------------
# Logger fixtures (THIS IS THE FIX)
# ------------------------
@pytest.fixture(scope="session")
def test_info_logger():
    return _test_info_logger


@pytest.fixture(scope="session")
def test_error_logger():
    return _test_error_logger


@pytest.fixture(scope="session")
def test_debug_logger():
    return _test_debug_logger


# ------------------------
# Other fixtures
# ------------------------
@pytest.fixture
def test_timer():
    start = time.time()
    yield lambda: time.time() - start


@pytest.fixture(scope="session")
def tests_root_dir():
    return Path(__file__).parents[2].resolve()
