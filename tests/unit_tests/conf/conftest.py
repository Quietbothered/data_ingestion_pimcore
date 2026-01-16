import logging
import time
import pytest
from pathlib import Path

# ------------------------------------------------------------------
# Test log directory
# ------------------------------------------------------------------
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------------
# Logger setup
# ------------------------------------------------------------------
def setup_logger(name, file):
    logger = logging.getLogger(name)

    # Prevent duplicate handlers during pytest reloads
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    handler = logging.FileHandler(LOG_DIR / file)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


test_info_logger = setup_logger("test_info", "test_info.log")
test_error_logger = setup_logger("test_error", "test_error.log")
test_debug_logger = setup_logger("test_debug", "test_debug.log")

# ------------------------------------------------------------------
# Timer fixture
# ------------------------------------------------------------------
@pytest.fixture
def test_timer():
    start = time.time()
    yield lambda: time.time() - start

# ------------------------------------------------------------------
# Test root directory fixture
# ------------------------------------------------------------------
@pytest.fixture(scope="session")
def tests_root_dir():
    """
    Returns absolute path to `tests/`
    """
    return Path(__file__).parent.resolve()