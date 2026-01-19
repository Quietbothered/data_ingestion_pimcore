import pytest


@pytest.mark.order(0)
class TestSuiteStart:

    def test_suite_start(self, test_info_logger):
        from .utils.logger import TestLoggingConfig

        test_info_logger.info("========== TEST SUITE STARTED ==========")
        test_info_logger.info(f"Log directory: {TestLoggingConfig.LOG_DIR}")

        for log_file in [
            "test_info.log",
            "test_error.log",
            "test_debug.log",
        ]:
            test_info_logger.info(
                f"Created log file: {TestLoggingConfig.LOG_DIR / log_file}"
            )

        assert True


@pytest.mark.order(99)
class TestSuiteEnd:
    def test_suite_end(self, test_info_logger):
        test_info_logger.info("========== TEST SUITE FINISHED ==========")
        assert True
