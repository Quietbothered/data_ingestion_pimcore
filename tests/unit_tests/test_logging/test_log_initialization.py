class TestLoggingCreation:
    def test_log_directory_created(self):
        from conf.logging_config import TestLoggingConfig

        log_dir = TestLoggingConfig.LOG_DIR
        assert log_dir.exists()
        assert log_dir.is_dir()

    def test_log_files_created(self):
        from conf.logging_config import TestLoggingConfig

        log_dir = TestLoggingConfig.LOG_DIR
        for log_file in [
            "test_info.log",
            "test_error.log",
            "test_debug.log",
        ]:
            assert (log_dir / log_file).exists()

    def test_log_files_are_writable(self):
        from conf.logging_config import TestLoggingConfig

        for log_file in [
            "test_info.log",
            "test_error.log",
            "test_debug.log",
        ]:
            assert (TestLoggingConfig.LOG_DIR / log_file).stat().st_size > 0
