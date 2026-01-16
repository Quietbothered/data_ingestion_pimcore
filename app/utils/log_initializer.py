from pathlib import Path
import os
from app.core.config import MicroServiceConfigurations

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE_LOG_DIR = PROJECT_ROOT / "app" / "logs"

class LogInitializer:
    """
    Responsible for creating log directories and files if they don't exist.
    """
    LOG_STRUCTURE = MicroServiceConfigurations.LOG_STRUCTURE.value
    
    @classmethod
    def initialize(cls) -> None:
        """
        Create log directories and files if they do not exist.
        """
        for folder, filename in cls.LOG_STRUCTURE.items():
            dir_path =  BASE_LOG_DIR / folder
            file_path = dir_path / filename

            # Create directory
            dir_path.mkdir(parents=True, exist_ok=True)

            # Create empty log file if missing
            if not file_path.exists():
                file_path.touch()
