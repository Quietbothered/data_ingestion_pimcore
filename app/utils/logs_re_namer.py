from logging.handlers import RotatingFileHandler
from pathlib import Path

def numbered_log_namer(default_name: str) -> str:
    """
    Converts:
    debug.log.1 → debug1.log
    debug.log.2 → debug2.log
    """
    base = Path(default_name)

    if base.suffix.isdigit():
        # not used, but safe guard
        return default_name

    if "." in base.name:
        name, index = base.name.rsplit(".", 1)
        if index.isdigit():
            return str(base.with_name(f"{name.replace('.log','')}{index}.log"))

    return default_name