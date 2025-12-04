import logging
from pathlib import Path
import datetime

def setup_logger(db_path=None, log_file=None, no_file_log=False):
    """
    Setup console + optional file logging with timestamped filename.
    Returns the logger instance.
    """
    logger = logging.getLogger("photo_dedup")
    logger.setLevel(logging.DEBUG)

    # Remove all existing handlers first (reset logger)
    if logger.hasHandlers():
        logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter("[%(asctime)s] %(message)s", "%H:%M:%S")
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler
    if not no_file_log:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        if log_file:
            log_path = Path(log_file)
            log_path = log_path.with_name(f"{log_path.stem}_{timestamp}{log_path.suffix}")
        elif db_path:
            db_path = Path(db_path)
            log_path = db_path.with_name(f"{db_path.stem}_{timestamp}.log")
        else:
            log_path = Path(f"photo_dedup_{timestamp}.log")

        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # Print log file info immediately
        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Logging to file: {log_path}")

    return logger
