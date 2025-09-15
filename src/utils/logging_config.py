import logging
import logging.config
import os
import socket
from ..storage.log_storage import ParquetLogStorage, ParquetLogHandler

# Global log storage instance
_log_storage = None


def get_log_storage() -> ParquetLogStorage:
    """Get or create the global log storage instance"""
    global _log_storage
    if _log_storage is None:
        _log_storage = ParquetLogStorage(base_path="logs")
    return _log_storage


def setup_logging(
    log_level: str = "INFO", enable_console: bool = True, enable_parquet: bool = True
):
    """
    Setup logging configuration for the entire application

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        enable_console: Whether to enable console output
        enable_parquet: Whether to enable Parquet storage
    """

    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)

    # Base logging configuration
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "detailed": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(funcName)s() - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "json": {
                "format": "%(asctime)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
                "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
            },
        },
        "handlers": {},
        "root": {"level": log_level, "handlers": []},
        "loggers": {
            "src": {"level": log_level, "handlers": [], "propagate": False},
            "src.crawlers": {"level": log_level, "handlers": [], "propagate": False},
            "src.storage": {"level": log_level, "handlers": [], "propagate": False},
            "src.api": {"level": log_level, "handlers": [], "propagate": False},
        },
    }

    # Add console handler if enabled
    if enable_console:
        config["handlers"]["console"] = {
            "class": "logging.StreamHandler",
            "level": log_level,
            "formatter": "standard",
            "stream": "ext://sys.stdout",
        }
        config["root"]["handlers"].append("console")
        for logger_name in config["loggers"]:
            config["loggers"][logger_name]["handlers"].append("console")

    # Add file handler for error logs
    config["handlers"]["error_file"] = {
        "class": "logging.handlers.RotatingFileHandler",
        "level": "ERROR",
        "formatter": "detailed",
        "filename": "logs/error.log",
        "maxBytes": 10485760,  # 10MB
        "backupCount": 5,
    }
    config["root"]["handlers"].append("error_file")
    for logger_name in config["loggers"]:
        config["loggers"][logger_name]["handlers"].append("error_file")

    # Apply configuration
    logging.config.dictConfig(config)

    # Add Parquet handler if enabled (done after config to avoid circular imports)
    if enable_parquet:
        log_storage = get_log_storage()
        parquet_handler = ParquetLogHandler(log_storage)
        parquet_handler.setLevel(log_level)

        # Create a custom formatter that adds extra fields
        class ParquetFormatter(logging.Formatter):
            def format(self, record):
                # Add hostname to record
                record.hostname = socket.gethostname()

                # Add any extra context data
                if not hasattr(record, "extra_data"):
                    record.extra_data = {}

                # Add custom fields based on logger name
                if "crawler" in record.name.lower():
                    record.extra_data["component"] = "crawler"
                elif "storage" in record.name.lower():
                    record.extra_data["component"] = "storage"
                elif "api" in record.name.lower():
                    record.extra_data["component"] = "api"

                return super().format(record)

        formatter = ParquetFormatter("%(message)s")
        parquet_handler.setFormatter(formatter)

        # Add to root logger and all configured loggers
        logging.getLogger().addHandler(parquet_handler)
        for logger_name in config["loggers"]:
            logging.getLogger(logger_name).addHandler(parquet_handler)

    # Log initialization
    logger = logging.getLogger(__name__)
    logger.info(
        f"Logging initialized - Level: {log_level}, Console: {enable_console}, Parquet: {enable_parquet}"
    )


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the given name

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)
