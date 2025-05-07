"""
Logging configuration module using Loguru.
"""

import logging
import sys
from pathlib import Path
from typing import Dict, List, Union

from loguru import logger

# Xóa import này để tránh import cycle
# from settings import app_settings
from .helpers import get_logger, log_error_with_context, log_request_info


class InterceptHandler(logging.Handler):
    """
    Intercept standard logging messages toward Loguru.
    This allows us to capture logs from libraries using standard logging.
    """

    def emit(self, record):
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where the logged message originated
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def setup_logging(
    log_level: str = "INFO",
    json_logs: bool = False,
    log_to_file: bool = False,  # Changed default to False
):
    """
    Set up Loguru logging for the application.

    Args:
        log_level: Minimum log level to display
        json_logs: Whether to format logs as JSON
        log_to_file: Whether to save logs to file
    """
    # Remove default loguru handler
    logger.remove()

    # Intercept standard logging
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    # Configure handlers
    handlers: List[Dict[str, Union[str, bool, int]]] = [
        # Console handler
        {
            "sink": sys.stderr,
            "level": log_level,
            "colorize": True,
            "backtrace": True,
            "diagnose": True,
            "format": (
                "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                "<level>{message}</level>"
            )
            if not json_logs
            else None,
            "serialize": json_logs,
        }
    ]

    # Add file handler if requested
    if log_to_file:
        # Try to create logs directory
        try:
            # Động thái import settings khi cần
            from settings import app_settings

            # First attempt: use the configured data_dir
            log_dir = Path(app_settings.data_dir) / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
        except (PermissionError, FileNotFoundError, ImportError):
            # Fallback: use user's home directory
            log_dir = Path.home() / ".local" / "share" / "lug-back" / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            logger.warning(
                f"Cannot write to data_dir/logs, using {log_dir} instead"
            )

        # Define log file path
        log_file = log_dir / "lug-back.log"

        logger.info(f"Logging to file: {log_file}")

        # Add rotation configuration
        handlers.append(
            {
                "sink": str(log_file),
                "level": log_level,
                "rotation": "10 MB",
                "retention": "1 week",
                "compression": "zip",
                "backtrace": True,
                "diagnose": True,
                "format": (
                    "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
                    "{level: <8} | "
                    "{name}:{function}:{line} | "
                    "{message}"
                )
                if not json_logs
                else None,
                "serialize": json_logs,
            }
        )

    # Configure loguru with all handlers
    for handler in handlers:
        logger.configure(handlers=[handler])

    # Update Sentry SDK's internal logger to use Loguru
    for name in [
        "sentry_sdk",
        "sentry_sdk.errors",
        "sentry_sdk.integrations",
        "uvicorn",
        "uvicorn.error",
        "fastapi",
    ]:
        logging.getLogger(name).handlers = [InterceptHandler()]

    # Set level for specific loggers if needed
    for name, level in [
        ("uvicorn.access", "INFO"),
        ("uvicorn.error", "INFO"),
        ("uvicorn.asgi", "INFO"),
        ("httpcore.http11", "WARNING"),
    ]:
        logging.getLogger(name).setLevel(getattr(logging, level))

    return logger


from .middleware import setup_fastapi_logging

__all__ = [
    "setup_logging",
    "setup_fastapi_logging",
    "InterceptHandler",
    "get_logger",
    "log_request_info",
    "log_error_with_context",
]
