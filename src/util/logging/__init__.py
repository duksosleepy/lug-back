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
    log_to_file: bool = False,
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

    # Configure handlers
    handlers = [
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
        # Get data directory from environment or use default
        import os

        data_dir = os.environ.get("DATA_DIR", "/var/lib/lug-back")

        # Try to create logs directory
        try:
            log_dir = Path(data_dir) / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
        except (PermissionError, FileNotFoundError):
            # Fallback: use user's home directory
            log_dir = Path.home() / ".local" / "share" / "lug-back" / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            logger.warning(
                f"Cannot write to {data_dir}/logs, using {log_dir} instead"
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

    # CHANGE THIS SECTION to avoid duplicate logs
    # Intercept standard logging (only do this ONCE)
    for _name in [
        "uvicorn",
        "uvicorn.error",
        "uvicorn.access",
        "fastapi",
        "sentry_sdk",
        "sentry_sdk.errors",
        "sentry_sdk.integrations",
    ]:
        # Existing handlers might be causing duplicates
        # Remove existing handlers before adding our interceptor
        _logger = logging.getLogger(_name)
        if _logger.handlers:
            for handler in _logger.handlers:
                _logger.removeHandler(handler)
        _logger.handlers = [InterceptHandler()]
        # Prevent propagation to avoid duplicate handling
        _logger.propagate = False

    # Set level for specific loggers if needed
    for name, level in [
        ("uvicorn.access", "INFO"),
        ("uvicorn.error", "INFO"),
        ("uvicorn.asgi", "INFO"),
        ("httpcore.http11", "WARNING"),
    ]:
        logging.getLogger(name).setLevel(getattr(logging, level))

    # This must remain AFTER setting up the above loggers to prevent duplicate logs
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

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
