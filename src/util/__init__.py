"""
Utility module chứa các tiện ích sử dụng trong toàn bộ ứng dụng.
"""

# Core utilities that have minimal dependencies
from .logging import get_logger, setup_logging

# Only export names without importing immediately
__all__ = [
    # Email
    "EmailClient",
    "load_config",
    "send_notification_email",
    # Google
    "get_sheets_service",
    "update_sheet_with_retry",
    "get_credentials",
    # Date/time
    "convert_to_gmt7",
    "get_adjusted_dates",
    # Phone
    "is_valid_phone",
    "format_phone_number",
    # Excel
    "validate_excel_file",
    # HTTP
    "post_json_data",
    "get_json_data",
    # Sentry
    "init_sentry",
    # Logging
    "setup_logging",
    "get_logger",
]


# Define lazy loading functions
def _lazy_import(name):
    """Lazy import function to prevent circular references"""
    import importlib

    return importlib.import_module(f".{name}", package="util")


# Email
def EmailClient(*args, **kwargs):
    from .mail_client import EmailClient as _EmailClient

    return _EmailClient(*args, **kwargs)


def load_config():
    from .send_email import load_config as _load_config

    return _load_config()


def send_notification_email(*args, **kwargs):
    from .send_email import send_notification_email as _send_notification_email

    return _send_notification_email(*args, **kwargs)


# Date/time
def convert_to_gmt7(iso_datetime_str):
    from .date_utils import convert_to_gmt7 as _convert_to_gmt7

    return _convert_to_gmt7(iso_datetime_str)


def get_adjusted_dates(start_date, end_date, format="standard"):
    from .date_utils import get_adjusted_dates as _get_adjusted_dates

    return _get_adjusted_dates(start_date, end_date, format)


# Google
def get_sheets_service():
    from .google_utils import get_sheets_service as _get_sheets_service

    return _get_sheets_service()


def update_sheet_with_retry(*args, **kwargs):
    from .google_utils import (
        update_sheet_with_retry as _update_sheet_with_retry,
    )

    return _update_sheet_with_retry(*args, **kwargs)


def get_credentials():
    from .google_utils import get_credentials as _get_credentials

    return _get_credentials()


# Phone
def is_valid_phone(phone):
    from .phone_utils import is_valid_phone as _is_valid_phone

    return _is_valid_phone(phone)


def format_phone_number(phone):
    from .phone_utils import format_phone_number as _format_phone_number

    return _format_phone_number(phone)


# HTTP
def post_json_data(*args, **kwargs):
    from .http_utils import post_json_data as _post_json_data

    return _post_json_data(*args, **kwargs)


def get_json_data(*args, **kwargs):
    from .http_utils import get_json_data as _get_json_data

    return _get_json_data(*args, **kwargs)


# Sentry
def init_sentry():
    from .sentry import init as _init

    return _init()


# Excel - chỉ export hàm validate_excel_file từ api.server
def validate_excel_file(filename, allowed_extensions=None):
    """
    Kiểm tra file có phải là file Excel không.

    Args:
        filename: Tên file cần kiểm tra
        allowed_extensions: Tập hợp các phần mở rộng được phép
    """
    from pathlib import Path

    if allowed_extensions is None:
        from settings import app_settings

        allowed_extensions = app_settings.allowed_extensions

    suffix = Path(filename).suffix
    if suffix not in allowed_extensions:
        from fastapi import HTTPException

        raise HTTPException(
            status_code=400,
            detail=f"Chỉ chấp nhận file Excel ({', '.join(allowed_extensions)}). File của bạn có đuôi: {suffix}",
        )
