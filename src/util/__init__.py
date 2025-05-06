"""
Utility module chứa các tiện ích sử dụng trong toàn bộ ứng dụng.
"""

# Email
# Date/time
from .date_utils import convert_to_gmt7, get_adjusted_dates

# Google
from .google_utils import (
    get_credentials,
    get_sheets_service,
    update_sheet_with_retry,
)

# HTTP
from .http_utils import get_json_data, post_json_data
from .mail_client import EmailClient

# Phone
from .phone_utils import format_phone_number
from .send_email import load_config, send_notification_email

# Sentry
from .sentry import init as init_sentry


# Excel - chỉ export hàm validate_excel_file từ server.py
def validate_excel_file(filename: str, allowed_extensions=None):
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
    "format_phone_number",
    # Excel
    "validate_excel_file",
    # HTTP
    "post_json_data",
    "get_json_data",
    # Sentry
    "init_sentry",
]
