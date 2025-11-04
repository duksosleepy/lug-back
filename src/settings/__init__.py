"""
Settings module quản lý tập trung tất cả cấu hình cho ứng dụng.
Sử dụng lớp Settings từ các module tương ứng để truy cập cấu hình.
"""

# Import các class settings
# Import các lớp settings
import logging
from pathlib import Path

# Nạp dotenv ngay từ đầu
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

from .app import AppSettings
from .email import EmailSettings
from .google import GoogleSettings
from .sapo import SapoSettings
from .sentry import SentrySettings

# Thử nạp .env từ vị trí hiện tại
dotenv_path = Path(".env")
if dotenv_path.exists():
    load_dotenv(dotenv_path=dotenv_path)
else:
    parent_dotenv = Path("../.env")
    if parent_dotenv.exists():
        load_dotenv(dotenv_path=parent_dotenv)


# Khởi tạo và export các instances
# Private variables to hold settings instances
_app_settings = None
_email_settings = None
_sapo_settings = None
_google_settings = None
_sentry_settings = None


# Lazy loading functions
def get_app_settings():
    global _app_settings
    if _app_settings is None:
        _app_settings = AppSettings()
    return _app_settings


def get_email_settings():
    global _email_settings
    if _email_settings is None:
        try:
            _email_settings = EmailSettings()
        except ValueError as e:
            logger.warning(
                f"Email settings initialization failed: {e}. Email functionality will be unavailable."
            )
            _email_settings = None
    return _email_settings


def get_sapo_settings():
    global _sapo_settings
    if _sapo_settings is None:
        _sapo_settings = SapoSettings()
    return _sapo_settings


def get_google_settings():
    global _google_settings
    if _google_settings is None:
        _google_settings = GoogleSettings()
    return _google_settings


def get_sentry_settings():
    global _sentry_settings
    if _sentry_settings is None:
        _sentry_settings = SentrySettings()
    return _sentry_settings


# For backward compatibility
app_settings = get_app_settings()
email_settings = get_email_settings()
sapo_settings = get_sapo_settings()
google_settings = get_google_settings()
sentry_settings = get_sentry_settings()

__all__ = [
    "app_settings",
    "email_settings",
    "sapo_settings",
    "google_settings",
    "sentry_settings",
]
