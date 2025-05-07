"""
Settings module quản lý tập trung tất cả cấu hình cho ứng dụng.
Sử dụng lớp Settings từ các module tương ứng để truy cập cấu hình.
"""

# Import các class settings
# Import các lớp settings
from pathlib import Path

# Nạp dotenv ngay từ đầu
from dotenv import load_dotenv

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
app_settings = AppSettings()
email_settings = EmailSettings()
sapo_settings = SapoSettings()
google_settings = GoogleSettings()
sentry_settings = SentrySettings()

__all__ = [
    "app_settings",
    "email_settings",
    "sapo_settings",
    "google_settings",
    "sentry_settings",
]
