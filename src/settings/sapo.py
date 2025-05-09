from typing import Dict, Tuple

from src.util.logging import get_logger

from .base import BaseSettings

logger = get_logger(__name__)


class SapoSettings(BaseSettings):
    """
    Thiết lập cho Sapo API.
    """

    CONFIG_SECTION = "SAPO"

    def __init__(self):
        """Khởi tạo cấu hình Sapo API."""
        # mysapo.net
        self.api_key = self.get_env("SAPO_API_KEY", "")
        self.secret_key = self.get_env("SAPO_SECRET_KEY", "")

        # mysapogo.com
        self.access_token = self.get_env("SAPO_ACCESS_TOKEN", "")

        # Google Sheets
        self.spreadsheet_id = self.get_env(
            "SAPO_SPREADSHEET_ID",
            "1WyN3PA4oWkjYGBGIC8nZy65102FkCD9jmnyWna6zFr8",
        )
        self.landing_site_range = self.get_env(
            "SAPO_LANDING_SITE_RANGE", "Landing site!D2:AX"
        )
        self.data_range = self.get_env("SAPO_DATA_RANGE", "Data!G2:U")

    def get_mysapo_net_base_url(self) -> str:
        """
        Trả về URL cơ sở cho mysapo.net API với thông tin xác thực

        Returns:
            str: URL API với thông tin xác thực
        """
        return f"https://{self.api_key}:{self.secret_key}@congtysangtam.mysapo.net/admin"

    def get_mysapogo_com_headers(self) -> Dict[str, str]:
        """
        Trả về headers cho mysapogo.com API

        Returns:
            Dict[str, str]: Headers với thông tin xác thực
        """
        return {"X-Sapo-Access-Token": self.access_token}

    def is_configured(self) -> bool:
        """
        Kiểm tra xem cấu hình có đầy đủ thông tin xác thực không

        Returns:
            bool: True nếu đã cấu hình đầy đủ
        """
        return bool(self.api_key and self.secret_key and self.access_token)

    def get_adjusted_dates(
        self, start_date: str, end_date: str, format="standard"
    ) -> Tuple[str, str]:
        """
        Điều chỉnh ngày bắt đầu và kết thúc theo định dạng yêu cầu

        Args:
            start_date: Ngày bắt đầu định dạng YYYY-MM-DD
            end_date: Ngày kết thúc định dạng YYYY-MM-DD
            format: Định dạng output ('standard' hoặc 'iso')

        Returns:
            Tuple[str, str]: Cặp (start_date, end_date) đã điều chỉnh
        """
        from datetime import datetime, timedelta

        start_dt = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=1)
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)

        if format == "iso":
            # Giữ nguyên định dạng Z cho API request
            return (
                f"{start_dt.strftime('%Y-%m-%d')}T00:00:00Z",
                f"{end_dt.strftime('%Y-%m-%d')}T24:00:00Z",
            )
        else:
            return start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")
