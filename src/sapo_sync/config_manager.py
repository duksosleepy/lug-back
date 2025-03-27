import logging
import os
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SapoConfig:
    """Lưu trữ cấu hình Sapo API"""

    # mysapo.net
    api_key: str
    secret_key: str

    # mysapogo.com
    access_token: str

    # Google Sheets
    spreadsheet_id: str
    landing_site_range: str
    data_range: str

    @classmethod
    def from_env(cls) -> "SapoConfig":
        """Tạo cấu hình từ biến môi trường, với fallback về giá trị mặc định"""
        # Cảnh báo nếu thiếu biến môi trường
        required_vars = [
            "SAPO_API_KEY",
            "SAPO_SECRET_KEY",
            "SAPO_ACCESS_TOKEN",
            "SAPO_SPREADSHEET_ID",
        ]

        missing_vars = [var for var in required_vars if var not in os.environ]
        if missing_vars:
            logger.warning(
                f"⚠️ Thiếu biến môi trường: {', '.join(missing_vars)}. "
                "Sử dụng giá trị mặc định (KHÔNG AN TOÀN cho môi trường production)."
            )

        return cls(
            # mysapo.net
            api_key=os.environ.get("SAPO_API_KEY", ""),
            secret_key=os.environ.get("SAPO_SECRET_KEY", ""),
            # mysapogo.com
            access_token=os.environ.get("SAPO_ACCESS_TOKEN", ""),
            # Google Sheets
            spreadsheet_id=os.environ.get(
                "SAPO_SPREADSHEET_ID",
                "1h0Vrl5BgeSoN1tY2uQhHFV4Qv01yykFGXXDSeZJPm10",
            ),
            landing_site_range=os.environ.get(
                "SAPO_LANDING_SITE_RANGE", "Landing site!D2:AX"
            ),
            data_range=os.environ.get("SAPO_DATA_RANGE", "Data!G2:U"),
        )

    def get_mysapo_net_base_url(self) -> str:
        """Trả về URL cơ sở cho mysapo.net API với thông tin xác thực"""
        return f"https://{self.api_key}:{self.secret_key}@congtysangtam.mysapo.net/admin"

    def get_mysapogo_com_headers(self) -> dict:
        """Trả về headers cho mysapogo.com API"""
        return {"X-Sapo-Access-Token": self.access_token}


# Tạo đối tượng cấu hình được chia sẻ
sapo_config = SapoConfig.from_env()
