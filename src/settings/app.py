from typing import List

from src.util.logging import get_logger

from .base import BaseSettings

logger = get_logger(__name__)


class AppSettings(BaseSettings):
    """
    Thiết lập chung cho ứng dụng.
    """

    CONFIG_SECTION = "APP"

    def __init__(self):
        """Khởi tạo cấu hình ứng dụng."""
        # Tên ứng dụng
        self.app_name = self.get_env("APP_NAME", "lug-back")

        # Debug mode
        self.debug = self.get_bool_env("DEBUG", False)

        # Đường dẫn thư mục dữ liệu
        self.data_dir = self.get_env("DATA_DIR", "/var/lib/lug-back")

        # CORS settings
        self.cors_origins = self._parse_cors_origins()

        # Upload settings
        self.allowed_extensions = {".xlsx", ".xls", ".ods"}

        # Lấy cấu hình error notification emails
        self.error_notification_emails = self._get_error_notification_emails()
        self.online_error_notification_emails = (
            self._get_online_error_notification_emails()
        )

        # XC_TOKEN - token cho API
        self.xc_token = self.get_env("XC_TOKEN", "")

        # API endpoint
        self.api_endpoint = self.get_env(
            "API_ENDPOINT", "http://10.100.0.1:8081/api/v2"
        )

        # Product mapping file path
        self.product_mapping_file = self.get_env(
            "PRODUCT_MAPPING_FILE",
            "src/crm/warranty_rule.xlsx"
        )

        # CRM Batch Service Fallback Ports
        # For cluster environments where the port may change
        self.crm_batch_fallback_ports = self._parse_crm_batch_fallback_ports()

    def _parse_cors_origins(self) -> List[str]:
        """
        Phân tích danh sách origins cho CORS từ biến môi trường.

        Returns:
            List[str]: Danh sách các nguồn được phép, mặc định cho localhost
        """
        origins_str = self.get_env("CORS_ORIGINS", "")
        if origins_str:
            return [origin.strip() for origin in origins_str.split(",")]

        # Giá trị mặc định nếu không được cấu hình
        return [
            "http://localhost:3000",
            "http://localhost",
        ]

    def _get_error_notification_emails(self) -> List[str]:
        """
        Lấy danh sách email nhận thông báo lỗi.

        Returns:
            List[str]: Danh sách các email nhận thông báo
        """
        emails_str = self.get_env("ERROR_NOTIFICATION_EMAILS", "")
        if emails_str:
            return [email.strip() for email in emails_str.split(",")]

        # Giá trị mặc định
        return [
            "songkhoi123@gmail.com",
            "nam.nguyen@lug.vn",
            "dang.le@sangtam.com",
            "tan.nguyen@sangtam.com",
        ]

    def _get_online_error_notification_emails(self) -> List[str]:
        """
        Lấy danh sách email nhận thông báo lỗi cho process online.

        Returns:
            List[str]: Danh sách các email nhận thông báo
        """
        # Bổ sung email cho process online
        online_emails_str = self.get_env("ONLINE_ERROR_NOTIFICATION_EMAILS", "")
        if online_emails_str:
            return [email.strip() for email in online_emails_str.split(",")]

        # Nếu không cấu hình riêng, bổ sung thêm email vào danh sách thông thường
        return self.error_notification_emails + ["kiet.huynh@sangtam.com"]

    def _parse_crm_batch_fallback_ports(self) -> List[int]:
        """
        Phân tích danh sách cổng fallback cho CRM batch service từ biến môi trường.

        Format: "8080,28080,18080" (comma-separated)

        Returns:
            List[int]: Danh sách các cổng fallback, mặc định [8080, 28080, 18080]
        """
        ports_str = self.get_env("CRM_BATCH_FALLBACK_PORTS", "8080,28080,18080")
        try:
            return [int(port.strip()) for port in ports_str.split(",")]
        except (ValueError, AttributeError):
            logger.warning(
                f"Invalid CRM_BATCH_FALLBACK_PORTS format: {ports_str}. "
                "Using default: [8080, 28080, 18080]"
            )
            return [8080, 28080, 18080]

    def get_crm_batch_fallback_ports(self) -> List[int]:
        """
        Lấy danh sách cổng fallback cho CRM batch service.

        Returns:
            List[int]: Danh sách các cổng fallback
        """
        return self.crm_batch_fallback_ports
