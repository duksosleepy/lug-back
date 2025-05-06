from typing import Dict

from util.logging import get_logger

from .base import BaseSettings

logger = get_logger(__name__)


class EmailSettings(BaseSettings):
    """
    Thiết lập cho việc gửi email.
    """

    CONFIG_SECTION = "EMAIL"

    def __init__(self):
        """Khởi tạo cấu hình email."""
        # Tải cấu hình từ file nếu có (ưu tiên biến môi trường PATH)
        config_path = self.get_env(
            "EMAIL_CONFIG_PATH", "/etc/lug-back/email_config.ini"
        )
        self.config = self.load_config_file(config_path)

        # SMTP server
        self.smtp_server = self._get_setting("SMTP_SERVER", required=True)

        # SMTP port
        smtp_port_str = self._get_setting("SMTP_PORT", required=True)
        try:
            self.smtp_port = int(smtp_port_str)
        except (ValueError, TypeError):
            logger.error(
                f"SMTP_PORT không phải là số nguyên hợp lệ: {smtp_port_str}"
            )
            raise ValueError(
                f"SMTP_PORT phải là số nguyên, nhận được: {smtp_port_str}"
            )

        # Email address
        self.email_address = self._get_setting("EMAIL_ADDRESS", required=True)

        # Password
        self.password = self._get_setting("EMAIL_PASSWORD", required=True)

    def _get_setting(self, name: str, required: bool = False) -> str:
        """
        Lấy cài đặt email theo thứ tự ưu tiên:
        1. Từ biến môi trường
        2. Từ file cấu hình

        Args:
            name: Tên thiết lập
            required: Nếu True, sẽ raise lỗi nếu không tìm thấy

        Returns:
            str: Giá trị thiết lập
        """
        # Ưu tiên biến môi trường
        env_value = self.get_env(name)
        if env_value is not None:
            return env_value

        # Thử từ file cấu hình
        config_value = self.get_config_value(
            self.config, name, section=self.CONFIG_SECTION
        )
        if config_value is not None:
            return config_value

        # Nếu bắt buộc mà không tìm thấy
        if required:
            raise ValueError(f"Thiết lập bắt buộc không tìm thấy: {name}")

        return ""

    def get_config_dict(self) -> Dict[str, str]:
        """
        Trả về từ điển cấu hình để sử dụng với EmailClient.

        Returns:
            Dict[str, str]: Từ điển cấu hình email
        """
        return {
            "smtp_server": self.smtp_server,
            "smtp_port": self.smtp_port,
            "email_address": self.email_address,
            "password": self.password,
        }
