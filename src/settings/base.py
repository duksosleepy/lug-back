import logging
import os
from pathlib import Path
from typing import Any, TypeVar

# Thêm import cho dotenv
from dotenv import load_dotenv

T = TypeVar("T")

logger = logging.getLogger(__name__)


class BaseSettings:
    """
    Lớp cơ sở cho các thiết lập trong ứng dụng.
    Cung cấp các tiện ích để tải cấu hình từ biến môi trường và file cấu hình.
    """

    # Config sections sẽ được các lớp con ghi đè
    CONFIG_SECTION = "DEFAULT"

    # Đường dẫn mặc định cho file cấu hình
    DEFAULT_CONFIG_PATHS = [
        "/etc/lug-back/config.ini",
        str(Path.home() / ".config" / "lug-back" / "config.ini"),
        "config.ini",
    ]

    # Biến để theo dõi việc đã nạp .env hay chưa
    _dotenv_loaded = False

    @classmethod
    def ensure_dotenv_loaded(cls):
        """
        Đảm bảo file .env đã được nạp (chỉ nạp một lần).
        """
        if not cls._dotenv_loaded:
            # Tìm file .env ở thư mục hiện tại hoặc thư mục cha
            dotenv_path = os.environ.get("DOTENV_PATH")
            if dotenv_path and os.path.exists(dotenv_path):
                load_dotenv(dotenv_path=dotenv_path)
                logger.info(f"Đã nạp biến môi trường từ file: {dotenv_path}")
            else:
                # Thử nạp từ các vị trí thông thường
                potential_paths = [".env", "../.env"]
                for path in potential_paths:
                    if os.path.exists(path):
                        load_dotenv(dotenv_path=path)
                        logger.info(f"Đã nạp biến môi trường từ file: {path}")
                        break

            cls._dotenv_loaded = True

    @classmethod
    def get_env(
        cls, name: str, default: Any = None, required: bool = False
    ) -> Any:
        """
        Lấy giá trị từ biến môi trường.

        Args:
            name: Tên biến môi trường
            default: Giá trị mặc định nếu không tìm thấy
            required: Nếu True, sẽ raise lỗi nếu không tìm thấy

        Returns:
            Giá trị của biến môi trường hoặc giá trị mặc định
        """
        # Đảm bảo .env được nạp trước khi truy cập biến môi trường
        cls.ensure_dotenv_loaded()

        value = os.environ.get(name)

        if value is None:
            if required:
                raise ValueError(
                    f"Biến môi trường bắt buộc không tìm thấy: {name}"
                )
            return default

        return value

    # Các phương thức khác giữ nguyên như đã định nghĩa...
