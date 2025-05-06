import configparser
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional, TypeVar

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

    @classmethod
    def get_bool_env(cls, name: str, default: bool = False) -> bool:
        """
        Lấy giá trị boolean từ biến môi trường.

        Args:
            name: Tên biến môi trường
            default: Giá trị mặc định nếu không tìm thấy

        Returns:
            bool: Giá trị boolean
        """
        value = cls.get_env(name)
        if value is None:
            return default

        # Xử lý các giá trị chuỗi thành boolean
        if value.lower() in ("true", "yes", "1", "t", "y"):
            return True
        elif value.lower() in ("false", "no", "0", "f", "n"):
            return False

        # Nếu không khớp với các trường hợp đã biết, trả về mặc định
        logger.warning(
            f"Giá trị '{value}' không thể chuyển thành boolean cho biến {name}. Sử dụng mặc định: {default}"
        )
        return default

    @classmethod
    def get_int_env(cls, name: str, default: int = 0) -> int:
        """
        Lấy giá trị số nguyên từ biến môi trường.

        Args:
            name: Tên biến môi trường
            default: Giá trị mặc định nếu không tìm thấy hoặc không chuyển đổi được

        Returns:
            int: Giá trị số nguyên
        """
        value = cls.get_env(name)
        if value is None:
            return default

        try:
            return int(value)
        except ValueError:
            logger.warning(
                f"Giá trị '{value}' không thể chuyển thành số nguyên cho biến {name}. Sử dụng mặc định: {default}"
            )
            return default

    @classmethod
    def get_float_env(cls, name: str, default: float = 0.0) -> float:
        """
        Lấy giá trị số thực từ biến môi trường.

        Args:
            name: Tên biến môi trường
            default: Giá trị mặc định nếu không tìm thấy hoặc không chuyển đổi được

        Returns:
            float: Giá trị số thực
        """
        value = cls.get_env(name)
        if value is None:
            return default

        try:
            return float(value)
        except ValueError:
            logger.warning(
                f"Giá trị '{value}' không thể chuyển thành số thực cho biến {name}. Sử dụng mặc định: {default}"
            )
            return default

    @classmethod
    def load_config_file(cls, config_path: str) -> Dict[str, Dict[str, str]]:
        """
        Tải cấu hình từ file INI.

        Args:
            config_path: Đường dẫn đến file cấu hình

        Returns:
            Dict[str, Dict[str, str]]: Từ điển cấu hình được tải từ file
        """
        config = {}
        if not config_path or not os.path.exists(config_path):
            # Nếu file không tồn tại, thử các đường dẫn mặc định
            for path in cls.DEFAULT_CONFIG_PATHS:
                if os.path.exists(path):
                    config_path = path
                    break

        if config_path and os.path.exists(config_path):
            try:
                parser = configparser.ConfigParser()
                parser.read(config_path)

                # Chuyển đổi ConfigParser thành từ điển
                for section in parser.sections():
                    config[section] = {}
                    for key, value in parser.items(section):
                        config[section][key] = value

                logger.info(f"Đã tải cấu hình từ file: {config_path}")
            except Exception as e:
                logger.error(
                    f"Lỗi khi tải file cấu hình {config_path}: {str(e)}"
                )
        else:
            logger.warning(f"Không tìm thấy file cấu hình tại: {config_path}")

        return config

    @classmethod
    def get_config_value(
        cls,
        config: Dict[str, Dict[str, str]],
        key: str,
        section: Optional[str] = None,
        default: Any = None,
    ) -> Any:
        """
        Lấy giá trị từ cấu hình.

        Args:
            config: Từ điển cấu hình
            key: Khóa cần tìm
            section: Phần cấu hình, nếu None sẽ dùng CONFIG_SECTION của lớp
            default: Giá trị mặc định nếu không tìm thấy

        Returns:
            Any: Giá trị cấu hình hoặc giá trị mặc định
        """
        if not config:
            return default

        section_name = section or cls.CONFIG_SECTION

        if section_name not in config:
            return default

        return config[section_name].get(key.lower(), default)
