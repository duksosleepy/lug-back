import base64
import json
import logging
import os
from pathlib import Path
from typing import List, Optional

from .base import BaseSettings

logger = logging.getLogger(__name__)


class GoogleSettings(BaseSettings):
    """
    Thiết lập cho Google API (Sheets, Drive, etc.).
    """

    CONFIG_SECTION = "GOOGLE"

    def __init__(self):
        """Khởi tạo cấu hình Google API."""
        # Các đường dẫn thông dụng để tìm kiếm file credentials
        self.common_credential_paths = self._get_common_credential_paths()

        # Đường dẫn cụ thể đến file credentials (nếu có)
        self.credentials_path = self.get_env("GOOGLE_CREDENTIALS_PATH")

        # Encoded credentials (nếu có)
        self.credentials_b64 = self.get_env("GOOGLE_CREDENTIALS_B64")

        # Kiểm tra và log thông tin về việc tìm thấy thông tin xác thực
        self._log_credentials_info()

    def _get_common_credential_paths(self) -> List[str]:
        """
        Tạo danh sách các đường dẫn phổ biến để tìm kiếm file credentials.

        Returns:
            List[str]: Danh sách các đường dẫn
        """
        # Kiểm tra biến môi trường GOOGLE_APPLICATION_CREDENTIALS
        paths = []
        app_creds = self.get_env("GOOGLE_APPLICATION_CREDENTIALS")
        if app_creds:
            paths.append(app_creds)

        # Thêm các đường dẫn phổ biến khác
        common_paths = [
            "credentials.json",  # Thư mục gốc
            "./credentials.json",  # Thư mục hiện tại, chỉ định rõ ràng
            "../credentials.json",  # Thư mục cha
            "./config/credentials.json",  # Thư mục config
            "./secrets/credentials.json",  # Thư mục secrets
            str(Path.home() / "credentials.json"),  # Thư mục home của user
        ]
        paths.extend(common_paths)

        return paths

    def _log_credentials_info(self) -> None:
        """
        Log thông tin về việc tìm thấy thông tin xác thực.
        """
        if self.credentials_b64:
            logger.info(
                "Thông tin xác thực Google có sẵn từ biến môi trường GOOGLE_CREDENTIALS_B64"
            )

        elif self.credentials_path and os.path.exists(self.credentials_path):
            logger.info(
                f"Thông tin xác thực Google có sẵn từ GOOGLE_CREDENTIALS_PATH: {self.credentials_path}"
            )

        else:
            # Kiểm tra các đường dẫn thông dụng
            found_path = None
            for path in self.common_credential_paths:
                if os.path.exists(path):
                    found_path = path
                    break

            if found_path:
                logger.info(
                    f"Thông tin xác thực Google có sẵn tại: {found_path}"
                )
            else:
                logger.warning(
                    "Không tìm thấy thông tin xác thực Google. Một số tính năng có thể không hoạt động."
                )

    def get_credentials_info(self) -> Optional[dict]:
        """
        Trả về thông tin credentials dưới dạng dictionary.

        Returns:
            Optional[dict]: Thông tin xác thực hoặc None nếu không tìm thấy
        """
        # Ưu tiên từ biến môi trường B64
        if self.credentials_b64:
            try:
                credentials_json = base64.b64decode(
                    self.credentials_b64
                ).decode("utf-8")
                return json.loads(credentials_json)
            except Exception as e:
                logger.error(f"Lỗi khi giải mã GOOGLE_CREDENTIALS_B64: {e}")

        # Thử từ file path cụ thể
        if self.credentials_path and os.path.exists(self.credentials_path):
            try:
                with open(self.credentials_path, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(
                    f"Lỗi khi đọc file credentials từ {self.credentials_path}: {e}"
                )

        # Thử từ các đường dẫn thông dụng
        for path in self.common_credential_paths:
            if os.path.exists(path):
                try:
                    with open(path, "r") as f:
                        return json.load(f)
                except Exception as e:
                    logger.error(f"Lỗi khi đọc file credentials từ {path}: {e}")

        return None

    def get_credentials_path(self) -> Optional[str]:
        """
        Trả về đường dẫn đến file credentials nếu tồn tại.

        Returns:
            Optional[str]: Đường dẫn đến file credentials hoặc None nếu không tìm thấy
        """
        # Kiểm tra đường dẫn cụ thể
        if self.credentials_path and os.path.exists(self.credentials_path):
            return self.credentials_path

        # Kiểm tra các đường dẫn thông dụng
        for path in self.common_credential_paths:
            if os.path.exists(path):
                return path

        return None

    def show_detailed_error(self) -> str:
        """
        Hiển thị thông báo lỗi chi tiết với hướng dẫn khi không tìm thấy thông tin xác thực.

        Returns:
            str: Thông báo lỗi chi tiết
        """
        error_msg = """
        ===================== LỖI XÁC THỰC GOOGLE SHEETS =====================
        Không thể tìm thấy file credentials.json hoặc thông tin xác thực hợp lệ.

        Để khắc phục, vui lòng thực hiện MỘT trong các bước sau:

        1. Đặt file credentials.json vào thư mục gốc của dự án
           hoặc một trong các thư mục: ./config, ./secrets

        2. Thiết lập biến môi trường với đường dẫn:
           GOOGLE_CREDENTIALS_PATH="/đường/dẫn/đến/credentials.json"

        3. Mã hóa nội dung file credentials.json bằng base64 và thiết lập:
           GOOGLE_CREDENTIALS_B64="<chuỗi_base64>"

           Để tạo chuỗi base64 trong Windows bằng Python:
           ```
           python -c "import base64; print(base64.b64encode(open('credentials.json', 'rb').read()).decode('utf-8'))"
           ```

        File credentials.json cần được tạo từ Google Cloud Console:
        1. Mở https://console.cloud.google.com
        2. Tạo hoặc chọn một dự án
        3. Tạo Service Account và tải file credentials
        4. Chia sẻ Google Sheet với email service account
        ========================================================================
        """
        return error_msg
