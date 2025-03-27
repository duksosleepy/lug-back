import base64
import json
import logging
import os
from pathlib import Path
from typing import Optional

from google.oauth2 import service_account
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)


class CredentialsManager:
    """Quản lý việc tải credentials theo thứ tự ưu tiên từ các nguồn khác nhau"""

    @classmethod
    def get_sheets_service(cls):
        """Tạo Google Sheets API service với credentials từ nhiều nguồn khác nhau"""
        SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

        # Thử từng phương pháp theo thứ tự ưu tiên
        credentials = None

        # 1. Thử từ biến môi trường GOOGLE_CREDENTIALS_B64
        credentials = cls.from_base64_env()

        # 2. Thử từ file được chỉ định trong biến môi trường
        if credentials is None:
            credentials = cls.from_env_path()

        # 3. Thử các đường dẫn thông thường
        if credentials is None:
            credentials = cls.from_common_paths()

        # 4. Nếu vẫn không có, báo lỗi rõ ràng
        if credentials is None:
            cls._show_detailed_error()

        # Tạo và trả về service
        return build(
            "sheets", "v4", credentials=credentials.with_scopes(SCOPES)
        )

    @classmethod
    def from_base64_env(cls) -> Optional[Credentials]:
        """Tạo credentials từ biến môi trường chứa chuỗi base64"""
        if "GOOGLE_CREDENTIALS_B64" in os.environ:
            try:
                credentials_json = base64.b64decode(
                    os.environ["GOOGLE_CREDENTIALS_B64"]
                ).decode("utf-8")
                credentials_info = json.loads(credentials_json)
                credentials = (
                    service_account.Credentials.from_service_account_info(
                        credentials_info
                    )
                )
                logger.info(
                    "✅ Sử dụng Google credentials từ biến môi trường GOOGLE_CREDENTIALS_B64"
                )
                return credentials
            except Exception as e:
                logger.error(
                    f"❌ Lỗi khi tạo credentials từ biến môi trường: {e}"
                )
        return None

    @classmethod
    def from_env_path(cls) -> Optional[Credentials]:
        """Tạo credentials từ đường dẫn trong biến môi trường"""
        if "GOOGLE_CREDENTIALS_PATH" in os.environ:
            creds_path = os.environ["GOOGLE_CREDENTIALS_PATH"]
            return cls._try_load_from_file(
                creds_path, "GOOGLE_CREDENTIALS_PATH"
            )
        return None

    @classmethod
    def from_common_paths(cls) -> Optional[Credentials]:
        """Tìm kiếm file credentials trong các vị trí thông dụng"""
        # Danh sách các đường dẫn phổ biến để tìm kiếm
        common_paths = [
            "credentials.json",  # Thư mục gốc
            "./credentials.json",  # Thư mục hiện tại, chỉ định rõ ràng
            "../credentials.json",  # Thư mục cha
            "./config/credentials.json",  # Thư mục config
            "./secrets/credentials.json",  # Thư mục secrets
            str(Path.home() / "credentials.json"),  # Thư mục home của user
        ]

        # Kiểm tra biến môi trường GOOGLE_APPLICATION_CREDENTIALS
        if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
            common_paths.insert(0, os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

        # Thử từng đường dẫn
        for path in common_paths:
            result = cls._try_load_from_file(path)
            if result:
                return result

        return None

    @classmethod
    def _try_load_from_file(
        cls, path: str, source: str = "file system"
    ) -> Optional[Credentials]:
        """Thử tải credentials từ file"""
        try:
            if os.path.exists(path):
                credentials = Credentials.from_service_account_file(path)
                logger.info(
                    f"✅ Sử dụng Google credentials từ {source}: {path}"
                )
                return credentials
        except Exception as e:
            logger.error(f"❌ Lỗi khi tải credentials từ {path}: {e}")
        return None

    @classmethod
    def _show_detailed_error(cls):
        """Hiển thị thông báo lỗi chi tiết với hướng dẫn"""
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
        logger.error(error_msg)
        raise FileNotFoundError(
            "Không tìm thấy file credentials.json hoặc thông tin xác thực hợp lệ."
        )
