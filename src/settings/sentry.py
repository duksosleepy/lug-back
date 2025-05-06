import logging
from typing import Any, Callable, Dict, List, Optional

from .base import BaseSettings

logger = logging.getLogger(__name__)


class SentrySettings(BaseSettings):
    """
    Thiết lập cho Sentry error tracking.
    """

    CONFIG_SECTION = "SENTRY"

    def __init__(self):
        """Khởi tạo cấu hình Sentry."""
        # DSN
        self.dsn = self.get_env("SENTRY_DSN", "")

        # Environment
        self.environment = self.get_env("SENTRY_ENVIRONMENT", "development")

        # Release version
        self.release = self.get_env("SENTRY_RELEASE", "0.1.0")

        # Traces sample rate
        self.traces_sample_rate = self.get_float_env(
            "SENTRY_TRACES_SAMPLE_RATE", 0.0
        )

        # Danh sách các loại ngoại lệ không báo cáo
        self.non_reported_exceptions = self._get_non_reported_exceptions()

        # Cờ bật/tắt Sentry
        self.enabled = self.get_bool_env("SENTRY_ENABLED", False)

    def _get_non_reported_exceptions(self) -> List[str]:
        """
        Lấy danh sách các loại ngoại lệ không cần báo cáo lên Sentry.

        Returns:
            List[str]: Danh sách tên các loại ngoại lệ
        """
        exceptions_str = self.get_env(
            "SENTRY_NON_REPORTED_EXCEPTIONS", "QueryExecutionError"
        )
        if exceptions_str:
            return [ex.strip() for ex in exceptions_str.split(",")]

        # Giá trị mặc định
        return ["QueryExecutionError"]

    def is_configured(self) -> bool:
        """
        Kiểm tra xem Sentry đã được cấu hình đúng và bật chưa.

        Returns:
            bool: True nếu Sentry đã được cấu hình và bật
        """
        return self.enabled and bool(self.dsn)

    def get_before_send(
        self,
    ) -> Callable[[Dict[str, Any], Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        Tạo hàm before_send để lọc các exception không cần báo cáo.

        Returns:
            Callable: Hàm before_send cho Sentry SDK
        """

        def before_send(
            event: Dict[str, Any], hint: Dict[str, Any]
        ) -> Optional[Dict[str, Any]]:
            """
            Lọc các exception không cần báo cáo.

            Args:
                event: Sự kiện Sentry
                hint: Gợi ý về sự kiện

            Returns:
                Optional[Dict[str, Any]]: Sự kiện đã lọc hoặc None nếu nên loại bỏ
            """
            if "exc_info" in hint:
                exc_type, exc_value, tb = hint["exc_info"]
                if any(
                    [
                        (e in str(type(exc_value)))
                        for e in self.non_reported_exceptions
                    ]
                ):
                    return None

            return event

        return before_send
