import logging
import random
import time
from datetime import datetime, timedelta

from .credentials_manager import CredentialsManager

logger = logging.getLogger(__name__)


def convert_to_gmt7(iso_datetime_str):
    """
    Chuyển đổi chuỗi thời gian ISO từ GMT+0 sang GMT+7 để hiển thị
    Chỉ chuyển đổi định dạng hiển thị, không làm thay đổi dữ liệu gốc
    """
    if not iso_datetime_str:
        return ""

    try:
        # Xử lý trường hợp không có định dạng múi giờ
        if (
            "Z" not in iso_datetime_str
            and "+" not in iso_datetime_str
            and "T" in iso_datetime_str
        ):
            iso_datetime_str += "Z"  # Giả định UTC nếu không có múi giờ

        # Chuyển đổi từ chuỗi sang datetime
        dt = datetime.fromisoformat(iso_datetime_str.replace("Z", "+00:00"))

        # Thêm 7 giờ để chuyển từ GMT+0 sang GMT+7
        dt = dt + timedelta(hours=7)

        # Định dạng lại theo mong muốn của bạn
        # Có thể thay đổi định dạng output tùy theo yêu cầu hiển thị
        return dt.strftime("%Y-%m-%dT%H:%M:%S+07:00")
    except Exception:
        # Nếu có lỗi, giữ nguyên chuỗi gốc
        return iso_datetime_str


def get_adjusted_dates(start_date: str, end_date: str, format="standard"):
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


def get_sheets_service():
    """Tạo và trả về Google Sheets API service"""
    return CredentialsManager.get_sheets_service()


def update_sheet_with_retry(service, spreadsheet_id, body, retries=5):
    """Cập nhật dữ liệu vào Google Sheet với cơ chế retry."""
    for i in range(retries):
        try:
            result = (
                service.spreadsheets()
                .values()
                .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
                .execute()
            )
            return result
        except Exception as e:
            sleep_time = (2**i) + random.uniform(0, 1)
            logger.warning(
                f"Error updating sheet: {e}. Retrying in {sleep_time:.2f} seconds."
            )
            time.sleep(sleep_time)
    raise Exception("Failed to update sheet after multiple retries.")
