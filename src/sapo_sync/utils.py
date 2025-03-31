import logging
import random
import time
from datetime import datetime, timedelta

from .credentials_manager import CredentialsManager

logger = logging.getLogger(__name__)


def convert_to_gmt7(iso_datetime_str):
    """
    Chuyển đổi chuỗi thời gian ISO từ GMT+0 sang GMT+7

    Args:
        iso_datetime_str (str): Chuỗi thời gian dạng ISO (ví dụ: "2025-03-01T12:00:00Z")

    Returns:
        str: Chuỗi thời gian đã được chuyển đổi sang GMT+7
    """
    if not iso_datetime_str:
        return ""

    try:
        # Xử lý một số trường hợp đặc biệt
        if (
            "Z" not in iso_datetime_str
            and "+" not in iso_datetime_str
            and "-" not in iso_datetime_str[10:]
        ):
            if "T" in iso_datetime_str:
                iso_datetime_str += (
                    "Z"  # Giả sử GMT+0 nếu không có chỉ định múi giờ
                )
            else:
                return iso_datetime_str  # Trả về nguyên gốc nếu không phải định dạng ISO

        # Parse chuỗi thời gian
        dt = datetime.fromisoformat(iso_datetime_str.replace("Z", "+00:00"))

        # Thêm 7 giờ để chuyển từ GMT+0 sang GMT+7
        dt = dt + timedelta(hours=7)

        # Định dạng lại với múi giờ +07:00
        return dt.strftime("%Y-%m-%dT%H:%M:%S+07:00")
    except Exception as e:
        logger.error(
            f"Lỗi khi chuyển đổi thời gian {iso_datetime_str}: {str(e)}"
        )
        return iso_datetime_str  # Trả về chuỗi gốc nếu có lỗi


def get_adjusted_dates(start_date: str, end_date: str, format="standard"):
    """
    Tính toán startDate - 1 ngày và endDate + 1 ngày

    Args:
        start_date: Ngày bắt đầu (YYYY-MM-DD)
        end_date: Ngày kết thúc (YYYY-MM-DD)
        format: 'standard' cho YYYY-MM-DD hoặc 'iso' cho ISO8601

    Returns:
        tuple: (adjusted_start_date, adjusted_end_date)
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=1)
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)

    if format == "iso":
        # Thay đổi từ Z (GMT+0) sang +07:00 (GMT+7)
        # Điều chỉnh giờ để tương đương với múi giờ GMT+7
        return (
            f"{start_dt.strftime('%Y-%m-%d')}T00:00:00+07:00",
            f"{end_dt.strftime('%Y-%m-%d')}T23:59:59+07:00",
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
