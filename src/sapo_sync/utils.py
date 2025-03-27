import logging
import random
import time
from datetime import datetime, timedelta

from .credentials_manager import CredentialsManager

logger = logging.getLogger(__name__)


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
        return (
            f"{start_dt.strftime('%Y-%m-%d')}T17:00:00Z",
            f"{end_dt.strftime('%Y-%m-%d')}T16:59:59Z",
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
