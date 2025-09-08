"""
Tiện ích cho Google API.
Di chuyển từ src/sapo_sync/credentials_manager.py
"""

from src.util.logging import get_logger

logger = get_logger(__name__)


def get_credentials():
    """
    Tạo credentials từ thông tin trong settings.

    Returns:
        Optional[Credentials]: Đối tượng credentials hoặc None nếu không thể tạo
    """
    # Lazy import to avoid circular dependency
    from google.oauth2 import service_account

    from src.settings import google_settings

    credentials_info = google_settings.get_credentials_info()
    if not credentials_info:
        return None

    return service_account.Credentials.from_service_account_info(
        credentials_info
    )


def get_sheets_service():
    """
    Tạo Google Sheets API service với credentials từ settings.

    Returns:
        Resource: Google Sheets API service
    """
    # Lazy import to avoid circular dependency
    from googleapiclient.discovery import build

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    credentials = get_credentials()
    if not credentials:
        # Lazy import to avoid circular dependency
        from src.settings import google_settings

        error_msg = google_settings.show_detailed_error()
        logger.error(error_msg)
        raise FileNotFoundError(
            "Không tìm thấy file credentials.json hoặc thông tin xác thực hợp lệ."
        )

    return build("sheets", "v4", credentials=credentials.with_scopes(SCOPES))


def update_sheet_with_retry(service, spreadsheet_id, body, retries=5):
    """
    Cập nhật dữ liệu vào Google Sheet với cơ chế retry.

    Args:
        service: Google Sheets API service
        spreadsheet_id: ID của Google Sheet
        body: Dữ liệu cập nhật
        retries: Số lần thử lại tối đa

    Returns:
        dict: Kết quả cập nhật
    """
    import random
    import time

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
