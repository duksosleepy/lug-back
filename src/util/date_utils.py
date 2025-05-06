"""
Tiện ích xử lý ngày tháng.
Di chuyển từ src/sapo_sync/utils.py
"""

import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def convert_to_gmt7(iso_datetime_str):
    """
    Chuyển đổi chuỗi thời gian ISO từ GMT+0 sang GMT+7 để hiển thị.
    Chỉ chuyển đổi định dạng hiển thị, không làm thay đổi dữ liệu gốc.

    Args:
        iso_datetime_str: Chuỗi thời gian theo định dạng ISO

    Returns:
        str: Chuỗi thời gian đã chuyển đổi sang GMT+7
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

        # Định dạng lại theo mong muốn
        return dt.strftime("%Y-%m-%dT%H:%M:%S+07:00")
    except Exception:
        # Nếu có lỗi, giữ nguyên chuỗi gốc
        return iso_datetime_str


def get_adjusted_dates(start_date: str, end_date: str, format="standard"):
    """
    Điều chỉnh ngày bắt đầu và kết thúc cho API request.

    Args:
        start_date: Ngày bắt đầu định dạng YYYY-MM-DD
        end_date: Ngày kết thúc định dạng YYYY-MM-DD
        format: Định dạng output ('standard' hoặc 'iso')

    Returns:
        tuple: Cặp (start_date, end_date) đã điều chỉnh
    """
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
