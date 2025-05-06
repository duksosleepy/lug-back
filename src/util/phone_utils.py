"""
Tiện ích xử lý số điện thoại.
Di chuyển từ src/api/server.py
"""

import logging

logger = logging.getLogger(__name__)


def format_phone_number(phone: str) -> str:
    """
    Chuyển đổi số điện thoại từ định dạng quốc tế (+84...) sang định dạng Việt Nam (0...)

    Args:
        phone: Số điện thoại cần định dạng

    Returns:
        str: Số điện thoại đã định dạng
    """
    if not phone:
        return ""

    # Loại bỏ khoảng trắng và các ký tự không cần thiết
    phone = phone.strip()

    # Nếu số điện thoại bắt đầu bằng +84, thay bằng 0
    if phone.startswith("+84"):
        return "0" + phone[3:]

    # Nếu số điện thoại bắt đầu bằng 84 (không có dấu +), thay bằng 0
    if phone.startswith("84") and not phone.startswith("0"):
        return "0" + phone[2:]

    return phone
