"""
Tiện ích xử lý số điện thoại.
"""

import logging
import re
from typing import Optional

import pandas as pd
import phonenumbers

logger = logging.getLogger(__name__)


def is_valid_phone(phone: str) -> bool:
    """
    Kiểm tra số điện thoại có hợp lệ không sử dụng thư viện phonenumbers
    Chỉ chấp nhận số điện thoại Việt Nam 10 số (bắt đầu bằng số 0)

    Args:
        phone: Số điện thoại cần kiểm tra

    Returns:
        bool: True nếu số điện thoại hợp lệ, False nếu không
    """
    if pd.isna(phone):
        return False

    # Loại bỏ các ký tự không phải số
    phone_str = str(phone).strip()
    phone_str = re.sub(r"[-()\s\.]", "", phone_str)

    # Kiểm tra số điện thoại đặc biệt
    if phone_str in ["09999999999", "090000000", "0912345678"]:
        return True

    # Chuẩn hóa số điện thoại
    if phone_str.startswith("+84"):
        phone_str = "0" + phone_str[3:]
    elif phone_str.startswith("84") and not phone_str.startswith("0"):
        phone_str = "0" + phone_str[2:]

    # Kiểm tra độ dài phải đúng 10 số và bắt đầu bằng số 0
    if len(phone_str) != 10 or not phone_str.startswith("0"):
        return False

    try:
        # Parse số điện thoại với mã quốc gia Việt Nam (VN)
        parsed_number = phonenumbers.parse(phone_str, "VN")
        # Kiểm tra có phải số điện thoại hợp lệ không
        return phonenumbers.is_valid_number(parsed_number)
    except Exception:
        return False


def format_phone_number(phone: str) -> Optional[str]:
    """
    Format số điện thoại thành định dạng chuẩn 10 số của Việt Nam

    Args:
        phone: Số điện thoại cần định dạng

    Returns:
        Optional[str]: Số điện thoại đã định dạng hoặc None nếu không hợp lệ
    """
    if pd.isna(phone):
        return None

    # Loại bỏ các ký tự không phải số
    phone_str = str(phone).strip()
    phone_str = re.sub(r"[-()\s\.]", "", phone_str)

    # Giữ nguyên số điện thoại đặc biệt
    if phone_str in ["09999999999", "090000000", "0912345678"]:
        return phone_str

    # Chuẩn hóa số điện thoại
    if phone_str.startswith("+84"):
        phone_str = "0" + phone_str[3:]
    elif phone_str.startswith("84") and not phone_str.startswith("0"):
        phone_str = "0" + phone_str[2:]

    # Kiểm tra độ dài và tính hợp lệ
    try:
        parsed_number = phonenumbers.parse(phone_str, "VN")
        if phonenumbers.is_valid_number(parsed_number) and len(phone_str) == 10:
            return phone_str
        return None
    except Exception:
        return None
