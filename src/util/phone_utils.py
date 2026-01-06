"""
Tiện ích xử lý số điện thoại với regex.
"""

import re
from typing import Optional

import pandas as pd

from src.util.logging import get_logger

logger = get_logger(__name__)


def is_valid_phone(phone: str) -> bool:
    """
    Kiểm tra số điện thoại có hợp lệ không sử dụng regex
    Chấp nhận số điện thoại bắt đầu bằng 84 hoặc 0[3|5|7|8|9] và có 8 chữ số sau đó

    Args:
        phone: Số điện thoại cần kiểm tra

    Returns:
        bool: True nếu số điện thoại hợp lệ, False nếu không
    """
    if pd.isna(phone):
        return False

    # Loại bỏ các ký tự không phải số
    phone_str = str(phone).strip()
    phone_str = re.sub(r"[-()\s\.\+]", "", phone_str)

    # Kiểm tra số điện thoại đặc biệt
    if phone_str in ["09999999999", "090000000", "0912345678", "0999999999"]:
        return True

    # Chuẩn hóa số điện thoại
    if phone_str.startswith("+84"):
        phone_str = "0" + phone_str[3:]
    elif phone_str.startswith("84") and not phone_str.startswith("0"):
        phone_str = "0" + phone_str[2:]

    # Kiểm tra số có đúng định dạng không (có 0 đứng đầu và có 10 chữ số)
    # Hoặc không có 0 đứng đầu nhưng có đúng 9 chữ số (trường hợp nhập thiếu 0)
    if (
        phone_str.startswith("0")
        and len(phone_str) == 10
        and phone_str[1] in ["3", "5", "7", "8", "9"]
    ) or (
        not phone_str.startswith("0")
        and len(phone_str) == 9
        and phone_str[0] in ["3", "5", "7", "8", "9"]
    ):
        return True

    # Áp dụng regex pattern để xử lý các trường hợp đặc biệt
    regex_pattern = r"^(84|0[3|5|7|8|9])([0-9]{8})\b"
    return bool(re.match(regex_pattern, phone_str))


def format_phone_number(phone: str) -> Optional[str]:
    """
    Format số điện thoại thành định dạng chuẩn của Việt Nam

    Args:
        phone: Số điện thoại cần định dạng

    Returns:
        Optional[str]: Số điện thoại đã định dạng hoặc None nếu không hợp lệ
    """
    if pd.isna(phone):
        return None

    # Loại bỏ các ký tự không phải số
    phone_str = str(phone).strip()
    phone_str = re.sub(r"[-()\s\.\+]", "", phone_str)

    # Giữ nguyên số điện thoại đặc biệt
    if phone_str in ["09999999999", "090000000", "0912345678", "0999999999"]:
        return phone_str

    # Chuẩn hóa số điện thoại
    if phone_str.startswith("+84"):
        phone_str = "0" + phone_str[3:]
    elif phone_str.startswith("84") and not phone_str.startswith("0"):
        phone_str = "0" + phone_str[2:]

    # Xử lý trường hợp người dùng nhập số điện thoại có số 0 đứng đầu
    # Nếu số có 10 chữ số và bắt đầu bằng 0, loại bỏ số 0 đầu tiên để đảm bảo chỉ có 9 chữ số
    if (
        phone_str.startswith("0")
        and len(phone_str) == 10
        and phone_str[1] in ["3", "5", "7", "8", "9"]
    ):
        # Lưu ý: Nếu bạn muốn bỏ số 0 đầu, dùng dòng sau:
        # return phone_str[1:]
        # Nếu bạn vẫn muốn giữ định dạng 10 số, sử dụng dòng sau:
        return phone_str

    # Trường hợp số đã đúng định dạng 9 chữ số (không có 0 đứng đầu)
    if (
        not phone_str.startswith("0")
        and len(phone_str) == 9
        and phone_str[0] in ["3", "5", "7", "8", "9"]
    ):
        return phone_str

    # Kiểm tra hợp lệ theo regex
    regex_pattern = r"^(84|0[3|5|7|8|9])([0-9]{8})\b"
    if re.match(regex_pattern, phone_str):
        # Xử lý định dạng theo yêu cầu
        if phone_str.startswith("84"):
            # Chuyển đổi 84xxx thành xxx (bỏ 84 đi)
            return phone_str[2:]
        elif phone_str.startswith("0"):
            # Chuyển đổi 0xxx thành xxx (bỏ 0 đi)
            return phone_str[1:]

    return None
