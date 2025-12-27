"""
Module for sending email notifications using Apprise
"""

import tempfile
from typing import Dict, List

import apprise
import pandas as pd
from loguru import logger

from src.settings import app_settings


def send_warranty_match_email(matched_records: List[Dict]) -> bool:
    """
    Send email notification with matched warranty data using Apprise.

    Args:
        matched_records: List of matched warranty records

    Returns:
        bool: True if email sent successfully, False otherwise
    """
    if not matched_records:
        logger.warning("No matched records to send")
        return False

    try:
        # Create Apprise instance
        apobj = apprise.Apprise()

        # Load configuration from URL
        config = apprise.AppriseConfig()
        config.add(app_settings.apprise_config_url)
        apobj.add(config)

        # Create Excel file from matched records
        excel_file_path = _create_warranty_excel(matched_records)

        try:
            # Create attachment
            attachment = apprise.AppriseAttachment(excel_file_path)

            # Prepare email body
            body = _prepare_email_body(matched_records)

            # Send notification
            result = apobj.notify(
                title="Dữ liệu khách hàng đăng ký bảo hành",
                body=body,
                tag=app_settings.apprise_warranty_email_tag,
                attach=attachment,
            )

            if result:
                logger.info(
                    f"Successfully sent warranty match email with {len(matched_records)} records"
                )
            else:
                logger.error("Failed to send warranty match email via Apprise")

            return result

        finally:
            # Clean up temporary file
            import os

            if os.path.exists(excel_file_path):
                os.unlink(excel_file_path)

    except Exception as e:
        logger.error(
            f"Error sending warranty match email: {str(e)}", exc_info=True
        )
        return False


def _create_warranty_excel(records: List[Dict]) -> str:
    """
    Create an Excel file from warranty records.

    Args:
        records: List of warranty record dictionaries

    Returns:
        str: Path to the created Excel file
    """
    # Define column mapping
    headers = [
        "Ngày Ct",
        "Mã Ct",
        "Số Ct",
        "Mã bộ phận",
        "Mã đơn hàng",
        "Tên khách hàng",
        "Số điện thoại",
        "Tỉnh thành",
        "Quận huyện",
        "Phường xã",
        "Địa chỉ",
        "Mã hàng",
        "Tên hàng",
        "Imei",
        "Số lượng",
        "Doanh thu",
        "Ghi chú",
    ]

    # Map record fields to headers
    field_mapping = {
        "Ngày Ct": "ngay_ct",
        "Mã Ct": "ma_ct",
        "Số Ct": "so_ct",
        "Mã bộ phận": "ma_bo_phan",
        "Mã đơn hàng": "ma_don_hang",
        "Tên khách hàng": "ten_khach_hang",
        "Số điện thoại": "so_dien_thoai",
        "Tỉnh thành": "tinh_thanh",
        "Quận huyện": "quan_huyen",
        "Phường xã": "phuong_xa",
        "Địa chỉ": "dia_chi",
        "Mã hàng": "ma_hang",
        "Tên hàng": "ten_hang",
        "Imei": "imei",
        "Số lượng": "so_luong",
        "Doanh thu": "doanh_thu",
        "Ghi chú": "ghi_chu",
    }

    # Create rows for DataFrame
    rows = []
    for record in records:
        row = {}
        for header in headers:
            field_name = field_mapping.get(header)
            if field_name and field_name in record:
                row[header] = record[field_name]
            else:
                row[header] = ""
        rows.append(row)

    # Create DataFrame
    df = pd.DataFrame(rows, columns=headers)

    # Create temporary file
    temp_file = tempfile.NamedTemporaryFile(
        suffix=".xlsx", delete=False, prefix="warranty_match_"
    )
    temp_file_path = temp_file.name
    temp_file.close()

    # Save to Excel
    df.to_excel(temp_file_path, index=False, sheet_name="Matched Customers")

    logger.info(f"Created warranty Excel file: {temp_file_path}")

    return temp_file_path


def _prepare_email_body(records: List[Dict]) -> str:
    """
    Prepare the email body with summary information.

    Args:
        records: List of warranty records

    Returns:
        str: Formatted email body
    """
    # Get unique customer count
    unique_customers = set()
    unique_orders = set()

    for record in records:
        phone = record.get("so_dien_thoai", "")
        order = record.get("ma_don_hang", "")

        if phone:
            unique_customers.add(phone)
        if order:
            unique_orders.add(order)

    body = f"""Danh sách khách hàng đã đăng ký bảo hành qua website.

Tóm tắt:
- Số lượng bản ghi: {len(records)}
- Số khách hàng (theo SĐT): {len(unique_customers)}
- Số đơn hàng: {len(unique_orders)}

Chi tiết trong file đính kèm.

Đây là email tự động, vui lòng không trả lời."""

    return body
