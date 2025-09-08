import json
import re
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
from typing import Dict, List

import httpx
import pandas as pd

from src.settings import app_settings
from src.util.logging import get_logger
from src.util.send_email import send_notification_email

logger = get_logger(__name__)
# API endpoints and credentials - Using environment variables
AUTH_URL = app_settings.get_env("CRM_AUTH_URL")
ONLINE_DATA_URL = app_settings.get_env("CRM_ONLINE_DATA_URL")
OFFLINE_DATA_URL = app_settings.get_env("CRM_OFFLINE_DATA_URL")
BATCH_URL = app_settings.get_env("CRM_BATCH_URL")
TARGET_URL = app_settings.get_env(
    "CRM_TARGET_URL",
)
API_KEY = app_settings.get_env("CRM_API_KEY")
BATCH_TIMEOUT = app_settings.get_int_env(
    "CRM_BATCH_TIMEOUT", 60
)  # Default to 60 seconds

# Authentication credentials from environment
AUTH_CREDENTIALS = {
    "email": app_settings.get_env("CRM_AUTH_EMAIL"),
    "password": app_settings.get_env("CRM_AUTH_PASSWORD"),
}

# Field mapping from filter rules to API response
FIELD_MAPPING = {
    "Tên khách hàng": "Ten_Khach_Hang",
    "Mã vật tư": "Ma_Hang",
    "Tên vật tư": "Ten_Hang",
    "Số điện thoại": "So_Dien_Thoai",
    "Mã Ctừ": "Ma_Ct",
    "Mã bộ phận": "Ma_BP",
    "Tiền doanh thu": "Doanh_Thu",
    "Loại vật tư": "Loai_Hang",
    "Mã nhóm vật tư": "Ma_Nhom_Hang",
}

# Filter rules from the provided JSON
FILTER_RULES = {
    "online_processor": {
        "filters": [
            {
                "name": "customer_exclusion",
                "column": "Tên khách hàng",
                "type": "exclude_containing",
                "values": ["BƯU ĐIỆN"],
            },
            {
                "name": "product_code_exclusion",
                "column": "Mã vật tư",
                "type": "exclude_containing",
                "values": ["PBHDT", "THUNG", "DVVC_ONL", "TUINILONPK"],
            },
            {
                "name": "product_name_exclusion",
                "column": "Tên vật tư",
                "type": "exclude_containing",
                "values": ["BAO LÌ XÌ"],
            },
            {
                "name": "product_type_exclusion",
                "column": "Loại vật tư",
                "type": "exclude_equals",
                "values": ["VPP"],
            },
            {
                "name": "document_type_exclusion",
                "column": "Mã Ctừ",
                "type": "exclude_equals",
                "values": ["TLO"],
            },
            {
                "name": "non_empty_product_name",
                "column": "Tên vật tư",
                "type": "not_null",
                "values": [],
            },
            {
                "name": "phone_number_filter",
                "column": "Số điện thoại",
                "type": "validation_function",
                "function": "is_valid_phone",
            },
            {
                "name": "kl_test_records_filter",
                "column": "Số điện thoại",
                "type": "exclude_equals",
                "values": ["0912345678"],
            },
        ]
    },
    "offline_processor": {
        "filters": [
            {
                "name": "document_code_inclusion",
                "column": "Mã Ctừ",
                "type": "include_only",
                "values": ["BL", "BLK", "HG", "TG"],
            },
            {
                "name": "document_code_exclusion",
                "column": "Mã Ctừ",
                "type": "exclude_containing",
                "values": ["HD"],
            },
            {
                "name": "product_code_exclusion",
                "column": "Mã vật tư",
                "type": "exclude_containing",
                "values": [
                    "DV_GHEMASSAGE",
                    "THUNG",
                    "DVVC_ONL",
                    "PBHDT",
                    "TUINILONPK",
                ],
            },
            {
                "name": "product_type_exclusion",
                "column": "Loại vật tư",
                "type": "exclude_equals",
                "values": ["NUOC", "TPCN", "KEM", "VPP"],
            },
            {
                "name": "product_name_keyword_exclusion",
                "column": "Tên vật tư",
                "type": "exclude_containing",
                "values": [
                    "dịch vụ",
                    "online",
                    "thương mại điện tử",
                    "shopee",
                    "lazada",
                    "tiktok",
                ],
            },
            {
                "name": "non_empty_product_name",
                "column": "Tên vật tư",
                "type": "not_null",
                "values": [],
            },
            {
                "name": "phone_number_filter",
                "column": "Số điện thoại",
                "type": "validation_function",
                "function": "is_valid_phone",
            },
            {
                "name": "kl_test_records_filter",
                "column": "Số điện thoại",
                "type": "exclude_equals",
                "values": ["0912345678"],
            },
        ]
    },
}


def get_access_token() -> str:
    """Authenticate with the API and return the access token"""
    try:
        with httpx.Client(timeout=BATCH_TIMEOUT) as client:
            response = client.post(AUTH_URL, json=AUTH_CREDENTIALS)
            response.raise_for_status()
            return response.json()["data"]["access_token"]
    except httpx.HTTPError as e:
        logger.error(f"Authentication failed: {e}")
        raise


def fetch_data(token: str, is_online: bool, limit: int = 50) -> List[Dict]:
    """Fetch sales data from the API using the provided token"""
    url = ONLINE_DATA_URL if is_online else OFFLINE_DATA_URL
    source_type = "online" if is_online else "offline"

    # Calculate date range
    today = datetime.now()
    yesterday = today - timedelta(days=2)

    # Format dates for API query: current day at 5:00 AM and previous day at 5:00 AM
    date_lte = today.strftime("%Y-%m-%dT05:00:00")
    date_gt = yesterday.strftime("%Y-%m-%dT05:00:00")

    # Construct date filter
    date_filter = {
        "_and": [{"Ngay_Ct": {"_gt": date_gt}}, {"Ngay_Ct": {"_lte": date_lte}}]
    }

    # Parameters for the API request
    headers = {"authorization": f"Bearer {token}"}
    params = {
        "limit": -1,  # Get all records
        "filter": json.dumps(date_filter),
    }

    logger.info(f"Fetching {source_type} data from {date_gt} to {date_lte}")
    logger.info(f"Current server time: {today}")

    try:
        with httpx.Client(timeout=BATCH_TIMEOUT) as client:
            response = client.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()["data"]
            logger.info(f"Retrieved {len(data)} {source_type} sales records")
            return data
    except httpx.HTTPError as e:
        logger.error(f"Failed to fetch {source_type} sales data: {e}")
        raise


def is_valid_phone(phone: str) -> bool:
    """Validate phone number format"""
    if not phone:
        return False

    # Remove any spaces or dashes
    phone = re.sub(r"[\s-]", "", phone)

    # Basic Vietnamese phone number validation (adjust as needed)
    pattern = r"^(0|\+84)(\d{9,10})$"
    return bool(re.match(pattern, phone))


def apply_filters(data: List[Dict], is_online: bool) -> List[Dict]:
    """Apply filter rules to the data based on processor type"""
    processor_key = "online_processor" if is_online else "offline_processor"
    filters = FILTER_RULES[processor_key]["filters"]
    filtered_data = []

    for item in data:
        should_remove = False

        for filter_rule in filters:
            filter_type = filter_rule["type"]

            # Map the column name from filter rule to API response field
            column = filter_rule.get("column")
            mapped_column = FIELD_MAPPING.get(column) if column else None

            # For complex conditions with multiple columns
            if filter_type == "complex_condition":
                # Complex conditions are not implemented in this simplified version
                continue

            # Skip if the column doesn't exist in our mapping
            if not mapped_column and filter_type != "complex_condition":
                continue

            value = item.get(mapped_column)

            # Apply filter based on type - REVERSED LOGIC FROM PREVIOUS VERSION
            if filter_type == "exclude_containing":
                for exclude_value in filter_rule["values"]:
                    if value and exclude_value in str(value).upper():
                        should_remove = True
                        break

            elif filter_type == "exclude_equals":
                if value and str(value).upper() in [
                    v.upper() for v in filter_rule["values"]
                ]:
                    should_remove = True

            elif filter_type == "include_only":
                if not value or str(value).upper() not in [
                    v.upper() for v in filter_rule["values"]
                ]:
                    should_remove = True

            elif filter_type == "not_null":
                if not value:
                    should_remove = True

            elif filter_type == "equals":
                # For "equals" filter, we want to KEEP records that match the value
                # This is typically used for test data filtering
                if value and str(value) != filter_rule["values"][0]:
                    should_remove = True

            elif filter_type == "validation_function":
                function_name = filter_rule["function"]
                if function_name == "is_valid_phone":
                    # If the phone is valid, KEEP the record (don't remove)
                    # If the phone is invalid, REMOVE the record
                    if not is_valid_phone(str(value) if value else ""):
                        should_remove = True

            # If any filter marks the item for removal, break early
            if should_remove:
                break

        if not should_remove:
            filtered_data.append(item)

    logger.info(
        f"Filtered {len(data)} records down to {len(filtered_data)} records"
    )
    return filtered_data


def transform_data(sales_data: List[Dict]) -> List[Dict]:
    """Transform the sales data to the format required by the batch service
    Each individual record is treated as a separate submission"""
    transformed_data = []

    for item in sales_data:
        # Extract date part only from datetime string (YYYY-MM-DD)
        date_str = item.get("Ngay_Ct", "")
        if date_str and len(date_str) >= 10:
            date_str = date_str[:10]

        # Create a single record with master and detail
        record = {
            "master": {
                "ngayCT": date_str,
                "maCT": item.get("Ma_Ct"),
                "soCT": item.get("So_Ct"),
                "maBoPhan": item.get("Ma_BP"),
                "maDonHang": item.get("Ma_Don_Hang"),
                "tenKhachHang": item.get("Ten_Khach_Hang"),
                "soDienThoai": item.get("So_Dien_Thoai"),
                "tinhThanh": item.get("Tinh_Thanh"),
                "quanHuyen": item.get("Quan_Huyen"),
                "phuongXa": item.get("Phuong_Xa"),
                "diaChi": item.get("Dia_Chi"),
            },
            "detail": [
                {
                    "maHang": item.get("Ma_Hang"),
                    "tenHang": item.get("Ten_Hang"),
                    "imei": item.get("Imei"),
                    "soLuong": item.get("So_Luong"),
                    "doanhThu": item.get("Doanh_Thu"),
                }
            ],
        }

        # Add to the batch data
        transformed_data.append(
            {"url": TARGET_URL, "data": {"apikey": API_KEY, "data": [record]}}
        )

    return transformed_data


def submit_batch(batch_data: List[Dict]) -> Dict:
    """Submit the transformed data to the batch service in smaller chunks"""
    if not batch_data:
        logger.warning("No data to submit after filtering")
        return {
            "status": "no_data",
            "message": "No data to submit after filtering",
        }

    # Process in chunks of 30 requests
    chunk_size = 30
    results = []

    total_chunks = (len(batch_data) - 1) // chunk_size + 1

    for i in range(0, len(batch_data), chunk_size):
        chunk = batch_data[i : i + chunk_size]
        chunk_number = i // chunk_size + 1
        logger.info(
            f"Submitting chunk {chunk_number}/{total_chunks} with {len(chunk)} requests"
        )

        try:
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            with httpx.Client(
                timeout=BATCH_TIMEOUT
            ) as client:  # Use configurable timeout
                response = client.post(BATCH_URL, headers=headers, json=chunk)
                response.raise_for_status()
                results.append(response.json())
                logger.info(
                    f"Successfully submitted chunk {chunk_number}/{total_chunks}"
                )
        except httpx.HTTPError as e:
            logger.error(
                f"Failed to submit batch chunk {chunk_number}/{total_chunks}: {e}"
            )
            raise

    return {
        "status": "success",
        "message": f"Successfully submitted {len(batch_data)} requests in {len(results)} chunks",
        "results": results,
    }


def process_data():
    """Main function to execute the data pipeline, designed to be called by a Celery task"""
    try:
        # Step 1: Get access token
        logger.info("Getting access token...")
        token = get_access_token()

        # Step 2: Fetch both online and offline sales data
        logger.info("Fetching online data...")
        online_data = fetch_data(token, is_online=True)

        logger.info("Fetching offline data...")
        offline_data = fetch_data(token, is_online=False)

        # Log the raw data counts
        logger.info(
            f"Raw data counts - Online: {len(online_data)}, Offline: {len(offline_data)}"
        )

        # Combine all data before filtering negative records
        all_data = online_data + offline_data

        # Step 3: Filter negative records (Doanh_Thu or So_Luong < 0)
        logger.info("Filtering negative records...")
        negative_records = filter_negative_records(all_data)
        logger.info(f"Found {len(negative_records)} negative records")

        # Step 4: Apply filters to both datasets
        logger.info("Applying filters to online data...")
        filtered_online_data = apply_filters(online_data, is_online=True)

        logger.info("Applying filters to offline data...")
        filtered_offline_data = apply_filters(offline_data, is_online=False)

        # Step 5: Combine filtered data
        all_filtered_data = filtered_online_data + filtered_offline_data
        logger.info(
            f"Total filtered records: {len(all_filtered_data)} (Online: {len(filtered_online_data)}, Offline: {len(filtered_offline_data)})"
        )

        # Log sample data if available for debugging
        if online_data:
            logger.info(
                f"Sample online record: {online_data[0] if online_data else 'None'}"
            )
        if offline_data:
            logger.info(
                f"Sample offline record: {offline_data[0] if offline_data else 'None'}"
            )

        # Step 6: Transform data
        logger.info("Transforming data...")
        batch_data = transform_data(all_filtered_data)
        logger.info(f"Transformed data into {len(batch_data)} batch requests")

        # Step 7: Submit batch
        if batch_data:
            logger.info("Submitting batch...")
            result = submit_batch(batch_data)
            logger.info(f"Batch submission result: {result}")

            # Step 8: Send completion email with Excel attachments
            logger.info("Sending completion email...")
            send_completion_email(
                filtered_online_data, filtered_offline_data, negative_records
            )

            return result
        else:
            logger.warning(
                "No data to submit after filtering and transformation"
            )
            # Send email even if no data to submit
            send_completion_email([], [], negative_records)
            return {"status": "no_data", "message": "No data to submit"}

    except Exception as e:
        logger.error(f"An error occurred in the data pipeline: {e}")
        raise


def create_excel_file(data: List[Dict], filename: str) -> str:
    """Create an Excel file with the specified headers from the data.

    Args:
        data: List of dictionaries containing the data
        filename: Name for the temporary file

    Returns:
        Path to the created Excel file
    """
    # Define the required headers
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

    # Create a DataFrame with the specified columns
    # For each row, map the data to the required headers
    rows = []
    for item in data:
        row = {}
        for header in headers:
            # Map the header to the corresponding field in the data
            field_mapping = {
                "Ngày Ct": "Ngay_Ct",
                "Mã Ct": "Ma_Ct",
                "Số Ct": "So_Ct",
                "Mã bộ phận": "Ma_BP",
                "Mã đơn hàng": "Ma_Don_Hang",
                "Tên khách hàng": "Ten_Khach_Hang",
                "Số điện thoại": "So_Dien_Thoai",
                "Tỉnh thành": "Tinh_Thanh",
                "Quận huyện": "Quan_Huyen",
                "Phường xã": "Phuong_Xa",
                "Địa chỉ": "Dia_Chi",
                "Mã hàng": "Ma_Hang",
                "Tên hàng": "Ten_Hang",
                "Imei": "Imei",
                "Số lượng": "So_Luong",
                "Doanh thu": "Doanh_Thu",
                "Ghi chú": "Ghi_Chu",
            }

            # Get the corresponding field name or use None if not found
            field_name = field_mapping.get(header)
            if field_name and field_name in item:
                row[header] = item[field_name]
            else:
                row[header] = None  # Fill with null if field doesn't exist

        rows.append(row)

    # Create DataFrame and save to Excel
    df = pd.DataFrame(rows, columns=headers)

    # Create a temporary file
    with NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        temp_file_path = tmp.name

    # Save DataFrame to Excel
    df.to_excel(temp_file_path, index=False, sheet_name="Data")

    return temp_file_path


def filter_negative_records(data: List[Dict]) -> List[Dict]:
    """Filter records with negative Doanh_Thu or So_Luong values.

    Args:
        data: List of dictionaries containing the data

    Returns:
        List of dictionaries with negative values
    """
    negative_records = []

    for item in data:
        # Check if Doanh_Thu or So_Luong is negative
        doanh_thu = item.get("Doanh_Thu", 0)
        so_luong = item.get("So_Luong", 0)

        # Convert to float for comparison if possible
        try:
            doanh_thu = float(doanh_thu) if doanh_thu is not None else 0
            so_luong = float(so_luong) if so_luong is not None else 0
        except (ValueError, TypeError):
            # If conversion fails, assume non-negative
            doanh_thu = 0
            so_luong = 0

        if doanh_thu < 0 or so_luong < 0:
            negative_records.append(item)

    return negative_records


def send_completion_email(
    filtered_online_data: List[Dict],
    filtered_offline_data: List[Dict],
    negative_records: List[Dict],
):
    """Send completion email with Excel attachments.

    Args:
        filtered_online_data: Final processed online data
        filtered_offline_data: Final processed offline data
        negative_records: Records with negative values
    """
    try:
        # Create separate Excel files for online and offline data
        online_data_file = create_excel_file(
            filtered_online_data, "online.xlsx"
        )
        offline_data_file = create_excel_file(
            filtered_offline_data, "offline.xlsx"
        )
        negative_records_file = create_excel_file(
            negative_records, "negative_records.xlsx"
        )

        # Send email with all attachments
        subject = "CRM Data Processing Completed"
        body = f"""
        Xử lý dữ liệu CRM thành công.

        Tóm tắt:

        Dữ liệu online: {len(filtered_online_data)} bản ghi
        Dữ liệu offline: {len(filtered_offline_data)} bản ghi
        Bản ghi âm (Doanh thu hoặc Số lượng < 0): {len(negative_records)} bản ghi
        Vui lòng xem các tệp Excel đính kèm để tham khảo:
        online.xlsx: Chứa dữ liệu online đã lọc
        offline.xlsx: Chứa dữ liệu offline đã lọc
        negative_records.xlsx: Chứa các bản ghi có giá trị âm

        Đây là email được gửi tự động.
        """

        result = send_notification_email(
            to=["nam.nguyen@lug.vn", "songkhoi123@gmail.com"],
            subject=subject,
            body=body,
            attachment_paths=[
                online_data_file,
                offline_data_file,
                negative_records_file,
            ],
        )

        if result:
            logger.info("Completion email sent successfully")
        else:
            logger.warning("Failed to send completion email")

        # Clean up temporary files
        import os

        try:
            os.unlink(online_data_file)
            os.unlink(offline_data_file)
            os.unlink(negative_records_file)
        except Exception as e:
            logger.warning(f"Failed to clean up temporary files: {e}")

    except Exception as e:
        logger.error(f"Failed to send completion email: {e}", exc_info=True)
        # Re-raise the exception so it can be handled by the calling function
        raise


# Kept for backward compatibility
def main():
    """Main function for manual script execution"""
    return process_data()


if __name__ == "__main__":
    main()
