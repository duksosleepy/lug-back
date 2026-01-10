import asyncio
import io
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, List

import httpx
import pandas as pd

from src.processors.product_mapping_processor import ProductMappingProcessor
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
    "Mã vật tư": "Ma_Hang_Old",
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
                "values": [
                    "PBHDT",
                    "THUNG",
                    "DVVC_ONL",
                    "TUINILONPK",
                    "THECAO",
                    "TUI GIAY LON",
                    "BAOTRUMNILON_20",
                    "PBH_VANG",
                    "TUIGIAY_25X10X34",
                    "TUIGIAY",
                    "TUINILON",
                ],
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
                    "THECAO",
                    "TUI GIAY LON",
                    "BAOTRUMNILON_20",
                    "PBH_VANG",
                    "TUIGIAY_25X10X34",
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

    # Calculate date range: from 12:00 AM 5 days ago to 12:00 AM 4 days ago
    task_runtime = datetime.now()
    current_day_midnight = task_runtime.replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    target_day_start = current_day_midnight - timedelta(days=3)
    target_day_end = current_day_midnight - timedelta(days=2)

    # Format dates for API query: >= 5 days ago 00:00:00, < 4 days ago 00:00:00
    date_gte = target_day_start.strftime("%Y-%m-%dT00:00:00")
    date_lte = target_day_end.strftime("%Y-%m-%dT00:00:00")

    # Construct date filter
    date_filter = {
        "_and": [
            {"Ngay_Ct": {"_gte": date_gte}},
            {"Ngay_Ct": {"_lt": date_lte}},
        ]
    }

    # Parameters for the API request
    headers = {"authorization": f"Bearer {token}"}
    params = {
        "limit": -1,  # Get all records
        "filter": json.dumps(date_filter),
    }

    logger.info(f"Fetching {source_type} data from {date_gte} to {date_lte}")
    logger.info(f"Task runtime: {task_runtime}")

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
    """Validate Vietnamese phone number format"""
    if not phone:
        return False

    # Remove any spaces, dashes, dots, or parentheses
    phone = re.sub(r"[\s\-\.\(\)]", "", phone)

    # Vietnamese phone number patterns:
    # - Mobile: 0 + 3/5/7/8/9 + 8 digits = 10 digits total
    # - Or with +84: +84 + 3/5/7/8/9 + 8 digits
    # - Or with 84: 84 + 3/5/7/8/9 + 8 digits (without +)
    # - Or with international codes: 001 + 0 + 3/5/7/8/9 + 8 digits
    patterns = [
        r"^0[35789][0-9]{8}$",  # 0 + mobile prefix + 8 digits
        r"^\+84[35789][0-9]{8}$",  # +84 + mobile prefix + 8 digits
        r"^84[35789][0-9]{8}$",  # 84 + mobile prefix + 8 digits (without +)
        r"^001[035789][0-9]{8}$",  # 001 + 0/3/5/7/8/9 + 8 digits
    ]

    return any(re.match(pattern, phone) for pattern in patterns)


def apply_filters(
    data: List[Dict], is_online: bool
) -> tuple[List[Dict], List[Dict], List[Dict]]:
    """Apply filter rules to the data based on processor type and collect invalid phone records and KL phone records"""
    processor_key = "online_processor" if is_online else "offline_processor"
    filters = FILTER_RULES[processor_key]["filters"]
    filtered_data = []
    invalid_phone_records = []
    kl_phone_records = []

    for item in data:
        should_remove = False
        is_invalid_phone = False
        is_kl_phone = False

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
                # Special handling for KL phone number filter
                if filter_rule.get("name") == "kl_test_records_filter":
                    phone_value = str(value) if value else ""
                    if phone_value in ["0912345678"]:
                        is_kl_phone = True
                        should_remove = True
                elif value and str(value).upper() in [
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
                    phone_value = str(value) if value else ""
                    # Check if phone is invalid and not the test phone number
                    if not is_valid_phone(phone_value) and phone_value not in [
                        "0912345678",
                    ]:
                        is_invalid_phone = True
                        should_remove = True

            # If any filter marks the item for removal, break early
            if should_remove:
                break

        # Collect KL phone records (test phone number)
        if is_kl_phone:
            kl_phone_records.append(item)

        # Collect invalid phone records (excluding test phone)
        if is_invalid_phone:
            invalid_phone_records.append(item)

        if not should_remove:
            filtered_data.append(item)

    logger.info(
        f"Filtered {len(data)} records down to {len(filtered_data)} records"
    )
    logger.info(
        f"Found {len(invalid_phone_records)} records with invalid phone numbers"
    )
    logger.info(f"Found {len(kl_phone_records)} records with KL phone numbers")
    return filtered_data, invalid_phone_records, kl_phone_records


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
                    "maHang": item.get("Ma_Hang_Old"),
                    "tenHang": item.get("Ten_Hang"),
                    "imei": item.get("Imei"),
                    "soLuong": item.get("So_Luong"),
                    "doanhThu": item.get("Doanh_Thu") or 0,
                }
            ],
        }

        # Add to the batch data
        transformed_data.append(
            {"url": TARGET_URL, "data": {"apikey": API_KEY, "data": [record]}}
        )

    return transformed_data


async def send_kl_records_to_api(kl_records: List[Dict]) -> Dict:
    """Send KL phone number records (0912345678 only) to the API endpoint"""
    if not kl_records:
        logger.info("No KL phone records to send")
        return {"status": "no_data", "message": "No KL phone records to send"}

    # Safety filter: Only process records with phone number 0912345678
    kl_records_filtered = [
        record
        for record in kl_records
        if record.get("So_Dien_Thoai") == "0912345678"
    ]

    if not kl_records_filtered:
        logger.warning(
            f"Filtered out {len(kl_records)} records - none had phone number 0912345678"
        )
        return {
            "status": "no_data",
            "message": "No records with phone 0912345678 to send",
        }

    if len(kl_records_filtered) != len(kl_records):
        logger.warning(
            f"Filtered KL records: {len(kl_records)} -> {len(kl_records_filtered)} "
            f"(removed {len(kl_records) - len(kl_records_filtered)} non-0912345678 records)"
        )

    try:
        # Get the API endpoint and token from settings
        api_endpoint = app_settings.get_env("API_ENDPOINT")
        xc_token = app_settings.get_env("XC_TOKEN")

        if not api_endpoint:
            logger.error("API_ENDPOINT environment variable not set")
            return {"status": "error", "message": "API endpoint not configured"}

        if not xc_token:
            logger.warning("XC_TOKEN environment variable not set")
            xc_token = ""

        # Transform KL records to the required API format
        transformed_records = []
        for record in kl_records_filtered:
            # Convert date format from datetime to YYYY-MM-DD if needed
            date_str = record.get("Ngay_Ct", "")
            if date_str and len(date_str) >= 10:
                date_str = date_str[:10]

            # Format numeric values
            def format_numeric(value):
                if value is None:
                    return ""
                if (
                    isinstance(value, (int, float))
                    and float(value).is_integer()
                ):
                    return str(int(value))
                return str(value)

            transformed_record = {
                "ngay_ct": date_str,
                "ma_ct": record.get("Ma_Ct", ""),
                "so_ct": str(record.get("So_Ct", "")).zfill(4)
                if record.get("So_Ct")
                else "",
                "ma_bo_phan": record.get("Ma_BP", ""),
                "ma_don_hang": record.get("Ma_Don_Hang", ""),
                "ten_khach_hang": record.get("Ten_Khach_Hang", ""),
                "so_dien_thoai": record.get("So_Dien_Thoai", ""),
                "tinh_thanh": record.get("Tinh_Thanh", ""),
                "quan_huyen": record.get("Quan_Huyen", ""),
                "phuong_xa": record.get("Phuong_Xa", ""),
                "dia_chi": record.get("Dia_Chi", ""),
                "ma_hang": record.get("Ma_Hang_Old", ""),
                "ten_hang": record.get("Ten_Hang", ""),
                "imei": record.get("Imei", ""),
                "so_luong": format_numeric(record.get("So_Luong")),
                "doanh_thu": format_numeric(record.get("Doanh_Thu")),
                "ghi_chu": "KL test phone number record",
            }
            transformed_records.append(transformed_record)

        # Send data to API
        url = f"{api_endpoint}/tables/mtvvlryi3xc0gqd/records"
        headers = {
            "Content-Type": "application/json",
            "xc-token": xc_token,
        }

        logger.info(
            f"Sending {len(transformed_records)} KL phone records to API: {url}"
        )

        with httpx.Client(timeout=BATCH_TIMEOUT) as client:
            response = client.post(
                url, headers=headers, json=transformed_records
            )
            response.raise_for_status()

            logger.info(
                f"Successfully sent {len(transformed_records)} KL phone records to API"
            )
            return {
                "status": "success",
                "message": f"Successfully sent {len(transformed_records)} KL phone records to API",
                "records_sent": len(transformed_records),
            }

    except httpx.HTTPError as e:
        logger.error(f"Failed to send KL phone records to API: {e}")
        return {
            "status": "error",
            "message": f"Failed to send KL phone records to API: {e}",
            "records_attempted": len(kl_records_filtered),
        }
    except Exception as e:
        logger.error(f"Unexpected error sending KL phone records: {e}")
        return {
            "status": "error",
            "message": f"Unexpected error sending KL phone records: {e}",
            "records_attempted": len(kl_records_filtered),
        }


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
                response_data = response.json()
                results.append(response_data)
                logger.info(
                    f"Successfully submitted chunk {chunk_number}/{total_chunks}"
                )

                # Log detailed response information
                _log_batch_response(response_data, chunk_number)

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


def _log_batch_response(response_data: Dict, chunk_number: int) -> None:
    """Parse and log detailed batch submission response including errors.

    Args:
        response_data: The response from the batch submission API
        chunk_number: The chunk number being processed
    """
    try:
        # Log basic response information
        message = response_data.get("message", "")
        task_ids = response_data.get("task_ids", [])
        info = response_data.get("info", "")
        errors = response_data.get("errors", [])

        logger.info(f"Chunk {chunk_number} - Response message: {message}")
        logger.info(
            f"Chunk {chunk_number} - Successfully submitted task IDs: {len(task_ids)} tasks"
        )

        if info:
            logger.info(f"Chunk {chunk_number} - Info: {info}")

        # Log error details if any
        if errors:
            logger.warning(
                f"Chunk {chunk_number} - Found {len(errors)} error(s) in batch submission"
            )

            for idx, error in enumerate(errors, 1):
                task_id = error.get("task_id", "Unknown")
                error_msg = error.get("error", "Unknown error")
                status_code = error.get("status_code", "Unknown")
                response_data_error = error.get("response_data", {})

                # Extract specific error code if available
                error_code = response_data_error.get("errorCode", "")

                logger.error(
                    f"Chunk {chunk_number} - Error {idx}/{len(errors)}: "
                    f"Task ID: {task_id}, Status Code: {status_code}, "
                    f"Error: {error_msg}"
                )

                if error_code:
                    logger.error(
                        f"Chunk {chunk_number} - Error Code: {error_code}"
                    )

                if response_data_error:
                    logger.error(
                        f"Chunk {chunk_number} - Response Data: {json.dumps(response_data_error, ensure_ascii=False)}"
                    )
        else:
            logger.info(f"Chunk {chunk_number} - No errors in batch submission")

    except Exception as e:
        logger.warning(
            f"Error while logging batch response details for chunk {chunk_number}: {e}"
        )


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
        filtered_online_data, invalid_online_phones, kl_online_phones = (
            apply_filters(online_data, is_online=True)
        )

        logger.info("Applying filters to offline data...")
        filtered_offline_data, invalid_offline_phones, kl_offline_phones = (
            apply_filters(offline_data, is_online=False)
        )

        # Step 5: Combine filtered data
        all_filtered_data = filtered_online_data + filtered_offline_data
        logger.info(
            f"Total filtered records: {len(all_filtered_data)} (Online: {len(filtered_online_data)}, Offline: {len(filtered_offline_data)})"
        )

        # Step 5.1: Split records where So_Luong > 1
        logger.info("Splitting records with So_Luong > 1...")
        all_filtered_data = split_quantity_records(all_filtered_data)

        # Step 5.2: Apply product mapping to records with Ma_Hang field
        logger.info("Applying product mapping to all filtered data...")
        all_filtered_data = apply_product_mapping(all_filtered_data)

        # Step 5.3: Split the processed data back into online/offline for email reporting
        # Determine which records are online vs offline based on original data IDs or other identifiers
        split_online_data = []
        split_offline_data = []

        # Create sets of original record identifiers for fast lookup
        online_ids = {
            (
                item.get("So_Ct"),
                item.get("Ma_Ct"),
                item.get("Ten_Khach_Hang"),
                item.get("So_Dien_Thoai"),
            )
            for item in filtered_online_data
        }

        for item in all_filtered_data:
            record_id = (
                item.get("So_Ct"),
                item.get("Ma_Ct"),
                item.get("Ten_Khach_Hang"),
                item.get("So_Dien_Thoai"),
            )
            if record_id in online_ids:
                split_online_data.append(item)
            else:
                split_offline_data.append(item)

        # Update the variables used for email reporting
        filtered_online_data = split_online_data
        filtered_offline_data = split_offline_data

        logger.info(
            f"After splitting - Online: {len(filtered_online_data)}, Offline: {len(filtered_offline_data)}"
        )

        # Count special phone numbers for statistics FROM FILTERED DATA (after filtering)
        # Count Khach khong cho thong tin from records with phone 0999999999 only
        all_final_filtered_data = filtered_online_data + filtered_offline_data
        kkctt_count = sum(
            1
            for record in all_final_filtered_data
            if record.get("So_Dien_Thoai") == "0999999999"
        )

        # Count Khach nuoc ngoai from the final filtered data (0900000000 only)
        khach_nuoc_ngoai_count = sum(
            1
            for record in all_final_filtered_data
            if record.get("So_Dien_Thoai") == "0900000000"
        )

        logger.info(f"Khach khong cho thong tin count: {kkctt_count}")
        logger.info(f"Khach nuoc ngoai count: {khach_nuoc_ngoai_count}")

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

        # Step 7: Send KL phone records to API if any exist (before batch submission)
        # Filter to ensure only 0912345678 records are sent
        all_kl_records = kl_online_phones + kl_offline_phones
        kl_records = [
            record
            for record in all_kl_records
            if record.get("So_Dien_Thoai") == "0912345678"
        ]
        if kl_records:
            logger.info(
                f"Sending {len(kl_records)} KL phone records (0912345678 only) to API..."
            )
            kl_result = asyncio.run(send_kl_records_to_api(kl_records))
            logger.info(f"KL phone records API result: {kl_result}")

        # Step 8: Send invalid phone records email if any exist (before batch submission)
        if invalid_online_phones or invalid_offline_phones:
            logger.info("Sending invalid phone records email...")
            send_invalid_phone_email(
                invalid_online_phones, invalid_offline_phones
            )

        # Step 9: Submit batch
        if batch_data:
            logger.info("Submitting batch...")
            result = submit_batch(batch_data)
            logger.info(f"Batch submission result: {result}")

            # Step 10: Send completion email with Excel attachments
            logger.info("Sending completion email...")
            send_completion_email(
                filtered_online_data,
                filtered_offline_data,
                negative_records,
                kkctt_count,
                khach_nuoc_ngoai_count,
            )

            return result
        else:
            logger.warning(
                "No data to submit after filtering and transformation"
            )
            # Send email even if no data to submit
            send_completion_email(
                [], [], negative_records, kkctt_count, khach_nuoc_ngoai_count
            )
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
                "Mã hàng": "Ma_Hang_Old",
                "Tên hàng": "Ten_Hang",
                "Imei": "Imei",
                "Số lượng": "So_Luong",
                "Doanh thu": "Doanh_Thu",
                "Ghi chú": "Ghi_Chu",
            }

            # Get the corresponding field name or use None if not found
            field_name = field_mapping.get(header)
            if header == "Ghi chú":
                row[header] = ""  # Always set Ghi chú to empty string
            elif field_name and field_name in item:
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


def split_quantity_records(data: List[Dict]) -> List[Dict]:
    """Split records where So_Luong > 1 into multiple records with So_Luong = 1
    and divide Doanh_Thu proportionally

    Args:
        data: List of dictionaries containing the data

    Returns:
        List of dictionaries with split records
    """
    split_data = []
    original_count = len(data)

    for item in data:
        so_luong = item.get("So_Luong", 1)
        doanh_thu = item.get("Doanh_Thu", 0)

        # Convert to numeric for processing
        try:
            so_luong = int(float(so_luong)) if so_luong is not None else 1
        except (ValueError, TypeError):
            so_luong = 1

        try:
            doanh_thu = float(doanh_thu) if doanh_thu is not None else 0
        except (ValueError, TypeError):
            doanh_thu = 0

        if so_luong > 1:
            # Calculate Doanh_Thu per unit
            doanh_thu_per_unit = doanh_thu / so_luong

            # Create multiple records with So_Luong = 1 and split Doanh_Thu
            for _ in range(so_luong):
                new_record = item.copy()
                new_record["So_Luong"] = 1
                new_record["Doanh_Thu"] = doanh_thu_per_unit
                split_data.append(new_record)
        else:
            # Keep original record if So_Luong <= 1
            split_data.append(item)

    logger.info(
        f"Split {original_count} records into {len(split_data)} records based on So_Luong"
    )
    return split_data


def apply_product_mapping(data: List[Dict]) -> List[Dict]:
    """Apply product mapping to records with Ma_Hang field using ProductMappingProcessor.

    Args:
        data: List of dictionaries containing the data with Ma_Hang field

    Returns:
        List of dictionaries with updated Ma_Hang values after mapping
    """
    if not data:
        logger.info("No data to apply product mapping")
        return data

    # Check if mapping file exists
    mapping_file_path = Path(app_settings.product_mapping_file)
    if not mapping_file_path.exists():
        logger.warning(
            f"Product mapping file not found at {mapping_file_path}. Skipping product mapping."
        )
        return data

    try:
        # Filter records that have Ma_Hang field
        records_with_ma_hang = [
            item for item in data if item.get("Ma_Hang_Old")
        ]

        if not records_with_ma_hang:
            logger.info(
                "No records with Ma_Hang field found for product mapping"
            )
            return data

        logger.info(
            f"Applying product mapping to {len(records_with_ma_hang)} records with Ma_Hang field"
        )

        # Convert data to DataFrame for processing
        df = pd.DataFrame(records_with_ma_hang)

        # Rename columns to match ProductMappingProcessor expectations
        if "Ma_Hang_Old" in df.columns:
            df["Mã hàng"] = df["Ma_Hang_Old"]
        if "Ten_Hang" in df.columns:
            df["Tên hàng"] = df["Ten_Hang"]

        # Create temporary file for data
        with NamedTemporaryFile(suffix=".xlsx", delete=False) as temp_data_file:
            df.to_excel(temp_data_file.name, index=False)
            temp_data_path = temp_data_file.name

        try:
            # Create output buffer
            output_buffer = io.BytesIO()

            # Initialize ProductMappingProcessor
            processor = ProductMappingProcessor(
                data_file=temp_data_path, mapping_file=str(mapping_file_path)
            )

            # Process the mapping
            mapping_result = processor.process_to_buffer(output_buffer)

            # Read the processed data back
            output_buffer.seek(0)
            processed_df = pd.read_excel(output_buffer, engine="openpyxl")

            # Convert back to list of dictionaries
            processed_records = processed_df.to_dict("records")

            # Update original data with mapped values
            updated_data = []
            processed_index = 0

            for item in data:
                if item.get("Ma_Hang_Old"):
                    if processed_index < len(processed_records):
                        processed_item = processed_records[processed_index]
                        # Update Ma_Hang and Ten_Hang if they were mapped
                        if "Mã hàng" in processed_item:
                            item["Ma_Hang_Old"] = processed_item["Mã hàng"]
                        if "Tên hàng" in processed_item and "Ten_Hang" in item:
                            item["Ten_Hang"] = re.sub(
                                r"\s+", " ", str(processed_item["Tên hàng"])
                            ).strip()
                        processed_index += 1

                updated_data.append(item)

            logger.info(
                f"Product mapping completed. Mapped {mapping_result.get('matched_count', 0)} out of {mapping_result.get('total_count', 0)} records"
            )

            return updated_data

        finally:
            # Clean up temporary file
            try:
                os.unlink(temp_data_path)
            except Exception as e:
                logger.warning(
                    f"Failed to clean up temporary file {temp_data_path}: {e}"
                )

    except Exception as e:
        logger.error(f"Error applying product mapping: {e}", exc_info=True)
        logger.warning("Continuing without product mapping due to error")
        return data


def send_completion_email(
    filtered_online_data: List[Dict],
    filtered_offline_data: List[Dict],
    negative_records: List[Dict],
    kkctt_count: int,
    khach_nuoc_ngoai_count: int,
):
    """Send completion email with Excel attachments.

    Args:
        filtered_online_data: Final processed online data
        filtered_offline_data: Final processed offline data
        negative_records: Records with negative values
        kkctt_count: Number of records with phone 0999999999 (khach khong cho thong tin)
        khach_nuoc_ngoai_count: Number of records with phone 0900000000 (khach nuoc ngoai)
    """
    try:
        # Generate timestamp for filenames
        task_runtime = datetime.now()
        previous_day_same_time = task_runtime - timedelta(days=2)

        # Format date for filename: YYYYMMDD (use previous day as reference)
        date_str = previous_day_same_time.strftime("%Y%m%d")

        # Create separate Excel files with shorter naming
        online_data_file = create_excel_file(
            filtered_online_data, f"CRM_Online_Data_{date_str}.xlsx"
        )
        offline_data_file = create_excel_file(
            filtered_offline_data, f"CRM_Offline_Data_{date_str}.xlsx"
        )
        negative_records_file = create_excel_file(
            negative_records, f"CRM_Negative_Records_{date_str}.xlsx"
        )

        # Send email with all attachments using shorter names
        subject = f"CRM Data Processing Completed - {date_str}"
        # Calculate the actual data retrieval date range (same logic as in fetch_data)
        current_day_midnight = task_runtime.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        target_day_start = current_day_midnight - timedelta(days=3)

        body = f"""
        Xử lý dữ liệu CRM thành công.

        Khoảng thời gian: {target_day_start.strftime("%Y-%m-%d")}

        Tóm tắt:
        - Dữ liệu online: {len(filtered_online_data)} bản ghi
        - Dữ liệu offline: {len(filtered_offline_data)} bản ghi
        - Bản ghi âm (Doanh thu hoặc Số lượng < 0): {len(negative_records)} bản ghi

        Các tệp đính kèm:
        - CRM_Online_Data_{date_str}.xlsx: Dữ liệu online đã lọc
        - CRM_Offline_Data_{date_str}.xlsx: Dữ liệu offline đã lọc
        - CRM_Negative_Records_{date_str}.xlsx: Các bản ghi có giá trị âm

        Số lượng khách không cho thông tin: {kkctt_count}
        Số lượng khách nước ngoài: {khach_nuoc_ngoai_count}

        Đây là email được gửi tự động.
        """

        result = send_notification_email(
            to=[
                "nam.nguyen@lug.vn",
                "songkhoi123@gmail.com",
                "dang.le@sangtam.com",
            ],
            subject=subject,
            body=body,
            attachment_paths=[
                {
                    "path": online_data_file,
                    "name": f"CRM_Online_Data_{date_str}.xlsx",
                },
                {
                    "path": offline_data_file,
                    "name": f"CRM_Offline_Data_{date_str}.xlsx",
                },
                {
                    "path": negative_records_file,
                    "name": f"CRM_Negative_Records_{date_str}.xlsx",
                },
            ],
        )

        if result:
            logger.info("Completion email sent successfully")
        else:
            logger.warning("Failed to send completion email")

        # Clean up temporary files
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


def send_invalid_phone_email(
    invalid_online_records: List[Dict],
    invalid_offline_records: List[Dict],
):
    """Send email with invalid phone number records.

    Args:
        invalid_online_records: Online records with invalid phone numbers
        invalid_offline_records: Offline records with invalid phone numbers
    """
    try:
        # Generate timestamp for filenames
        task_runtime = datetime.now()
        date_str = task_runtime.strftime("%Y%m%d_%H%M%S")

        attachment_paths = []

        # Create Excel files only if there are records
        if invalid_online_records:
            online_invalid_file = create_excel_file(
                invalid_online_records, f"Invalid_Phone_Online_{date_str}.xlsx"
            )
            attachment_paths.append(
                {
                    "path": online_invalid_file,
                    "name": f"Invalid_Phone_Online_{date_str}.xlsx",
                }
            )

        if invalid_offline_records:
            offline_invalid_file = create_excel_file(
                invalid_offline_records,
                f"Invalid_Phone_Offline_{date_str}.xlsx",
            )
            attachment_paths.append(
                {
                    "path": offline_invalid_file,
                    "name": f"Invalid_Phone_Offline_{date_str}.xlsx",
                }
            )

        # Only send email if there are invalid records
        if not attachment_paths:
            logger.info("No invalid phone records to send")
            return

        # Email recipients based on data type
        offline_recipients = [
            "songkhoi123@gmail.com",
            "nam.nguyen@lug.vn",
            "dang.le@sangtam.com",
            "tan.nguyen@sangtam.com",
        ]

        online_recipients = [
            "songkhoi123@gmail.com",
            "nam.nguyen@lug.vn",
            "dang.le@sangtam.com",
            "tan.nguyen@sangtam.com",
            "kiet.huynh@sangtam.com",
        ]

        # Send separate emails for online and offline if both have invalid records
        if invalid_online_records and invalid_offline_records:
            # Send online invalid records
            subject_online = f"Invalid Phone Numbers - Online Data - {date_str}"
            body_online = f"""
            Phát hiện số điện thoại không hợp lệ trong dữ liệu CRM Online.

            Thời gian kiểm tra: {task_runtime.strftime("%Y-%m-%d %H:%M:%S")}

            Tóm tắt:
            - Số bản ghi online có số điện thoại không hợp lệ: {len(invalid_online_records)}

            Tệp đính kèm:
            - Invalid_Phone_Online_{date_str}.xlsx: Dữ liệu online có số điện thoại không hợp lệ

            Lưu ý: Đây không bao gồm số điện thoại test (0912345678).

            Đây là email được gửi tự động.
            """

            send_notification_email(
                to=online_recipients,
                subject=subject_online,
                body=body_online,
                attachment_paths=[attachment_paths[0]],  # Online file
            )

            # Send offline invalid records
            subject_offline = (
                f"Invalid Phone Numbers - Offline Data - {date_str}"
            )
            body_offline = f"""
            Phát hiện số điện thoại không hợp lệ trong dữ liệu CRM Offline.

            Thời gian kiểm tra: {task_runtime.strftime("%Y-%m-%d %H:%M:%S")}

            Tóm tắt:
            - Số bản ghi offline có số điện thoại không hợp lệ: {len(invalid_offline_records)}

            Tệp đính kèm:
            - Invalid_Phone_Offline_{date_str}.xlsx: Dữ liệu offline có số điện thoại không hợp lệ

            Lưu ý: Đây không bao gồm số điện thoại test (0912345678).

            Đây là email được gửi tự động.
            """

            send_notification_email(
                to=offline_recipients,
                subject=subject_offline,
                body=body_offline,
                attachment_paths=[attachment_paths[1]],  # Offline file
            )

        elif invalid_online_records:
            # Send only online records
            subject = f"Invalid Phone Numbers - Online Data - {date_str}"
            body = f"""
            Phát hiện số điện thoại không hợp lệ trong dữ liệu CRM Online.

            Thời gian kiểm tra: {task_runtime.strftime("%Y-%m-%d %H:%M:%S")}

            Tóm tắt:
            - Số bản ghi online có số điện thoại không hợp lệ: {len(invalid_online_records)}

            Tệp đính kèm:
            - Invalid_Phone_Online_{date_str}.xlsx: Dữ liệu online có số điện thoại không hợp lệ

            Lưu ý: Đây không bao gồm số điện thoại test (0912345678).

            Đây là email được gửi tự động.
            """

            send_notification_email(
                to=online_recipients,
                subject=subject,
                body=body,
                attachment_paths=attachment_paths,
            )

        elif invalid_offline_records:
            # Send only offline records
            subject = f"Invalid Phone Numbers - Offline Data - {date_str}"
            body = f"""
            Phát hiện số điện thoại không hợp lệ trong dữ liệu CRM Offline.

            Thời gian kiểm tra: {task_runtime.strftime("%Y-%m-%d %H:%M:%S")}

            Tóm tắt:
            - Số bản ghi offline có số điện thoại không hợp lệ: {len(invalid_offline_records)}


            Tệp đính kèm:
            - Invalid_Phone_Offline_{date_str}.xlsx: Dữ liệu offline có số điện thoại không hợp lệ
            Lưu ý: Đây không bao gồm số điện thoại test (0912345678).

            Đây là email được gửi tự động.
            """

            send_notification_email(
                to=offline_recipients,
                subject=subject,
                body=body,
                attachment_paths=attachment_paths,
            )

        logger.info("Invalid phone records email sent successfully")

        # Clean up temporary files
        for attachment in attachment_paths:
            try:
                os.unlink(attachment["path"])
            except Exception as e:
                logger.warning(
                    f"Failed to clean up temporary file {attachment['path']}: {e}"
                )

    except Exception as e:
        logger.error(
            f"Failed to send invalid phone records email: {e}", exc_info=True
        )


# Kept for backward compatibility
def main():
    """Main function for manual script execution"""
    return process_data()


if __name__ == "__main__":
    main()
