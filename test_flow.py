#!/usr/bin/env python3
"""Manual test script for CRM flow with custom imei filter and detailed response logging"""

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import httpx

# Add parent directory to path to import src modules
sys.path.insert(0, str(Path(__file__).parent))

from src.settings import app_settings
from src.util.logging import get_logger

logger = get_logger(__name__)

# API endpoints and credentials
AUTH_URL = app_settings.get_env("CRM_AUTH_URL")
ONLINE_DATA_URL = app_settings.get_env("CRM_ONLINE_DATA_URL")
OFFLINE_DATA_URL = app_settings.get_env("CRM_OFFLINE_DATA_URL")
IMPORT_URL = "https://crm.lug.center/v1/importdonhang/import"
TARGET_URL = app_settings.get_env("CRM_TARGET_URL")
API_KEY = app_settings.get_env("CRM_API_KEY")
BATCH_TIMEOUT = app_settings.get_int_env("CRM_BATCH_TIMEOUT", 60)

AUTH_CREDENTIALS = {
    "email": app_settings.get_env("CRM_AUTH_EMAIL"),
    "password": app_settings.get_env("CRM_AUTH_PASSWORD"),
}

# IMEI list for filtering
IMEI_LIST = [
    "DDLLST30082510000272",
    "DDLLST60292501000394",
    "DDLECH21522310000352",
    "DDLHOL30082509000017",
    "DDLNAT21722408000216",
]

# Field mapping
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

# Filter rules (same as in flow.py)
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


def print_response_details(response_data: Dict, request_type: str) -> None:
    """Print detailed response information to screen"""
    print("\n" + "=" * 80)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {request_type}")
    print("=" * 80)
    print(json.dumps(response_data, indent=2, ensure_ascii=False))
    print("=" * 80)


def get_access_token() -> str:
    """Authenticate with the API and return the access token"""
    print("\n[AUTH] Getting access token...")
    try:
        with httpx.Client(timeout=BATCH_TIMEOUT, verify=False) as client:
            response = client.post(AUTH_URL, json=AUTH_CREDENTIALS)
            response.raise_for_status()
            token = response.json()["data"]["access_token"]
            print(f"[AUTH] Successfully obtained access token: {token[:20]}...")
            return token
    except httpx.HTTPError as e:
        print(f"[ERROR] Authentication failed: {e}")
        raise


def fetch_data_with_imei(token: str, is_online: bool) -> List[Dict]:
    """Fetch sales data from the API using IMEI filter instead of date filter"""
    url = ONLINE_DATA_URL if is_online else OFFLINE_DATA_URL
    source_type = "online" if is_online else "offline"

    # Construct IMEI filter instead of date filter
    imei_filter = {"Imei": {"_in": IMEI_LIST}}

    # Parameters for the API request
    headers = {"authorization": f"Bearer {token}"}
    params = {
        "limit": -1,  # Get all records
        "filter": json.dumps(imei_filter),
    }

    print(
        f"\n[FETCH] Fetching {source_type} data with IMEI filter: {IMEI_LIST}"
    )
    print(
        f"[FETCH] Filter: {json.dumps(imei_filter, indent=2, ensure_ascii=False)}"
    )

    try:
        with httpx.Client(timeout=BATCH_TIMEOUT, verify=False) as client:
            response = client.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()["data"]

            # Print response details
            response_summary = {
                "source": source_type,
                "request_url": url,
                "request_filter": imei_filter,
                "records_retrieved": len(data),
                "status_code": response.status_code,
            }
            print_response_details(
                response_summary, f"FETCH {source_type.upper()} DATA"
            )

            # Print first record as sample if available
            if data:
                print(f"\n[FETCH] Sample {source_type} record:")
                print(json.dumps(data[0], indent=2, ensure_ascii=False))

            logger.info(
                f"Retrieved {len(data)} {source_type} sales records with IMEI filter"
            )
            return data
    except httpx.HTTPError as e:
        print(f"[ERROR] Failed to fetch {source_type} sales data: {e}")
        logger.error(f"Failed to fetch {source_type} sales data: {e}")
        raise


def is_valid_phone(phone: str) -> bool:
    """Validate Vietnamese phone number format"""
    import re

    if not phone:
        return False

    phone = re.sub(r"[\s\-\.\(\)]", "", phone)

    patterns = [
        r"^0[35789][0-9]{8}$",
        r"^\+84[35789][0-9]{8}$",
        r"^84[35789][0-9]{8}$",
        r"^001[035789][0-9]{8}$",
    ]

    return any(re.match(pattern, phone) for pattern in patterns)


def apply_filters(
    data: List[Dict], is_online: bool
) -> tuple[List[Dict], List[Dict], List[Dict]]:
    """Apply filter rules to the data"""
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
            column = filter_rule.get("column")
            mapped_column = FIELD_MAPPING.get(column) if column else None

            if filter_type == "complex_condition":
                continue

            if not mapped_column and filter_type != "complex_condition":
                continue

            value = item.get(mapped_column)

            if filter_type == "exclude_containing":
                for exclude_value in filter_rule["values"]:
                    if value and exclude_value in str(value).upper():
                        should_remove = True
                        break

            elif filter_type == "exclude_equals":
                if filter_rule.get("name") == "kl_test_records_filter":
                    phone_value = str(value) if value else ""
                    if phone_value == "0912345678":
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

            elif filter_type == "validation_function":
                function_name = filter_rule["function"]
                if function_name == "is_valid_phone":
                    phone_value = str(value) if value else ""
                    if (
                        not is_valid_phone(phone_value)
                        and phone_value != "0912345678"
                    ):
                        is_invalid_phone = True
                        should_remove = True

            if should_remove:
                break

        if is_kl_phone:
            kl_phone_records.append(item)

        if is_invalid_phone:
            invalid_phone_records.append(item)

        if not should_remove:
            filtered_data.append(item)

    source_type = "online" if is_online else "offline"
    print(f"\n[FILTER] {source_type.upper()} filtering summary:")
    print(f"  - Original records: {len(data)}")
    print(f"  - Filtered records: {len(filtered_data)}")
    print(f"  - Invalid phone records: {len(invalid_phone_records)}")
    print(f"  - KL phone records: {len(kl_phone_records)}")

    logger.info(
        f"Filtered {len(data)} records down to {len(filtered_data)} records ({source_type})"
    )

    return filtered_data, invalid_phone_records, kl_phone_records


def transform_data(sales_data: List[Dict]) -> List[Dict]:
    """Transform the sales data to the format required by the batch service"""
    transformed_data = []

    for item in sales_data:
        date_str = item.get("Ngay_Ct", "")
        if date_str and len(date_str) >= 10:
            date_str = date_str[:10]

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

        transformed_data.append(
            {"url": TARGET_URL, "data": {"apikey": API_KEY, "data": [record]}}
        )

    print(f"\n[TRANSFORM] Transformed {len(transformed_data)} records")
    return transformed_data


def submit_batch(batch_data: List[Dict]) -> Dict:
    """Submit the transformed data to the import endpoint - one request per IMEI"""
    if not batch_data:
        print("\n[WARNING] No data to submit after filtering")
        return {
            "status": "no_data",
            "message": "No data to submit after filtering",
        }

    print(
        f"\n[SUBMIT] Preparing to submit {len(batch_data)} records to import endpoint"
    )
    print(f"[SUBMIT] Import URL: {IMPORT_URL}")
    print("[SUBMIT] Note: Sending one IMEI per request")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    successful_submissions = 0
    failed_submissions = 0
    already_imported = 0
    submission_results = []

    try:
        with httpx.Client(timeout=BATCH_TIMEOUT, verify=False) as client:
            # Submit each record (IMEI) individually
            for idx, item in enumerate(batch_data, 1):
                try:
                    # Extract the record from the item
                    if "data" in item and "data" in item["data"]:
                        records = item["data"]["data"]
                    else:
                        continue

                    payload = {"apikey": API_KEY, "data": records}

                    print(
                        f"\n[SUBMIT] Request {idx}/{len(batch_data)}: Submitting {len(records)} record(s) with IMEI: {records[0]['detail'][0]['imei'] if records and records[0].get('detail') else 'N/A'}"
                    )

                    response = client.post(
                        IMPORT_URL, headers=headers, json=payload
                    )
                    response.raise_for_status()
                    response_data = response.json()

                    print(f"[SUBMIT] Request {idx} - SUCCESS")
                    successful_submissions += 1
                    submission_results.append(
                        {
                            "request": idx,
                            "status": "success",
                            "response": response_data,
                        }
                    )

                except httpx.HTTPError as e:
                    print(f"[SUBMIT] Request {idx} - FAILED: {e}")

                    # Try to get error details from response
                    try:
                        error_data = e.response.json()
                        error_code = error_data.get("errorCode", "")

                        # Check if error is "already imported" error (Chứng từ ... đã nhập)
                        if "đã nhập" in error_code:
                            print(
                                f"[SUBMIT] Request {idx} - ALREADY IMPORTED: {error_code}"
                            )
                            already_imported += 1
                            submission_results.append(
                                {
                                    "request": idx,
                                    "status": "already_imported",
                                    "error": error_code,
                                }
                            )
                        else:
                            failed_submissions += 1
                            submission_results.append(
                                {
                                    "request": idx,
                                    "status": "failed",
                                    "error": error_code,
                                }
                            )
                            print(
                                f"[SUBMIT] Request {idx} - Error: {error_code}"
                            )
                    except:
                        failed_submissions += 1
                        submission_results.append(
                            {
                                "request": idx,
                                "status": "failed",
                                "error": str(e),
                            }
                        )

        # Print summary
        print("\n" + "=" * 80)
        print("SUBMISSION SUMMARY")
        print("=" * 80)
        print(f"Total requests: {len(batch_data)}")
        print(f"Successful: {successful_submissions}")
        print(f"Already imported: {already_imported}")
        print(f"Failed: {failed_submissions}")
        print("=" * 80)

        return {
            "status": "completed",
            "total_requests": len(batch_data),
            "successful": successful_submissions,
            "already_imported": already_imported,
            "failed": failed_submissions,
            "details": submission_results,
        }

    except Exception as e:
        print(f"\n[ERROR] Unexpected error during batch submission: {e}")
        logger.error(f"Unexpected error during batch submission: {e}")
        raise


def process_test_flow():
    """Main test flow function"""
    print("\n" + "=" * 80)
    print("CRM DATA PROCESSING TEST FLOW - MANUAL EXECUTION")
    print("=" * 80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"IMEI List: {IMEI_LIST}")
    print("Email sending: DISABLED (test mode)")
    print("=" * 80)

    try:
        # Step 1: Get access token
        token = get_access_token()

        # Step 2: Fetch online and offline data with IMEI filter
        print("\n[STEP 2] Fetching data with IMEI filter...")
        online_data = fetch_data_with_imei(token, is_online=True)
        print(f"\nTotal online records fetched: {len(online_data)}")

        offline_data = fetch_data_with_imei(token, is_online=False)
        print(f"Total offline records fetched: {len(offline_data)}")

        # Combine data
        all_data = online_data + offline_data
        print(f"\nTotal combined records: {len(all_data)}")

        # Step 3: Apply filters
        print("\n[STEP 3] Applying filters...")
        filtered_online_data, invalid_online_phones, kl_online_phones = (
            apply_filters(online_data, is_online=True)
        )

        filtered_offline_data, invalid_offline_phones, kl_offline_phones = (
            apply_filters(offline_data, is_online=False)
        )

        # Combine filtered data
        all_filtered_data = filtered_online_data + filtered_offline_data
        print(f"\nTotal filtered records: {len(all_filtered_data)}")

        # Step 4: Transform data
        print("\n[STEP 4] Transforming data...")
        batch_data = transform_data(all_filtered_data)

        # Step 5: Submit batch
        print("\n[STEP 5] Submitting batch to CRM...")
        if batch_data:
            result = submit_batch(batch_data)
        else:
            result = {"status": "no_data", "message": "No data to submit"}
            print_response_details(result, "BATCH SUBMIT RESULT")

        # Final summary
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Records fetched (online): {len(online_data)}")
        print(f"Records fetched (offline): {len(offline_data)}")
        print(f"Records after filtering (online): {len(filtered_online_data)}")
        print(
            f"Records after filtering (offline): {len(filtered_offline_data)}"
        )
        print(f"Total records submitted: {len(batch_data)}")
        print(f"Submission status: {result['status']}")
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

        return result

    except Exception as e:
        print(f"\n[ERROR] An error occurred: {e}")
        logger.error(f"An error occurred in test flow: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    try:
        process_test_flow()
    except Exception as e:
        print(f"\nTest flow failed: {e}")
        sys.exit(1)
