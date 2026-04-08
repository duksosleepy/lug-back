#!/usr/bin/env python3
"""
Test with real data from BL/GO_DIAN/20260404/0001 order
to see why 4 records are not being grouped together
"""

from typing import Dict, List


def transform_data_debug(sales_data: List[Dict]) -> List[Dict]:
    """Debug version of transform_data with extensive logging"""
    print(f"\n{'='*80}")
    print(f"transform_data: Processing {len(sales_data)} input records")
    print(f"{'='*80}\n")

    # Group records by maDonHang
    orders_by_ma_don_hang = {}

    for idx, item in enumerate(sales_data):
        ma_don_hang = item.get("Ma_Don_Hang")

        # Check for None or empty Ma_Don_Hang
        if not ma_don_hang:
            print(f"❌ Record {idx}: Ma_Don_Hang is None or empty! Skipping.")
            continue

        # DEBUG: Log each record being processed
        ma_hang = item.get("Ma_Hang_Old")
        print(f"Record {idx}:")
        print(f"  Ma_Don_Hang: '{ma_don_hang}' (type={type(ma_don_hang)}, repr={repr(ma_don_hang)})")
        print(f"  Ma_Hang_Old: '{ma_hang}'")
        print(f"  Existing keys in dict: {list(orders_by_ma_don_hang.keys())}")

        # Check if key exists
        key_exists = ma_don_hang in orders_by_ma_don_hang
        print(f"  Key '{ma_don_hang}' exists in dict: {key_exists}")

        # Extract date part only from datetime string (YYYY-MM-DD)
        date_str = item.get("Ngay_Ct", "")
        if date_str and len(date_str) >= 10:
            date_str = date_str[:10]

        # Create detail item for this record
        detail_item = {
            "maHang": item.get("Ma_Hang_Old"),
            "tenHang": item.get("Ten_Hang"),
            "imei": item.get("Imei"),
            "soLuong": item.get("So_Luong"),
            "doanhThu": item.get("Doanh_Thu") or 0,
        }

        # If this maDonHang hasn't been seen, create master record
        if ma_don_hang not in orders_by_ma_don_hang:
            print(f"  ✓ Creating NEW order group for '{ma_don_hang}'")
            orders_by_ma_don_hang[ma_don_hang] = {
                "master": {
                    "ngayCT": date_str,
                    "maCT": item.get("Ma_Ct"),
                    "soCT": item.get("So_Ct"),
                    "maBoPhan": item.get("Ma_BP"),
                    "maDonHang": ma_don_hang,
                    "tenKhachHang": item.get("Ten_Khach_Hang"),
                    "soDienThoai": item.get("So_Dien_Thoai"),
                    "tinhThanh": item.get("Tinh_Thanh"),
                    "quanHuyen": item.get("Quan_Huyen"),
                    "phuongXa": item.get("Phuong_Xa"),
                    "diaChi": item.get("Dia_Chi"),
                },
                "detail": []
            }
        else:
            print(f"  ✓ APPENDING to existing order group '{ma_don_hang}'")

        # Add detail item to this order's detail list
        orders_by_ma_don_hang[ma_don_hang]["detail"].append(detail_item)
        detail_count = len(orders_by_ma_don_hang[ma_don_hang]["detail"])
        print(f"  ✓ Total detail items for '{ma_don_hang}': {detail_count}\n")

    # Convert grouped orders to batch format
    print(f"\n{'='*80}")
    print("FINAL GROUPING RESULTS:")
    print(f"{'='*80}\n")

    transformed_data = []
    for ma_don_hang, order_data in orders_by_ma_don_hang.items():
        detail_count = len(order_data.get("detail", []))
        print(f"maDonHang: '{ma_don_hang}' → {detail_count} detail items")
        for i, detail in enumerate(order_data["detail"]):
            print(f"  Detail {i+1}: maHang={detail['maHang']}, soLuong={detail['soLuong']}")

        transformed_data.append({
            "url": "http://restate-server:9080/BatchService/submit_batch",
            "data": {
                "apikey": "test-api-key",
                "data": [order_data]
            }
        })

    print(f"\nTotal batch requests created: {len(transformed_data)}")
    print(f"{'='*80}\n")

    return transformed_data


# Test with the actual BL/GO_DIAN/20260404/0001 order
test_data = [
    {
        "Ma_Don_Hang": "BL/GO_DIAN/20260404/0001",
        "Ma_Ct": "BL",
        "So_Ct": "0001",
        "Ma_BP": "GO_DIAN",
        "Ngay_Ct": "2026-04-04",
        "Ten_Khach_Hang": "KHACH NUOC NGOAI",
        "So_Dien_Thoai": "0900000000",
        "Tinh_Thanh": "",
        "Quan_Huyen": "",
        "Phuong_Xa": "",
        "Dia_Chi": "NT",
        "Ma_Hang_Old": "TB67310",
        "Ten_Hang": "DÂY ĐAI DU LỊCH",
        "Imei": "",
        "So_Luong": 1,
        "Doanh_Thu": 299000
    },
    {
        "Ma_Don_Hang": "BL/GO_DIAN/20260404/0001",  # SAME!
        "Ma_Ct": "BL",
        "So_Ct": "0001",
        "Ma_BP": "GO_DIAN",
        "Ngay_Ct": "2026-04-04",
        "Ten_Khach_Hang": "KHACH NUOC NGOAI",
        "So_Dien_Thoai": "0900000000",
        "Tinh_Thanh": "",
        "Quan_Huyen": "",
        "Phuong_Xa": "",
        "Dia_Chi": "NT",
        "Ma_Hang_Old": "LUGGAGECOVE_M",
        "Ten_Hang": "BAO TRÙM VẢI",
        "Imei": "",
        "So_Luong": 1,
        "Doanh_Thu": 490000
    },
    {
        "Ma_Don_Hang": "BL/GO_DIAN/20260404/0001",  # SAME!
        "Ma_Ct": "BL",
        "So_Ct": "0001",
        "Ma_BP": "GO_DIAN",
        "Ngay_Ct": "2026-04-04",
        "Ten_Khach_Hang": "KHACH NUOC NGOAI",
        "So_Dien_Thoai": "0900000000",
        "Tinh_Thanh": "",
        "Quan_Huyen": "",
        "Phuong_Xa": "",
        "Dia_Chi": "NT",
        "Ma_Hang_Old": "MP030_24",
        "Ten_Hang": "VALY NHUA 8 BANH",
        "Imei": "",
        "So_Luong": 1,
        "Doanh_Thu": 1450000
    },
    {
        "Ma_Don_Hang": "BL/GO_DIAN/20260404/0001",  # SAME!
        "Ma_Ct": "BL",
        "So_Ct": "0001",
        "Ma_BP": "GO_DIAN",
        "Ngay_Ct": "2026-04-04",
        "Ten_Khach_Hang": "KHACH NUOC NGOAI",
        "So_Dien_Thoai": "0900000000",
        "Tinh_Thanh": "",
        "Quan_Huyen": "",
        "Phuong_Xa": "",
        "Dia_Chi": "NT",
        "Ma_Hang_Old": "MP030_28_HỒNG",
        "Ten_Hang": "VALY NHUA 8 BANH",
        "Imei": "",
        "So_Luong": 1,
        "Doanh_Thu": 1750000
    }
]

if __name__ == "__main__":
    print("\n" + "="*80)
    print("  TEST: Real Data from BL/GO_DIAN/20260404/0001 Order")
    print("  Expected: 1 batch request with 4 detail items")
    print("  Your issue: Getting 4 separate batch requests with 1 detail item each")
    print("="*80)

    result = transform_data_debug(test_data)

    print("\n" + "="*80)
    print("VERDICT:")
    print("="*80)
    if len(result) == 1 and len(result[0]["data"]["data"][0]["detail"]) == 4:
        print("✅ PASS: Correctly grouped into 1 request with 4 detail items")
        print("\nThis means the transform_data logic is CORRECT.")
        print("The issue must be happening BEFORE transform_data is called.")
        print("\nPossible causes:")
        print("1. The 4 records have DIFFERENT Ma_Don_Hang values (whitespace, encoding)")
        print("2. Data is being modified between fetch and transform_data")
        print("3. Records are being filtered out somewhere in the pipeline")
    elif len(result) == 4:
        print(f"❌ FAIL: Created {len(result)} separate requests instead of 1")
        print("\nThis means there IS a bug in transform_data logic!")
    else:
        print(f"⚠️  UNEXPECTED: Created {len(result)} requests with unexpected detail counts")
