#!/usr/bin/env python3
"""
Test script to verify order grouping logic without running servers.
This simulates the data flow from split_quantity_records -> transform_data -> restate server
"""

import json
from typing import Dict, List


def split_quantity_records(data: List[Dict]) -> List[Dict]:
    """Split records where So_Luong > 1 into multiple records with So_Luong = 1
    (Copied from src/crm/flow.py)
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

    print(f"✓ split_quantity_records: {original_count} records → {len(split_data)} records")
    return split_data


def transform_data(sales_data: List[Dict]) -> List[Dict]:
    """Transform the sales data to the format required by the batch service
    Records with the same maDonHang are combined into a single request with multiple detail items
    (Copied from src/crm/flow.py)
    """
    print(f"✓ transform_data: Processing {len(sales_data)} input records")

    # Group records by maDonHang
    orders_by_ma_don_hang = {}

    for item in sales_data:
        ma_don_hang = item.get("Ma_Don_Hang")

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

        # Add detail item to this order's detail list
        orders_by_ma_don_hang[ma_don_hang]["detail"].append(detail_item)

    # Convert grouped orders to batch format
    transformed_data = []
    for ma_don_hang, order_data in orders_by_ma_don_hang.items():
        detail_count = len(order_data.get("detail", []))
        print(f"  → maDonHang={ma_don_hang} has {detail_count} detail item(s)")
        transformed_data.append(
            {
                "url": "http://restate-server:9080/BatchService/submit_batch",
                "data": {
                    "apikey": "test-api-key",
                    "data": [order_data]
                }
            }
        )

    print(f"✓ transform_data: Created {len(transformed_data)} batch requests from {len(sales_data)} input records")
    return transformed_data


def simulate_restate_server(batch_requests: List[Dict]):
    """Simulate what restate server receives and would forward to final CRM server"""
    print(f"\n✓ Restate server received {len(batch_requests)} batch request(s)")

    for idx, req_data in enumerate(batch_requests):
        try:
            detail_count = len(req_data.get("data", {}).get("data", [{}])[0].get("detail", []))
            ma_don_hang = req_data.get("data", {}).get("data", [{}])[0].get("master", {}).get("maDonHang", "UNKNOWN")

            print(f"  Request {idx + 1}: maDonHang={ma_don_hang} with {detail_count} detail item(s)")

            # Show what would be sent to final CRM server
            print(f"  → Would forward to CRM server with {detail_count} detail item(s)")

        except Exception as e:
            print(f"  ERROR processing request {idx + 1}: {e}")


def print_section(title: str):
    """Print a section separator"""
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print('=' * 80)


# Test Case 1: Two separate products in same order
def test_case_1():
    print_section("TEST CASE 1: Two Different Products, Same maDonHang")

    test_data = [
        {
            "Ma_Don_Hang": "BL/LUG_VHM/20260330/0002",
            "Ma_Ct": "BL",
            "So_Ct": "0002",
            "Ma_BP": "LUG_VHM",
            "Ngay_Ct": "2026-03-30",
            "Ten_Khach_Hang": "CHI THU",
            "So_Dien_Thoai": "0903059876",
            "Tinh_Thanh": "",
            "Quan_Huyen": "",
            "Phuong_Xa": "",
            "Dia_Chi": "Q10",
            "Ma_Hang_Old": "MP030_28_PURPLE-SHINY",
            "Ten_Hang": "VALY NHUA 8 BANH HIEU HOLDALL",
            "Imei": "DDLHQL21752503000416",
            "So_Luong": 1,
            "Doanh_Thu": 2026000
        },
        {
            "Ma_Don_Hang": "BL/LUG_VHM/20260330/0002",  # SAME maDonHang
            "Ma_Ct": "BL",
            "So_Ct": "0002",
            "Ma_BP": "LUG_VHM",
            "Ngay_Ct": "2026-03-30",
            "Ten_Khach_Hang": "CHI THU",
            "So_Dien_Thoai": "0903059876",
            "Tinh_Thanh": "",
            "Quan_Huyen": "",
            "Phuong_Xa": "",
            "Dia_Chi": "Q10",
            "Ma_Hang_Old": "BL118_PURPLE",
            "Ten_Hang": "BALO SANTABARBARA",
            "Imei": "DBLSBP21392511000013",
            "So_Luong": 1,
            "Doanh_Thu": 0
        }
    ]

    print("\nInput: 2 records with same maDonHang")
    for i, record in enumerate(test_data):
        print(f"  Record {i+1}: maDonHang={record['Ma_Don_Hang']}, "
              f"maHang={record['Ma_Hang_Old']}, soLuong={record['So_Luong']}")

    # Process through pipeline
    split_data = split_quantity_records(test_data)
    batch_requests = transform_data(split_data)
    simulate_restate_server(batch_requests)

    # Verify result
    print("\n" + "─" * 80)
    final_detail_count = len(batch_requests[0]["data"]["data"][0]["detail"])
    if final_detail_count == 2:
        print("✅ PASS: Both detail items preserved in final output")
    else:
        print(f"❌ FAIL: Expected 2 detail items, got {final_detail_count}")

    # Show final JSON
    print("\nFinal JSON that would be sent to CRM server:")
    print(json.dumps(batch_requests[0]["data"], indent=2, ensure_ascii=False))


# Test Case 2: One product with So_Luong > 1
def test_case_2():
    print_section("TEST CASE 2: Single Product with So_Luong = 2")

    test_data = [
        {
            "Ma_Don_Hang": "BL/LUG_VHM/20260330/0003",
            "Ma_Ct": "BL",
            "So_Ct": "0003",
            "Ma_BP": "LUG_VHM",
            "Ngay_Ct": "2026-03-30",
            "Ten_Khach_Hang": "NGUYEN VAN A",
            "So_Dien_Thoai": "0901234567",
            "Tinh_Thanh": "HCM",
            "Quan_Huyen": "Q1",
            "Phuong_Xa": "P1",
            "Dia_Chi": "123 ABC",
            "Ma_Hang_Old": "MP030_28_PURPLE-SHINY",
            "Ten_Hang": "VALY NHUA 8 BANH HIEU HOLDALL",
            "Imei": "DDLHQL21752503000416",
            "So_Luong": 2,  # Will be split into 2 records
            "Doanh_Thu": 4052000
        }
    ]

    print("\nInput: 1 record with So_Luong = 2")
    print(f"  Record 1: maDonHang={test_data[0]['Ma_Don_Hang']}, "
          f"maHang={test_data[0]['Ma_Hang_Old']}, soLuong={test_data[0]['So_Luong']}")

    # Process through pipeline
    split_data = split_quantity_records(test_data)

    print(f"\nAfter split_quantity_records: {len(split_data)} records")
    for i, record in enumerate(split_data):
        print(f"  Record {i+1}: maDonHang={record['Ma_Don_Hang']}, "
              f"maHang={record['Ma_Hang_Old']}, soLuong={record['So_Luong']}, "
              f"doanhThu={record['Doanh_Thu']}")

    batch_requests = transform_data(split_data)
    simulate_restate_server(batch_requests)

    # Verify result
    print("\n" + "─" * 80)
    final_detail_count = len(batch_requests[0]["data"]["data"][0]["detail"])
    if final_detail_count == 2:
        print("✅ PASS: Split records correctly combined into 2 detail items")
    else:
        print(f"❌ FAIL: Expected 2 detail items, got {final_detail_count}")

    # Show final JSON
    print("\nFinal JSON that would be sent to CRM server:")
    print(json.dumps(batch_requests[0]["data"], indent=2, ensure_ascii=False))


# Test Case 3: Mixed scenario
def test_case_3():
    print_section("TEST CASE 3: Mixed - Multiple Products, Some with So_Luong > 1")

    test_data = [
        {
            "Ma_Don_Hang": "BL/LUG_VHM/20260330/0004",
            "Ma_Ct": "BL",
            "So_Ct": "0004",
            "Ma_BP": "LUG_VHM",
            "Ngay_Ct": "2026-03-30",
            "Ten_Khach_Hang": "TRAN THI B",
            "So_Dien_Thoai": "0907654321",
            "Tinh_Thanh": "HN",
            "Quan_Huyen": "BD",
            "Phuong_Xa": "P2",
            "Dia_Chi": "456 XYZ",
            "Ma_Hang_Old": "PRODUCT_A",
            "Ten_Hang": "Product A",
            "Imei": "IMEI_A",
            "So_Luong": 2,  # Will become 2 detail items
            "Doanh_Thu": 1000000
        },
        {
            "Ma_Don_Hang": "BL/LUG_VHM/20260330/0004",  # SAME maDonHang
            "Ma_Ct": "BL",
            "So_Ct": "0004",
            "Ma_BP": "LUG_VHM",
            "Ngay_Ct": "2026-03-30",
            "Ten_Khach_Hang": "TRAN THI B",
            "So_Dien_Thoai": "0907654321",
            "Tinh_Thanh": "HN",
            "Quan_Huyen": "BD",
            "Phuong_Xa": "P2",
            "Dia_Chi": "456 XYZ",
            "Ma_Hang_Old": "PRODUCT_B",
            "Ten_Hang": "Product B",
            "Imei": "IMEI_B",
            "So_Luong": 1,  # Stays as 1 detail item
            "Doanh_Thu": 500000
        },
        {
            "Ma_Don_Hang": "BL/LUG_VHM/20260330/0004",  # SAME maDonHang
            "Ma_Ct": "BL",
            "So_Ct": "0004",
            "Ma_BP": "LUG_VHM",
            "Ngay_Ct": "2026-03-30",
            "Ten_Khach_Hang": "TRAN THI B",
            "So_Dien_Thoai": "0907654321",
            "Tinh_Thanh": "HN",
            "Quan_Huyen": "BD",
            "Phuong_Xa": "P2",
            "Dia_Chi": "456 XYZ",
            "Ma_Hang_Old": "PRODUCT_C",
            "Ten_Hang": "Product C",
            "Imei": "IMEI_C",
            "So_Luong": 3,  # Will become 3 detail items
            "Doanh_Thu": 1500000
        }
    ]

    print("\nInput: 3 records with same maDonHang, different So_Luong values")
    for i, record in enumerate(test_data):
        print(f"  Record {i+1}: maHang={record['Ma_Hang_Old']}, soLuong={record['So_Luong']}")

    # Process through pipeline
    split_data = split_quantity_records(test_data)

    print(f"\nAfter split_quantity_records: {len(split_data)} records")

    batch_requests = transform_data(split_data)
    simulate_restate_server(batch_requests)

    # Verify result
    print("\n" + "─" * 80)
    final_detail_count = len(batch_requests[0]["data"]["data"][0]["detail"])
    expected_count = 2 + 1 + 3  # = 6
    if final_detail_count == expected_count:
        print(f"✅ PASS: All {expected_count} detail items preserved (2+1+3)")
    else:
        print(f"❌ FAIL: Expected {expected_count} detail items, got {final_detail_count}")

    # Show final JSON (abbreviated)
    print("\nFinal detail array:")
    for i, detail in enumerate(batch_requests[0]["data"]["data"][0]["detail"]):
        print(f"  Detail {i+1}: maHang={detail['maHang']}, soLuong={detail['soLuong']}, doanhThu={detail['doanhThu']}")


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("  ORDER GROUPING TEST SUITE")
    print("  Testing: split_quantity_records → transform_data → restate server")
    print("=" * 80)

    test_case_1()
    test_case_2()
    test_case_3()

    print("\n" + "=" * 80)
    print("  TEST SUITE COMPLETED")
    print("=" * 80 + "\n")
