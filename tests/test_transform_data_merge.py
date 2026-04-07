"""Test transform_data function to verify it correctly merges orders with same maDonHang"""
import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestTransformDataMerge(unittest.TestCase):
    """Test that transform_data correctly combines orders with same maDonHang"""

    def test_combine_same_ma_don_hang(self):
        """Test that multiple items with same maDonHang are combined into one request"""
        from crm.flow import transform_data
        
        # Create test data with two items having same maDonHang
        sales_data = [
            {
                "Ngay_Ct": "2026-03-30T10:00:00",
                "Ma_Ct": "BL",
                "So_Ct": "0002",
                "Ma_BP": "LUG_VHM",
                "Ma_Don_Hang": "BL/LUG_VHM/20260330/0002",
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
                "Doanh_Thu": 2026000,
            },
            {
                "Ngay_Ct": "2026-03-30T10:00:00",
                "Ma_Ct": "BL",
                "So_Ct": "0002",
                "Ma_BP": "LUG_VHM",
                "Ma_Don_Hang": "BL/LUG_VHM/20260330/0002",  # Same maDonHang
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
                "Doanh_Thu": 0,
            },
        ]
        
        result = transform_data(sales_data)
        
        # Should return only 1 request (combined)
        self.assertEqual(len(result), 1)
        
        # Check the structure
        request = result[0]
        self.assertIn("url", request)
        self.assertIn("data", request)
        
        # Check data structure
        data = request["data"]
        self.assertIn("apikey", data)
        self.assertIn("data", data)
        self.assertEqual(len(data["data"]), 1)  # One order
        
        order = data["data"][0]
        self.assertIn("master", order)
        self.assertIn("detail", order)
        
        # Check master data
        master = order["master"]
        self.assertEqual(master["maDonHang"], "BL/LUG_VHM/20260330/0002")
        self.assertEqual(master["tenKhachHang"], "CHI THU")
        
        # Check detail - should have 2 items
        self.assertEqual(len(order["detail"]), 2)
        
        # Verify both detail items are present
        detail_ma_hangs = [d["maHang"] for d in order["detail"]]
        self.assertIn("MP030_28_PURPLE-SHINY", detail_ma_hangs)
        self.assertIn("BL118_PURPLE", detail_ma_hangs)
        
        # Verify detail data
        detail_0 = order["detail"][0]
        self.assertEqual(detail_0["maHang"], "MP030_28_PURPLE-SHINY")
        self.assertEqual(detail_0["imei"], "DDLHQL21752503000416")
        self.assertEqual(detail_0["doanhThu"], 2026000)
        
        detail_1 = order["detail"][1]
        self.assertEqual(detail_1["maHang"], "BL118_PURPLE")
        self.assertEqual(detail_1["imei"], "DBLSBP21392511000013")
        self.assertEqual(detail_1["doanhThu"], 0)

    def test_different_ma_don_hang_not_combined(self):
        """Test that items with different maDonHang are NOT combined"""
        from crm.flow import transform_data
        
        sales_data = [
            {
                "Ngay_Ct": "2026-03-30T10:00:00",
                "Ma_Ct": "BL",
                "So_Ct": "0001",
                "Ma_BP": "LUG_VHM",
                "Ma_Don_Hang": "BL/LUG_VHM/20260330/0001",
                "Ten_Khach_Hang": "Customer A",
                "So_Dien_Thoai": "0901234567",
                "Tinh_Thanh": "",
                "Quan_Huyen": "",
                "Phuong_Xa": "",
                "Dia_Chi": "Q1",
                "Ma_Hang_Old": "ITEM001",
                "Ten_Hang": "Item 1",
                "Imei": "IMEI001",
                "So_Luong": 1,
                "Doanh_Thu": 1000000,
            },
            {
                "Ngay_Ct": "2026-03-30T10:00:00",
                "Ma_Ct": "BL",
                "So_Ct": "0002",
                "Ma_BP": "LUG_VHM",
                "Ma_Don_Hang": "BL/LUG_VHM/20260330/0002",  # Different maDonHang
                "Ten_Khach_Hang": "Customer B",
                "So_Dien_Thoai": "0909876543",
                "Tinh_Thanh": "",
                "Quan_Huyen": "",
                "Phuong_Xa": "",
                "Dia_Chi": "Q2",
                "Ma_Hang_Old": "ITEM002",
                "Ten_Hang": "Item 2",
                "Imei": "IMEI002",
                "So_Luong": 2,
                "Doanh_Thu": 2000000,
            },
        ]
        
        result = transform_data(sales_data)
        
        # Should return 2 separate requests
        self.assertEqual(len(result), 2)
        
        # Check each request has only 1 detail item
        for req in result:
            order = req["data"]["data"][0]
            self.assertEqual(len(order["detail"]), 1)

    def test_mixed_same_and_different_ma_don_hang(self):
        """Test a mix of same and different maDonHang"""
        from crm.flow import transform_data
        
        sales_data = [
            # Order 1 - Item 1
            {
                "Ngay_Ct": "2026-03-30",
                "Ma_Ct": "BL",
                "So_Ct": "0001",
                "Ma_BP": "LUG_VHM",
                "Ma_Don_Hang": "ORDER_001",
                "Ten_Khach_Hang": "Customer A",
                "So_Dien_Thoai": "0901111111",
                "Tinh_Thanh": "",
                "Quan_Huyen": "",
                "Phuong_Xa": "",
                "Dia_Chi": "Q1",
                "Ma_Hang_Old": "ITEM001",
                "Ten_Hang": "Item 1",
                "Imei": "IMEI001",
                "So_Luong": 1,
                "Doanh_Thu": 1000,
            },
            # Order 2 - Item 1
            {
                "Ngay_Ct": "2026-03-30",
                "Ma_Ct": "BL",
                "So_Ct": "0002",
                "Ma_BP": "LUG_VHM",
                "Ma_Don_Hang": "ORDER_002",
                "Ten_Khach_Hang": "Customer B",
                "So_Dien_Thoai": "0902222222",
                "Tinh_Thanh": "",
                "Quan_Huyen": "",
                "Phuong_Xa": "",
                "Dia_Chi": "Q2",
                "Ma_Hang_Old": "ITEM002",
                "Ten_Hang": "Item 2",
                "Imei": "IMEI002",
                "So_Luong": 1,
                "Doanh_Thu": 2000,
            },
            # Order 1 - Item 2 (same maDonHang as first)
            {
                "Ngay_Ct": "2026-03-30",
                "Ma_Ct": "BL",
                "So_Ct": "0001",
                "Ma_BP": "LUG_VHM",
                "Ma_Don_Hang": "ORDER_001",  # Same as first
                "Ten_Khach_Hang": "Customer A",
                "So_Dien_Thoai": "0901111111",
                "Tinh_Thanh": "",
                "Quan_Huyen": "",
                "Phuong_Xa": "",
                "Dia_Chi": "Q1",
                "Ma_Hang_Old": "ITEM003",
                "Ten_Hang": "Item 3",
                "Imei": "IMEI003",
                "So_Luong": 1,
                "Doanh_Thu": 3000,
            },
        ]
        
        result = transform_data(sales_data)
        
        # Should return 2 requests (ORDER_001 with 2 details, ORDER_002 with 1 detail)
        self.assertEqual(len(result), 2)
        
        # Find ORDER_001 and ORDER_002
        order_001 = None
        order_002 = None
        
        for req in result:
            ma_don_hang = req["data"]["data"][0]["master"]["maDonHang"]
            if ma_don_hang == "ORDER_001":
                order_001 = req["data"]["data"][0]
            elif ma_don_hang == "ORDER_002":
                order_002 = req["data"]["data"][0]
        
        # Verify ORDER_001 has 2 detail items
        self.assertIsNotNone(order_001)
        self.assertEqual(len(order_001["detail"]), 2)
        
        # Verify ORDER_002 has 1 detail item
        self.assertIsNotNone(order_002)
        self.assertEqual(len(order_002["detail"]), 1)


if __name__ == "__main__":
    unittest.main()
