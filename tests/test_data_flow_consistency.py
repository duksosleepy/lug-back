"""Test end-to-end data flow to ensure detail structure is consistent throughout the pipeline"""
import unittest
import json
import hashlib
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestDataFlowConsistency(unittest.TestCase):
    """Verify detail structure remains consistent from transform_data to restate to final server"""

    def test_detail_structure_in_transform_data(self):
        """Test that transform_data creates correct detail structure"""
        from crm.flow import transform_data

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
                "Ma_Don_Hang": "BL/LUG_VHM/20260330/0002",
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

        # Should be 1 combined request
        self.assertEqual(len(result), 1)

        batch_item = result[0]

        # Verify outer structure (what gets sent to Restate)
        self.assertIn("url", batch_item)
        self.assertIn("data", batch_item)
        self.assertIn("apikey", batch_item["data"])
        self.assertIn("data", batch_item["data"])
        self.assertEqual(len(batch_item["data"]["data"]), 1)

        order = batch_item["data"]["data"][0]

        # Verify order structure
        self.assertIn("master", order)
        self.assertIn("detail", order)
        self.assertIsInstance(order["detail"], list)
        self.assertEqual(len(order["detail"]), 2)

        # Verify each detail item has the correct structure
        for detail in order["detail"]:
            self.assertIn("maHang", detail)
            self.assertIn("tenHang", detail)
            self.assertIn("imei", detail)
            self.assertIn("soLuong", detail)
            self.assertIn("doanhThu", detail)

            # Verify field types
            self.assertIsInstance(detail["maHang"], str)
            self.assertIsInstance(detail["tenHang"], str)
            self.assertIsInstance(detail["imei"], str)
            self.assertIsInstance(detail["soLuong"], (int, float))
            self.assertIsInstance(detail["doanhThu"], (int, float))

        # Verify the exact data matches our input
        detail_0 = order["detail"][0]
        self.assertEqual(detail_0["maHang"], "MP030_28_PURPLE-SHINY")
        self.assertEqual(detail_0["tenHang"], "VALY NHUA 8 BANH HIEU HOLDALL")
        self.assertEqual(detail_0["imei"], "DDLHQL21752503000416")
        self.assertEqual(detail_0["soLuong"], 1)
        self.assertEqual(detail_0["doanhThu"], 2026000)

        detail_1 = order["detail"][1]
        self.assertEqual(detail_1["maHang"], "BL118_PURPLE")
        self.assertEqual(detail_1["tenHang"], "BALO SANTABARBARA")
        self.assertEqual(detail_1["imei"], "DBLSBP21392511000013")
        self.assertEqual(detail_1["soLuong"], 1)
        self.assertEqual(detail_1["doanhThu"], 0)

    def test_restate_receives_correct_structure(self):
        """Test that the structure Restate receives is correct"""
        from crm.flow import transform_data

        sales_data = [
            {
                "Ngay_Ct": "2026-03-30",
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
        ]

        result = transform_data(sales_data)

        # Simulate what gets sent to Restate BATCH_URL
        chunk = result  # This is what submit_batch sends

        # Verify Restate receives list of request objects
        self.assertIsInstance(chunk, list)
        self.assertEqual(len(chunk), 1)

        restate_request = chunk[0]

        # Verify structure matches HttpRequest model in restate server
        self.assertIn("url", restate_request)
        self.assertIn("data", restate_request)

        # Verify data structure matches what Restate expects
        data = restate_request["data"]
        self.assertIsInstance(data, dict)
        self.assertIn("apikey", data)
        self.assertIn("data", data)
        self.assertIsInstance(data["data"], list)
        self.assertEqual(len(data["data"]), 1)

        # Generate task_id the same way Restate does
        request_json = json.dumps(restate_request, sort_keys=True)
        task_id = hashlib.md5(request_json.encode()).hexdigest()

        # Verify task_id is generated correctly
        self.assertIsInstance(task_id, str)
        self.assertEqual(len(task_id), 32)

    def test_final_server_receives_correct_structure(self):
        """Test what the final CRM server receives (sent from Restate execute_request)"""
        from crm.flow import transform_data

        sales_data = [
            {
                "Ngay_Ct": "2026-03-30",
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
                "Ngay_Ct": "2026-03-30",
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
                "Ma_Hang_Old": "BL118_PURPLE",
                "Ten_Hang": "BALO SANTABARBARA",
                "Imei": "DBLSBP21392511000013",
                "So_Luong": 1,
                "Doanh_Thu": 0,
            },
        ]

        result = transform_data(sales_data)

        # Simulate Restate execute_request - it sends request.data to request.url
        restate_request = result[0]
        final_server_payload = restate_request["data"]

        # This is what the final CRM server receives via POST
        # Structure: {"apikey": "...", "data": [order_with_combined_details]}

        self.assertIn("apikey", final_server_payload)
        self.assertIn("data", final_server_payload)
        self.assertIsInstance(final_server_payload["data"], list)
        self.assertEqual(len(final_server_payload["data"]), 1)

        order = final_server_payload["data"][0]

        # Verify the order structure that final server expects
        self.assertIn("master", order)
        self.assertIn("detail", order)

        # CRITICAL: Verify detail array has BOTH items (combined)
        self.assertEqual(len(order["detail"]), 2)

        # Verify detail structure matches the expected format
        detail_keys = {"maHang", "tenHang", "imei", "soLuong", "doanhThu"}
        for detail in order["detail"]:
            self.assertEqual(set(detail.keys()), detail_keys)

        # Verify master structure
        master_keys = {
            "ngayCT",
            "maCT",
            "soCT",
            "maBoPhan",
            "maDonHang",
            "tenKhachHang",
            "soDienThoai",
            "tinhThanh",
            "quanHuyen",
            "phuongXa",
            "diaChi",
        }
        self.assertEqual(set(order["master"].keys()), master_keys)

        # Verify maDonHang is the same for both details
        self.assertEqual(order["master"]["maDonHang"], "BL/LUG_VHM/20260330/0002")

        # Verify both details are present
        detail_ma_hangs = {d["maHang"] for d in order["detail"]}
        self.assertEqual(
            detail_ma_hangs, {"MP030_28_PURPLE-SHINY", "BL118_PURPLE"}
        )

    def test_detail_structure_matches_original_format(self):
        """Test that new combined detail structure matches original single-item detail format"""
        from crm.flow import transform_data

        # Create data with 2 items that have same maDonHang
        sales_data = [
            {
                "Ngay_Ct": "2026-03-30",
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
        ]

        result = transform_data(sales_data)
        order = result[0]["data"]["data"][0]

        # Even with just 1 item, structure should be array
        self.assertIsInstance(order["detail"], list)
        self.assertEqual(len(order["detail"]), 1)

        # Detail item structure should be exactly the same as original
        detail = order["detail"][0]
        expected_keys = {"maHang", "tenHang", "imei", "soLuong", "doanhThu"}
        self.assertEqual(set(detail.keys()), expected_keys)

        # Verify values
        self.assertEqual(detail["maHang"], "MP030_28_PURPLE-SHINY")
        self.assertEqual(detail["tenHang"], "VALY NHUA 8 BANH HIEU HOLDALL")
        self.assertEqual(detail["imei"], "DDLHQL21752503000416")
        self.assertEqual(detail["soLuong"], 1)
        self.assertEqual(detail["doanhThu"], 2026000)


if __name__ == "__main__":
    unittest.main()
