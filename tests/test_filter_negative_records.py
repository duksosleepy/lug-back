"""Test filter_negative_records function with data simulating January 5, 2026 TLO records."""

from src.crm.flow import filter_negative_records


class TestFilterNegativeRecords:
    """Test cases for filter_negative_records function."""

    def test_9_tlo_negative_records_january_5_2026(self):
        """Test that 9 TLO records with negative values are correctly identified.

        This simulates the data from CRM_ONLINE_DATA_URL for January 5, 2026.
        """
        # Simulate 9 TLO records with negative Doanh_Thu or So_Luong
        test_data = [
            # Record 1: Negative Doanh_Thu, valid So_Luong
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0001",
                "Ma_BP": "BP001",
                "Ten_Khach_Hang": "Nguyen Van A",
                "So_Dien_Thoai": "0901234567",
                "Doanh_Thu": -500000,
                "So_Luong": 1,
            },
            # Record 2: Negative So_Luong, valid Doanh_Thu
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0002",
                "Ma_BP": "BP001",
                "Ten_Khach_Hang": "Tran Van B",
                "So_Dien_Thoai": "0902345678",
                "Doanh_Thu": 0,
                "So_Luong": -1,
            },
            # Record 3: Both negative
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0003",
                "Ma_BP": "BP002",
                "Ten_Khach_Hang": "Le Van C",
                "So_Dien_Thoai": "0903456789",
                "Doanh_Thu": -1000000,
                "So_Luong": -2,
            },
            # Record 4: Negative Doanh_Thu with empty So_Luong (edge case - the bug scenario)
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0004",
                "Ma_BP": "BP002",
                "Ten_Khach_Hang": "Pham Van D",
                "So_Dien_Thoai": "0904567890",
                "Doanh_Thu": -750000,
                "So_Luong": "",  # Empty string - edge case
            },
            # Record 5: Negative So_Luong with None Doanh_Thu (edge case)
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0005",
                "Ma_BP": "BP003",
                "Ten_Khach_Hang": "Hoang Van E",
                "So_Dien_Thoai": "0905678901",
                "Doanh_Thu": None,  # None value - edge case
                "So_Luong": -1,
            },
            # Record 6: Negative Doanh_Thu with invalid So_Luong string
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0006",
                "Ma_BP": "BP003",
                "Ten_Khach_Hang": "Vu Van F",
                "So_Dien_Thoai": "0906789012",
                "Doanh_Thu": -250000,
                "So_Luong": "invalid",  # Invalid string - edge case
            },
            # Record 7: Large negative Doanh_Thu
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0007",
                "Ma_BP": "BP004",
                "Ten_Khach_Hang": "Dao Van G",
                "So_Dien_Thoai": "0907890123",
                "Doanh_Thu": -5000000,
                "So_Luong": 1,
            },
            # Record 8: Negative Doanh_Thu as float
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0008",
                "Ma_BP": "BP004",
                "Ten_Khach_Hang": "Bui Van H",
                "So_Dien_Thoai": "0908901234",
                "Doanh_Thu": -123456.78,
                "So_Luong": 1,
            },
            # Record 9: Negative So_Luong as string (valid numeric string)
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0009",
                "Ma_BP": "BP005",
                "Ten_Khach_Hang": "Do Van I",
                "So_Dien_Thoai": "0909012345",
                "Doanh_Thu": 0,
                "So_Luong": "-3",  # String representation of negative number
            },
        ]

        # Also add some positive records that should NOT be included
        positive_records = [
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "BL",
                "So_Ct": "0010",
                "Ma_BP": "BP001",
                "Ten_Khach_Hang": "Positive Customer 1",
                "So_Dien_Thoai": "0910123456",
                "Doanh_Thu": 1000000,
                "So_Luong": 2,
            },
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "HG",
                "So_Ct": "0011",
                "Ma_BP": "BP002",
                "Ten_Khach_Hang": "Positive Customer 2",
                "So_Dien_Thoai": "0911234567",
                "Doanh_Thu": 500000,
                "So_Luong": 1,
            },
            # Zero values should also not be included
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "BL",
                "So_Ct": "0012",
                "Ma_BP": "BP003",
                "Ten_Khach_Hang": "Zero Customer",
                "So_Dien_Thoai": "0912345678",
                "Doanh_Thu": 0,
                "So_Luong": 0,
            },
        ]

        all_data = test_data + positive_records
        result = filter_negative_records(all_data)

        # Should find exactly 9 negative records
        assert len(result) == 9, (
            f"Expected 9 negative records, got {len(result)}"
        )

        # Verify all 9 TLO records are in the result
        result_so_cts = {r["So_Ct"] for r in result}
        expected_so_cts = {
            "0001",
            "0002",
            "0003",
            "0004",
            "0005",
            "0006",
            "0007",
            "0008",
            "0009",
        }
        assert result_so_cts == expected_so_cts, (
            f"Expected {expected_so_cts}, got {result_so_cts}"
        )

    def test_edge_case_empty_string_doanh_thu_negative_so_luong(self):
        """Test that negative So_Luong is detected even when Doanh_Thu is empty string."""
        data = [
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0001",
                "Doanh_Thu": "",  # Empty string
                "So_Luong": -1,  # Negative
            }
        ]
        result = filter_negative_records(data)
        assert len(result) == 1, (
            "Should detect negative So_Luong even with empty Doanh_Thu"
        )

    def test_edge_case_negative_doanh_thu_empty_string_so_luong(self):
        """Test that negative Doanh_Thu is detected even when So_Luong is empty string.

        This is the exact bug scenario that was fixed.
        """
        data = [
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0001",
                "Doanh_Thu": -500000,  # Negative
                "So_Luong": "",  # Empty string - would cause ValueError in old code
            }
        ]
        result = filter_negative_records(data)
        assert len(result) == 1, (
            "Should detect negative Doanh_Thu even with empty So_Luong"
        )

    def test_edge_case_negative_doanh_thu_invalid_so_luong(self):
        """Test that negative Doanh_Thu is detected even when So_Luong is invalid string."""
        data = [
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0001",
                "Doanh_Thu": -500000,  # Negative
                "So_Luong": "not_a_number",  # Invalid
            }
        ]
        result = filter_negative_records(data)
        assert len(result) == 1, (
            "Should detect negative Doanh_Thu even with invalid So_Luong"
        )

    def test_edge_case_both_fields_invalid(self):
        """Test that records with both invalid fields are not incorrectly flagged as negative."""
        data = [
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0001",
                "Doanh_Thu": "invalid",  # Invalid
                "So_Luong": "also_invalid",  # Invalid
            }
        ]
        result = filter_negative_records(data)
        assert len(result) == 0, (
            "Should not flag records with invalid (non-numeric) values as negative"
        )

    def test_missing_fields(self):
        """Test handling of records with missing Doanh_Thu or So_Luong fields."""
        data = [
            # Missing both fields
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0001",
            },
            # Missing So_Luong with negative Doanh_Thu
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0002",
                "Doanh_Thu": -100000,
            },
        ]
        result = filter_negative_records(data)
        assert len(result) == 1, (
            "Should detect negative Doanh_Thu even when So_Luong is missing"
        )
        assert result[0]["So_Ct"] == "0002"

    def test_none_values(self):
        """Test handling of None values for Doanh_Thu and So_Luong."""
        data = [
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0001",
                "Doanh_Thu": None,
                "So_Luong": None,
            },
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0002",
                "Doanh_Thu": None,
                "So_Luong": -1,
            },
        ]
        result = filter_negative_records(data)
        assert len(result) == 1, (
            "Should detect negative So_Luong even when Doanh_Thu is None"
        )
        assert result[0]["So_Ct"] == "0002"

    def test_string_numeric_negative_values(self):
        """Test that string representations of negative numbers are correctly handled."""
        data = [
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0001",
                "Doanh_Thu": "-500000",  # String negative
                "So_Luong": "1",
            },
            {
                "Ngay_Ct": "2026-01-05T00:00:00",
                "Ma_Ct": "TLO",
                "So_Ct": "0002",
                "Doanh_Thu": "1000",
                "So_Luong": "-2",  # String negative
            },
        ]
        result = filter_negative_records(data)
        assert len(result) == 2, (
            "Should detect string representations of negative numbers"
        )
