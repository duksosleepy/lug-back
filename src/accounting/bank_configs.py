#!/usr/bin/env python3
"""
Bank-specific configurations for different bank statement formats
"""

from dataclasses import dataclass

from src.accounting.bank_statement_reader import BankStatementConfig


@dataclass
class BankConfig:
    """Configuration for a specific bank"""

    name: str
    short_name: str
    code: str
    statement_config: BankStatementConfig


# VCB Configuration - based on _banks.json structure
VCB_CONFIG = BankConfig(
    name="NGÂN HÀNG THƯƠNG MẠI CỔ PHẦN NGOẠI THƯƠNG VIỆT NAM",
    short_name="VCB",
    code="VCB",
    statement_config=BankStatementConfig(
        bank_name="VCB",
        header_patterns={
            "reference": ["số tham chiếu", "so tham chieu", "reference", "ref"],
            "date": [
                "ngày giao dịch",
                "ngay giao dich",
                "ngày",
                "ngay",
                "date",
                "transaction date",
            ],
            "debit": [
                "số tiền ghi nợ",
                "so tien ghi no",
                "ghi nợ",
                "ghi no",
                "debit",
                "tiền ra",
                "tien ra",
            ],
            "credit": [
                "số tiền ghi có",
                "so tien ghi co",
                "ghi có",
                "ghi co",
                "credit",
                "tiền vào",
                "tien vao",
            ],
            "balance": ["số dư", "so du", "balance"],
            "description": [
                "mô tả",
                "mo ta",
                "diễn giải",
                "dien giai",
                "nội dung",
                "noi dung",
                "description",
            ],
        },
        data_start_patterns=[
            "ngày giao dịch",
            "ngay giao dich",
            "số tham chiếu",
            "so tham chieu",
        ],
        data_end_patterns=[
            "tổng số",
            "tong so",
            "tổng cộng",
            "tong cong",
            "total",
            "số dư cuối kỳ",
            "so du cuoi ky",
        ],
        required_columns=["date", "reference", "description"],
    ),
)

# BIDV Configuration (existing) - based on _banks.json structure
BIDV_CONFIG = BankConfig(
    name="NGÂN HÀNG THƯƠNG MẠI CỔ PHẦN ĐẦU TƯ VÀ PHÁT TRIỂN VIỆT NAM",
    short_name="BIDV",
    code="BIDV",
    statement_config=BankStatementConfig(
        bank_name="BIDV",
        header_patterns={
            "reference": [
                "số tham chiếu",
                "so tham chieu",
                "mã gd",
                "ma gd",
                "reference",
                "ref",
                "số ct",
                "so ct",
            ],
            "date": [
                "ngày hl",
                "ngay hl",
                "ngày",
                "ngay",
                "date",
                "ngày hiệu lực",
                "ngay hieu luc",
            ],
            "debit": [
                "ghi nợ",
                "ghi no",
                "tiền ra",
                "tien ra",
                "debit",
                "rút",
                "rut",
                "chi",
            ],
            "credit": [
                "ghi có",
                "ghi co",
                "tiền vào",
                "tien vao",
                "credit",
                "nạp",
                "nap",
                "thu",
            ],
            "balance": ["số dư", "so du", "balance", "tồn quỹ", "ton quy"],
            "description": [
                "mô tả",
                "mo ta",
                "diễn giải",
                "dien giai",
                "nội dung",
                "noi dung",
                "description",
                "detail",
            ],
        },
        data_start_patterns=[
            "STT",
            "stt",
            "Ngày HL",
            "ngày hl",
            "Số tham chiếu",
            "so tham chieu",
        ],
        data_end_patterns=[
            "tổng cộng",
            "tong cong",
            "total",
            "số dư cuối kỳ",
            "so du cuoi ky",
            "phát sinh trong kỳ",
            "phat sinh trong ky",
            "cộng phát sinh",
            "cong phat sinh",
        ],
        required_columns=["date", "description"],
    ),
)

# ACB Configuration - based on _banks.json structure
ACB_CONFIG = BankConfig(
    name="NGÂN HÀNG THƯƠNG MẠI CỔ PHẦN Á CHÂU",
    short_name="ACB",
    code="ACB",
    statement_config=BankStatementConfig(
        bank_name="ACB",
        header_patterns={
            "reference": ["số gd", "so gd"],  # Very specific to ACB
            "date": [
                "ngày giao dịch",
                "ngay giao dich",
            ],
            "debit": [
                "số tiền rút ra",
                "so tien rut ra",
            ],
            "credit": [
                "số tiền gửi vào",
                "so tien gui vao",
            ],
            "balance": ["số dư", "so du"],  # Only exact match, not partial
            "description": [
                "nội dung giao dịch",
                "noi dung giao dich",
            ],
        },
        data_start_patterns=[
            "ngày giao dịch",
            "ngay giao dich",
            "số gd",
            "so gd",
        ],
        data_end_patterns=[
            "tổng số",
            "tong so",
            "tổng cộng",
            "tong cong",
            "total",
            "số dư cuối kỳ",
            "so du cuoi ky",
        ],
        required_columns=["date", "reference", "description"],
    ),
)

# MBB Configuration - based on MBB statement format
MBB_CONFIG = BankConfig(
    name="NGÂN HÀNG THƯƠNG MẠI CỔ PHẦN QUÂN ĐỘI",
    short_name="MBB",
    code="MBB",
    statement_config=BankStatementConfig(
        bank_name="MBB",
        header_patterns={
            "reference": [
                "bút toán",
                "but toan",
                "BÚT TOÁN",
                "BUT TOAN",
            ],
            "date": [
                "ngày giao dịch",
                "ngay giao dich",
                "NGÀY GIAO DỊCH",
                "NGAY GIAO DICH",
            ],
            "debit": [
                "phát sinh nợ",
                "phat sinh no",
                "PHÁT SINH NỢ",
                "PHAT SINH NO",
            ],
            "credit": [
                "phát sinh có",
                "phat sinh co",
                "PHÁT SINH CÓ",
                "PHAT SINH CO",
            ],
            "balance": [
                "số dư",
                "so du",
                "SỐ DƯ",
                "SO DU",
            ],
            "description": [
                "nội dung",
                "noi dung",
                "NỘI DUNG",
                "NOI DUNG",
            ],
        },
        data_start_patterns=[
            "bút toán",
            "but toan",
            "BÚT TOÁN",
            "ngày giao dịch",
            "ngay giao dich",
            "NGÀY GIAO DỊCH",
        ],
        data_end_patterns=[
            "tổng cộng",
            "tong cong",
            "total",
            "số dư cuối kỳ",
            "so du cuoi ky",
            "phát sinh trong kỳ",
            "phat sinh trong ky",
        ],
        required_columns=["date", "reference", "description"],
    ),
)

# Bank configurations registry - using short_name as key to match _banks.json
BANK_CONFIGS = {
    "VCB": VCB_CONFIG,
    "BIDV": BIDV_CONFIG,
    "ACB": ACB_CONFIG,
    "MBB": MBB_CONFIG,
}


def get_bank_config(bank_short_name: str) -> BankConfig:
    """Get bank configuration by bank short_name (matches _banks.json structure)"""
    return BANK_CONFIGS.get(
        bank_short_name.upper(), BIDV_CONFIG
    )  # Default to BIDV
