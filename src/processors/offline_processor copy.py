# offline_processor.py
import io
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Union

import dask.dataframe as dd
import pandas as pd


@dataclass
class ProcessorConfig:
    excluded_keywords: List[str] = None
    excluded_codes: List[str] = None
    required_columns: List[str] = None

    def __post_init__(self):
        self.excluded_keywords = (
            [
                "dịch vụ",
                "online",
                "thương mại điện tử",
                "shopee",
                "lazada",
                "tiktok",
            ]
            if self.excluded_keywords is None
            else self.excluded_keywords
        )

        self.excluded_codes = (
            ["HDO", "TLO", "HD"]
            if self.excluded_codes is None
            else self.excluded_codes
        )

        self.required_columns = (
            [
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
            if self.required_columns is None
            else self.required_columns
        )


class DaskExcelProcessor:
    def __init__(
        self,
        input_file: Union[str, io.BytesIO],
        config: Optional[ProcessorConfig] = None,
    ):
        self.config = config or ProcessorConfig()
        # Nếu input_file là đường dẫn, xử lý theo kiểu file hệ thống
        if isinstance(input_file, (str, Path)):
            self.input_file = Path(input_file)
            if not self.input_file.exists():
                raise FileNotFoundError(f"Input file {input_file} not found")
            self.output_file = (
                self.input_file.parent
                / f"{self.input_file.stem}_final{self.input_file.suffix}"
            )
        else:
            # Nếu là file-like object (ví dụ: BytesIO) thì gán trực tiếp
            self.input_file = input_file
            self.output_file = None

        self.column_mapping = {
            "Mã Ctừ": "Mã Ct",
            "Số Ctừ": "Số Ct",
            "Tên khách hàng": "Tên khách hàng",
            "Số điện thoại": "Số điện thoại",
            "Địa chỉ": "Địa chỉ",
            "Imei": "Imei",
            "Số lượng": "Số lượng",
            "Tiền doanh thu": "Doanh thu",
            "Ghi chú": "Ghi chú",
            "Mã bộ phận": "Mã bộ phận",
            "Tên vật tư": "Tên hàng",
            "Mã vật tư": "Mã hàng",
        }

    def read_input_file(self) -> dd.DataFrame:
        # pd.read_excel hỗ trợ đọc từ file path hoặc file-like object
        df = pd.read_excel(
            self.input_file,
            sheet_name="Sheet1",
            dtype={"Số Ctừ": str, "Số điện thoại": str, "Imei": str},
        )

        # Lọc dữ liệu dựa trên các điều kiện
        excluded_product_codes = ["THUNG", "DVVC_ONL", "PBHDT", "TUINILONPK"]
        mask = (
            df["Tên vật tư"].notna()
            & ~df["Mã Ctừ"].isin(self.config.excluded_codes)
            & df["Mã Ctừ"].notna()
            & ~df["Tên vật tư"]
            .str.lower()
            .str.contains(
                "|".join(self.config.excluded_keywords), case=False, na=False
            )
            & ~df["Mã vật tư"].str.contains(
                "|".join(excluded_product_codes), case=False, na=False
            )
            & (df["Loại vật tư"].fillna("").str.upper() != "VPP")
        )

        filtered_df = df[mask].copy()
        filtered_df["Tiền doanh thu"] = filtered_df["Tiền doanh thu"].fillna(0)
        filtered_df["Số lượng"] = filtered_df["Số lượng"].fillna(0)

        return dd.from_pandas(filtered_df, npartitions=4)

    @staticmethod
    def split_rows_by_quantity(df: dd.DataFrame) -> dd.DataFrame:
        pdf = df.compute()

        if pdf.empty:
            return dd.from_pandas(pdf, npartitions=1)

        all_rows = []

        # Giữ lại các hàng có số lượng <= 1
        rows_no_expand = pdf[pdf["Số lượng"] <= 1]
        if not rows_no_expand.empty:
            all_rows.append(rows_no_expand)

        # Tách các hàng có số lượng > 1 thành các bản sao đơn vị
        rows_to_expand = pdf[pdf["Số lượng"] > 1]
        if not rows_to_expand.empty:
            expanded_list = []
            for _, row in rows_to_expand.iterrows():
                quantity = int(row["Số lượng"])
                unit_revenue = (
                    row["Doanh thu"] / quantity if quantity > 0 else 0
                )
                expanded_rows = pd.DataFrame([row.to_dict()] * quantity)
                expanded_rows["Số lượng"] = 1
                expanded_rows["Doanh thu"] = unit_revenue
                expanded_list.append(expanded_rows)

            if expanded_list:
                expanded_df = pd.concat(expanded_list, ignore_index=True)
                all_rows.append(expanded_df)

        if all_rows:
            for i in range(len(all_rows)):
                all_rows[i] = all_rows[i].astype(pdf.dtypes)
            result_df = pd.concat(all_rows, ignore_index=True)
        else:
            result_df = pd.DataFrame(columns=pdf.columns).astype(pdf.dtypes)

        return dd.from_pandas(result_df, npartitions=4)

    @staticmethod
    def create_order_id(row: pd.Series) -> str:
        return "".join(
            str(row[col]) if pd.notna(row[col]) else ""
            for col in ["Ngày Ct", "Mã Ct", "Số Ct", "Mã bộ phận"]
        )

    @staticmethod
    def format_phone_number(phone: str) -> Optional[str]:
        if pd.isna(phone):
            return None
        phone_str = str(phone)
        return (
            f"0{phone_str}"
            if phone_str.isdigit() and len(phone_str) == 9
            else phone_str
        )

    def process_data(self, df: dd.DataFrame) -> dd.DataFrame:
        # Đổi tên các cột theo mapping
        df_result = df.rename(columns=self.column_mapping)
        pdf_result = df_result.compute()

        processed_df = pd.DataFrame(index=pdf_result.index)
        processed_df["Số điện thoại"] = pdf_result["Số điện thoại"].apply(
            self.format_phone_number
        )
        processed_df["Mã đơn hàng"] = pdf_result.apply(
            self.create_order_id, axis=1
        )
        processed_df["Doanh thu"] = pdf_result["Doanh thu"].fillna(0)

        for col in pdf_result.columns:
            if col not in ["Số điện thoại", "Mã đơn hàng", "Doanh thu"]:
                processed_df[col] = pdf_result[col]

        final_df = pd.DataFrame(columns=self.config.required_columns)
        for col in self.config.required_columns:
            if col in processed_df.columns:
                final_df[col] = processed_df[col]
            else:
                final_df[col] = pd.NA

        result_dask = dd.from_pandas(final_df, npartitions=4)
        return self.split_rows_by_quantity(result_dask)

    def save_output_file(self, df: dd.DataFrame) -> None:
        # Phương thức này chỉ dùng khi output_file đã được xác định (xử lý file trên đĩa)  # noqa: E501
        if self.output_file:
            df.compute().to_excel(self.output_file, index=False)
        else:
            raise ValueError(
                "Output file path is not set for in-memory processing."
            )

    def process_to_buffer(self, output_buffer: io.BytesIO) -> None:
        """
        Xử lý file Excel và ghi kết quả vào output_buffer.
        Phương thức này hỗ trợ xử lý in-memory.
        """
        df = self.read_input_file()
        df_result = self.process_data(df)
        with pd.ExcelWriter(output_buffer, engine="openpyxl") as writer:
            df_result.compute().to_excel(writer, index=False)
        output_buffer.seek(0)

    def run(self) -> None:
        # Phương thức chạy kiểu cũ (xử lý từ file và lưu ra file hệ thống)
        try:
            df = self.read_input_file()
            df_result = self.process_data(df)
            self.save_output_file(df_result)
            print(f"File {self.output_file} đã được tạo thành công!")
            return self.output_file
        except Exception as e:
            print(f"Có lỗi xảy ra: {str(e)}")
            raise
