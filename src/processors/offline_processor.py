import io
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple, Union

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
            ["HD"] if self.excluded_codes is None else self.excluded_codes
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
        if isinstance(input_file, (str, Path)):
            self.input_file = Path(input_file)
            if not self.input_file.exists():
                raise FileNotFoundError(f"Input file {input_file} not found")
            self.output_file = (
                self.input_file.parent
                / f"{self.input_file.stem}_final{self.input_file.suffix}"
            )
        else:
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
        df = pd.read_excel(
            self.input_file,
            sheet_name="Sheet1",
            dtype={"Số Ctừ": str, "Số điện thoại": str, "Imei": str},
        )

        # Chỉ giữ lại các mã chứng từ được chỉ định
        valid_document_codes = ["BL", "BLK", "HG", "TG"]
        mask = (
            df["Mã Ctừ"].isin(valid_document_codes)
            & ~(
                (df["Mã Ctừ"].isin(["TG", "HG"]))
                & (
                    (df["Mã bộ phận"] == "SANGTAM")
                    | df["Mã bộ phận"].str.contains(
                        r"^DUAN\d+$", regex=True, na=False
                    )
                )
            )
            & df["Tên vật tư"].notna()
            & ~df["Mã vật tư"].str.contains(
                "DV_GHEMASSAGE", case=False, na=False
            )
            & ~df["Loại vật tư"].isin(["NUOC", "TPCN", "KEM"])
            & ~(
                (
                    df["Mã nhóm vật tư"].isin(
                        ["2.5.1 TCMN COI", "1.2.5 DDDL BAO BI"]
                    )
                )
                & (
                    df["Tiền doanh thu"].isna() | (df["Tiền doanh thu"] == 0)
                )  # Sửa từ "Doanh thu" thành "Tiền doanh thu"
            )
            & ~df["Mã Ctừ"].str.contains("HD", na=False)
            & ~df["Tên vật tư"]
            .str.lower()
            .str.contains(
                "|".join(self.config.excluded_keywords), case=False, na=False
            )
        )
        filtered_df = df[mask].copy()
        filtered_df["Tiền doanh thu"] = filtered_df["Tiền doanh thu"].fillna(0)
        filtered_df["Số lượng"] = filtered_df["Số lượng"].fillna(0)
        return dd.from_pandas(filtered_df, npartitions=4)

    @staticmethod
    def _is_valid_phone(phone: str) -> bool:
        if pd.isna(phone):
            return False
        phone_str = str(phone).strip()
        phone_str = re.sub(r"[-()\s\.]", "", phone_str)
        patterns = [
            r"^(0|84|\+84)?(3[2-9]|5[2689]|7[06-9]|8[1-9]|9[0-9])[0-9]{7}$",  # Di động
            r"^(0|84|\+84)?(2[0-9]{1})[0-9]{8}$",  # Cố định
        ]
        return any(re.match(pattern, phone_str) for pattern in patterns)

    @staticmethod
    def _format_phone_number(phone: str) -> str:
        if pd.isna(phone):
            return None
        phone_str = str(phone).strip()
        phone_str = re.sub(r"[-()\s\.]", "", phone_str)
        if phone_str.startswith("+84"):
            phone_str = "0" + phone_str[3:]
        elif phone_str.startswith("84"):
            phone_str = "0" + phone_str[2:]
        return phone_str

    @staticmethod
    def split_rows_by_quantity(df: dd.DataFrame) -> dd.DataFrame:
        pdf = df.compute()
        if pdf.empty:
            return dd.from_pandas(pdf, npartitions=1)
        all_rows = []
        rows_no_expand = pdf[pdf["Số lượng"] <= 1]
        if not rows_no_expand.empty:
            all_rows.append(rows_no_expand)
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

    # Hàm xử lý chung, với tùy chọn format_phone:
    def _process_final(
        self, df: pd.DataFrame, format_phone: bool = True
    ) -> pd.DataFrame:
        df_processed = df.copy()
        if format_phone:
            df_processed["Số điện thoại"] = df_processed["Số điện thoại"].apply(
                self._format_phone_number
            )
        # Tạo mã đơn hàng và cập nhật các trường cần thiết
        df_processed["Mã đơn hàng"] = df_processed.apply(
            self.create_order_id, axis=1
        )
        df_processed["Doanh thu"] = df_processed["Doanh thu"].fillna(0)
        final_df = pd.DataFrame(columns=self.config.required_columns)
        for col in self.config.required_columns:
            if col in df_processed.columns:
                final_df[col] = df_processed[col]
            else:
                final_df[col] = pd.NA
        return final_df

    def process_data(
        self, df: dd.DataFrame
    ) -> Tuple[dd.DataFrame, pd.DataFrame]:
        # Đổi tên cột theo mapping
        df_result = df.rename(columns=self.column_mapping)
        pdf_result = df_result.compute()
        # Xác định các record có số điện thoại hợp lệ
        pdf_result["is_valid_phone"] = pdf_result["Số điện thoại"].apply(
            self._is_valid_phone
        )
        valid_df = pdf_result[pdf_result["is_valid_phone"]].copy()
        invalid_df = pdf_result[~pdf_result["is_valid_phone"]].copy()
        # Xử lý riêng cho từng luồng:
        final_valid_df = self._process_final(valid_df, format_phone=True)
        final_invalid_df = self._process_final(invalid_df, format_phone=False)
        valid_dask = dd.from_pandas(final_valid_df, npartitions=4)
        valid_dask = self.split_rows_by_quantity(valid_dask)
        invalid_dask = dd.from_pandas(final_invalid_df, npartitions=4)
        invalid_dask = self.split_rows_by_quantity(invalid_dask)
        return valid_dask, invalid_dask.compute()

    def save_output_file(self, df: dd.DataFrame) -> None:
        if self.output_file:
            df.compute().to_excel(self.output_file, index=False)
        else:
            raise ValueError(
                "Output file path is not set for in-memory processing."
            )

    def process_to_buffer(
        self, output_buffer: io.BytesIO
    ) -> Tuple[bytes, bytes, int]:
        df = self.read_input_file()
        valid_dask, invalid_final_df = self.process_data(df)
        # Ghi file valid (đã xử lý)
        valid_buffer = io.BytesIO()
        with pd.ExcelWriter(valid_buffer, engine="openpyxl") as writer:
            valid_dask.compute().to_excel(writer, index=False)
        valid_buffer.seek(0)
        valid_content = valid_buffer.getvalue()
        # Ghi file invalid (giữ số điện thoại gốc)
        invalid_content = None
        invalid_count = (
            len(invalid_final_df) if not invalid_final_df.empty else 0
        )
        if invalid_count > 0:
            invalid_buffer = io.BytesIO()
            with pd.ExcelWriter(invalid_buffer, engine="openpyxl") as writer:
                invalid_final_df.to_excel(writer, index=False)
            invalid_buffer.seek(0)
            invalid_content = invalid_buffer.getvalue()
        return valid_content, invalid_content, invalid_count

    def run(self) -> None:
        try:
            df = self.read_input_file()
            valid_dask, invalid_final_df = self.process_data(df)
            self.save_output_file(valid_dask)
            if not invalid_final_df.empty:
                invalid_file = (
                    self.output_file.parent
                    / f"{self.output_file.stem}_invalid{self.output_file.suffix}"
                )
                invalid_final_df.to_excel(invalid_file, index=False)
                print(f"File chứa các bản ghi không hợp lệ: {invalid_file}")
            print(f"File {self.output_file} đã được tạo thành công!")
            return self.output_file
        except Exception as e:
            print(f"Có lỗi xảy ra: {str(e)}")
            raise
