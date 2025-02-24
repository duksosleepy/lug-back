import io
import re
from pathlib import Path
from typing import Dict, List, Tuple, Union

import dask.dataframe as dd
import pandas as pd


class DaskExcelProcessor:
    def __init__(self, input_file: Union[str, io.BytesIO]):
        self._headers: List[str] = [
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

        self.column_mapping: Dict[str, str] = {
            "mã ctừ": "Mã Ct",
            "số ctừ": "Số Ct",
            "tên khách hàng": "Tên khách hàng",
            "số điện thoại": "Số điện thoại",
            "địa chỉ": "Địa chỉ",
            "imei": "Imei",
            "số lượng": "Số lượng",
            "tiền doanh thu": "Doanh thu",
            "ghi chú": "Ghi chú",
            "mã bộ phận": "Mã bộ phận",
            "tên vật tư": "Tên hàng",
            "số po": "Mã đơn hàng",
            "mã vật tư": "Mã hàng",
        }
        self.excluded_customer_keywords = ["BƯU ĐIỆN"]
        self.excluded_product_codes = ["PBHDT"]
        self.excluded_product_names = ["BAO LÌ XÌ"]

    def read_input_file(self) -> dd.DataFrame:
        df = pd.read_excel(
            self.input_file,
            sheet_name="Sheet1",
            dtype={"Số Ctừ": str, "Số điện thoại": str, "Imei": str},
        )
        df = (
            df[df["Tên vật tư"].notna()]
            .pipe(self._filter_excluded_data)
            .assign(
                **{"Tiền doanh thu": lambda x: x["Tiền doanh thu"].fillna(0)}
            )
        )
        return dd.from_pandas(df, npartitions=4)

    def _filter_excluded_data(self, df: pd.DataFrame) -> pd.DataFrame:
        customer_mask = ~df["Tên khách hàng"].str.lower().str.contains(
            "|".join(self.excluded_customer_keywords), case=False, na=False
        )
        product_code_mask = ~df["Mã vật tư"].str.lower().str.contains(
            "|".join(self.excluded_product_codes), case=False, na=False
        )
        product_name_mask = ~df["Tên vật tư"].str.lower().str.contains(
            "|".join(self.excluded_product_names), case=False, na=False
        )
        vpp_mask = df["Loại vật tư"].fillna("").str.upper() != "VPP"
        return df[
            customer_mask & product_code_mask & product_name_mask & vpp_mask
        ]

    def _split_rows_by_quantity(self, df: dd.DataFrame) -> dd.DataFrame:
        pdf = df.compute()
        new_rows = []
        for idx, row in pdf.iterrows():
            quantity = row["Số lượng"]
            if pd.isna(quantity) or quantity <= 1:
                new_rows.append(pd.Series(row))
            else:
                unit_revenue = (
                    row["Doanh thu"] / quantity
                    if pd.notna(row["Doanh thu"])
                    else 0
                )
                for _ in range(int(quantity)):
                    new_row = row.copy()
                    new_row["Số lượng"] = 1
                    new_row["Doanh thu"] = unit_revenue
                    new_rows.append(new_row)
        return dd.from_pandas(pd.DataFrame(new_rows), npartitions=4)

    @staticmethod
    def _is_valid_phone(phone: str) -> bool:
        if pd.isna(phone):
            return False
        phone_str = str(phone).strip()
        phone_str = re.sub(r"[-()\s\.]", "", phone_str)
        patterns = [
            r"^(0|84|\+84)?(3[2-9]|5[2689]|7[06-9]|8[1-9]|9[0-9])[0-9]{7}$",
            r"^(0|84|\+84)?(2[0-9]{1})[0-9]{8}$",
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

    def _process_final(
        self, df: pd.DataFrame, format_phone: bool = True
    ) -> pd.DataFrame:
        df_processed = df.copy()
        if format_phone:
            df_processed["Số điện thoại"] = df_processed["Số điện thoại"].apply(
                self._format_phone_number
            )
        df_processed["Mã đơn hàng"] = df_processed.apply(
            lambda row: "".join(
                str(row[col]) if pd.notna(row[col]) else ""
                for col in ["Ngày Ct", "Mã Ct", "Số Ct", "Mã bộ phận"]
            ),
            axis=1,
        )
        df_processed["Doanh thu"] = df_processed["Doanh thu"].fillna(0)
        final_df = pd.DataFrame(columns=self._headers)
        for col in self._headers:
            if col in df_processed.columns:
                final_df[col] = df_processed[col]
            else:
                final_df[col] = pd.NA
        return final_df

    def process_data(
        self, df: dd.DataFrame
    ) -> Tuple[dd.DataFrame, pd.DataFrame]:
        df_result = df.rename(
            columns=lambda x: self.column_mapping.get(x.lower(), x)
        )
        pdf_result = df_result.compute()
        pdf_result["is_valid_phone"] = pdf_result["Số điện thoại"].apply(
            self._is_valid_phone
        )
        valid_df = pdf_result[pdf_result["is_valid_phone"]].copy()
        invalid_df = pdf_result[~pdf_result["is_valid_phone"]].copy()
        final_valid_df = self._process_final(valid_df, format_phone=True)
        final_invalid_df = self._process_final(invalid_df, format_phone=False)
        valid_dask = dd.from_pandas(final_valid_df, npartitions=4)
        valid_dask = self._split_rows_by_quantity(valid_dask)
        invalid_dask = dd.from_pandas(final_invalid_df, npartitions=4)
        invalid_dask = self._split_rows_by_quantity(invalid_dask)
        return valid_dask, invalid_dask.compute()

    def process_to_buffer(
        self, output_buffer: io.BytesIO
    ) -> Tuple[bytes, bytes, int]:
        df = self.read_input_file()
        valid_dask, invalid_final_df = self.process_data(df)
        valid_buffer = io.BytesIO()
        with pd.ExcelWriter(valid_buffer, engine="openpyxl") as writer:
            valid_dask.compute().to_excel(writer, index=False)
        valid_buffer.seek(0)
        valid_content = valid_buffer.getvalue()
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
