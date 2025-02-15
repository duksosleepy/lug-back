import io
from pathlib import Path
from typing import Dict, List, Union

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
        # Nếu input_file là đường dẫn thì sử dụng file hệ thống, ngược lại xử lý in-memory
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
        self.excluded_customer_keywords = ["BƯU ĐIỆN", "Khách lẻ"]
        self.excluded_product_codes = ["THUNG", "DVVC_ONL", "PBHDT"]
        self.excluded_product_names = ["BAO LÌ XÌ"]

    def read_input_file(self) -> dd.DataFrame:
        """Đọc và tiền xử lý dữ liệu từ file Excel (hỗ trợ file path hoặc file-like object)"""
        df = pd.read_excel(
            self.input_file,
            sheet_name="Sheet1",
            dtype={"Số Ctừ": str, "Số điện thoại": str, "Imei": str},
        )

        # Lọc các hàng có tên vật tư và thực hiện các bước loại trừ
        df = (
            df[df["Tên vật tư"].notna()]
            .pipe(self._filter_excluded_data)
            .assign(
                **{"Tiền doanh thu": lambda x: x["Tiền doanh thu"].fillna(0)}
            )
        )

        return dd.from_pandas(df, npartitions=4)

    def _filter_excluded_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Lọc dữ liệu theo các điều kiện loại trừ"""
        # Lọc mã chứng từ
        df = df[~df["Mã Ctừ"].isin(["TLO"])]

        # Lọc tên khách hàng
        customer_mask = ~df["Tên khách hàng"].str.lower().str.contains(
            "|".join(self.excluded_customer_keywords), case=False, na=False
        )

        # Lọc mã vật tư
        product_code_mask = ~df["Mã vật tư"].str.lower().str.contains(
            "|".join(self.excluded_product_codes), case=False, na=False
        )

        # Lọc tên vật tư (ví dụ: "BAO LÌ XÌ")
        product_name_mask = ~df["Tên vật tư"].str.lower().str.contains(
            "|".join(self.excluded_product_names), case=False, na=False
        )

        return df[customer_mask & product_code_mask & product_name_mask]

    def _split_rows_by_quantity(self, df: dd.DataFrame) -> dd.DataFrame:
        """Tách các hàng dựa trên số lượng"""
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
    def _format_phone_number(phone: str) -> str:
        """Định dạng số điện thoại"""
        if pd.isna(phone):
            return None
        phone_str = str(phone).strip()
        return (
            f"0{phone_str}"
            if phone_str.isdigit() and len(phone_str) == 9
            else phone_str
        )

    def process_data(self, df: dd.DataFrame) -> dd.DataFrame:
        """Xử lý và chuyển đổi dữ liệu"""
        # Đổi tên cột theo mapping không phân biệt chữ hoa/chữ thường
        df_result = df.rename(
            columns=lambda x: self.column_mapping.get(x.lower(), x)
        )
        pdf_result = df_result.compute()

        # Xử lý số điện thoại và đảm bảo doanh thu không null
        pdf_result["Số điện thoại"] = pdf_result["Số điện thoại"].apply(
            self._format_phone_number
        )
        pdf_result["Doanh thu"] = pdf_result["Doanh thu"].fillna(0)

        # Tạo DataFrame mới với các cột đầu ra theo thứ tự yêu cầu
        final_df = pd.DataFrame(columns=self._headers)
        for col in self._headers:
            if col in pdf_result.columns:
                final_df[col] = pdf_result[col]
            else:
                final_df[col] = pd.NA

        result_dask = dd.from_pandas(final_df, npartitions=4)
        return self._split_rows_by_quantity(result_dask)

    def save_output_file(self, df: dd.DataFrame) -> None:
        """Lưu kết quả ra file Excel (chỉ áp dụng khi xử lý theo file hệ thống)"""
        if self.output_file:
            df.compute().to_excel(self.output_file, index=False)
        else:
            raise ValueError(
                "Output file path is not set for in-memory processing."
            )

    def process_to_buffer(self, output_buffer: io.BytesIO) -> None:
        """
        Xử lý file Excel và ghi kết quả vào output_buffer.
        Phương thức này hỗ trợ xử lý in-memory, không ghi file ra đĩa.
        """
        df = self.read_input_file()
        df_result = self.process_data(df)
        with pd.ExcelWriter(output_buffer, engine="openpyxl") as writer:
            df_result.compute().to_excel(writer, index=False)
        output_buffer.seek(0)

    def run(self) -> Path:
        """
        Chạy toàn bộ quy trình xử lý theo kiểu file hệ thống và trả về đường dẫn file output.
        Dùng trong trường hợp đầu vào là file path.
        """
        try:
            df = self.read_input_file()
            df_result = self.process_data(df)
            self.save_output_file(df_result)
            print(f"File {self.output_file} đã được tạo thành công!")
            return self.output_file
        except Exception as e:
            print(f"Có lỗi xảy ra: {str(e)}")
            raise
