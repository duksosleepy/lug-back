from typing import Dict, List

import dask.dataframe as dd
import pandas as pd


class DaskExcelProcessor:
    def __init__(self, input_file: str, output_file: str):
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
        self.input_file = input_file
        self.output_file = output_file
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
        """Đọc và tiền xử lý dữ liệu từ file Excel"""
        df = pd.read_excel(
            self.input_file,
            sheet_name="Sheet1",
            dtype={"Số Ctừ": str, "Số điện thoại": str, "Imei": str},
        )

        # Xử lý dữ liệu ban đầu
        df = (
            df[df["Tên vật tư"].notna()]  # Lọc các hàng có tên vật tư
            .pipe(
                self._filter_excluded_data
            )  # Lọc dữ liệu theo điều kiện loại trừ
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

        # Lọc tên vật tư (bao lì xì)
        product_name_mask = ~df["Tên vật tư"].str.lower().str.contains(
            "|".join(self.excluded_product_names), case=False, na=False
        )

        return df[customer_mask & product_code_mask & product_name_mask]

    def _split_rows_by_quantity(self, df: dd.DataFrame) -> dd.DataFrame:
        """Tách các hàng dựa trên số lượng"""
        pdf = df.compute()
        new_rows = []

        for _, row in pdf.iterrows():
            quantity = row["Số lượng"]
            if pd.isna(quantity) or quantity <= 1:
                new_rows.append(row)
            else:
                unit_revenue = (
                    row["Doanh thu"] / quantity
                    if pd.notna(row["Doanh thu"])
                    else 0
                )
                new_rows.extend(
                    [
                        {
                            **row.to_dict(),
                            "Số lượng": 1,
                            "Doanh thu": unit_revenue,
                        }
                        for _ in range(int(quantity))
                    ]
                )

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
        # Đổi tên cột
        df_result = df.rename(
            columns=lambda x: self.column_mapping.get(x.lower(), x)
        )
        pdf_result = df_result.compute()

        # Xử lý số điện thoại và doanh thu
        pdf_result["Số điện thoại"] = pdf_result["Số điện thoại"].apply(
            self._format_phone_number
        )
        pdf_result["Doanh thu"] = pdf_result["Doanh thu"].fillna(0)

        # Tạo DataFrame mới với các cột cần thiết
        final_df = pd.DataFrame(
            {
                col: pdf_result.get(col, pd.Series([None] * len(pdf_result)))
                for col in self._headers
            }
        )

        result_dask = dd.from_pandas(final_df, npartitions=4)
        return self._split_rows_by_quantity(result_dask)

    def save_output_file(self, df: dd.DataFrame) -> None:
        """Lưu kết quả ra file Excel"""
        df.compute().to_excel(self.output_file, index=False)

    def run(self) -> None:
        """Chạy toàn bộ quy trình xử lý"""
        try:
            df = self.read_input_file()
            df_result = self.process_data(df)
            self.save_output_file(df_result)
            print(f"File {self.output_file} đã được tạo thành công!")
        except Exception as e:
            print(f"Có lỗi xảy ra: {str(e)}")
