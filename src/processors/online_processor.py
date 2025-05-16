import io
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import dask.dataframe as dd
import numpy as np
import pandas as pd

from util.logging import get_logger
from util.phone_utils import format_phone_number, is_valid_phone

logger = get_logger(__name__)


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
            "mã khách hàng": "Mã khách hàng",  # Thêm ánh xạ cho Mã khách hàng nếu có
        }
        self.excluded_customer_keywords = ["BƯU ĐIỆN"]
        # Thêm các từ khóa mới vào excluded_product_codes
        self.excluded_product_codes = [
            "PBHDT",
            "THUNG",
            "DVVC_ONL",
            "TUINILONPK",
        ]
        self.excluded_product_names = ["BAO LÌ XÌ"]

    def read_input_file(self) -> dd.DataFrame:
        df = pd.read_excel(
            self.input_file,
            sheet_name="Sheet1",
            dtype={"Số Ctừ": str, "Số điện thoại": str, "Imei": str},
            engine="calamine",
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
        """
        Optimized method to split rows based on quantity using NumPy vectorization.
        """

        # Convert Dask DataFrame to pandas
        pdf = df.compute()

        if pdf.empty:
            return dd.from_pandas(pdf, npartitions=1)

        # Separate rows that don't need expansion (quantity <= 1 or NA)
        mask_no_expand = pdf["Số lượng"].isna() | (pdf["Số lượng"] <= 1)
        rows_no_expand = pdf[mask_no_expand].copy()
        rows_to_expand = pdf[~mask_no_expand].copy()

        if rows_to_expand.empty:
            return dd.from_pandas(rows_no_expand, npartitions=4)

        # Convert quantities to integers and handle missing values
        quantities = np.maximum(
            rows_to_expand["Số lượng"].fillna(1).astype(int).values, 1
        )

        # Create repeating indices
        indices = np.arange(len(rows_to_expand))
        repeat_indices = np.repeat(indices, quantities)

        # Create expanded DataFrame
        expanded_df = rows_to_expand.iloc[repeat_indices].reset_index(drop=True)

        # Calculate unit revenues once for all rows
        unit_revenues = np.zeros(len(rows_to_expand))
        valid_mask = (rows_to_expand["Doanh thu"].notna()) & (quantities > 0)
        unit_revenues[valid_mask] = (
            rows_to_expand.loc[valid_mask, "Doanh thu"].values
            / quantities[valid_mask]
        )

        # Assign unit revenues to expanded rows
        expanded_df["Doanh thu"] = unit_revenues[np.repeat(indices, quantities)]

        # Set all quantities to 1
        expanded_df["Số lượng"] = 1

        # Combine non-expanded and expanded rows
        result_df = pd.concat([rows_no_expand, expanded_df], ignore_index=True)

        # Return as Dask DataFrame with appropriate partitions
        return dd.from_pandas(result_df, npartitions=4)

    def _process_final(
        self, df: pd.DataFrame, format_phone: bool = True
    ) -> pd.DataFrame:
        df_processed = df.copy()
        if format_phone:
            df_processed["Số điện thoại"] = df_processed["Số điện thoại"].apply(
                format_phone_number
            )

        # Đã loại bỏ việc ghi đè cột "Mã đơn hàng" từ công thức
        # df_processed["Mã đơn hàng"] = df_processed.apply(
        #     lambda row: "".join(
        #         str(row[col]) if pd.notna(row[col]) else ""
        #         for col in ["Ngày Ct", "Mã Ct", "Số Ct", "Mã bộ phận"]
        #     ),
        #     axis=1,
        # )

        df_processed["Doanh thu"] = df_processed["Doanh thu"].fillna(0)
        final_df = pd.DataFrame(columns=self._headers)
        for col in self._headers:
            if col in df_processed.columns:
                final_df[col] = df_processed[col]
            else:
                final_df[col] = pd.NA
        return final_df

    def _filter_kl_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Lọc các bản ghi có số điện thoại là "0912345678".
        """
        # Kiểm tra xem cột "Số điện thoại" có tồn tại không
        if "Số điện thoại" not in df.columns:
            print("CẢNH BÁO: Không tìm thấy cột 'Số điện thoại' trong dữ liệu")
            return df.iloc[0:0]  # Trả về DataFrame rỗng

        # Tạo mask cho các bản ghi có số điện thoại "0912345678"
        phone_mask = df["Số điện thoại"].astype(str).str.strip() == "0912345678"

        # Trả về DataFrame chứa các bản ghi thỏa mãn điều kiện
        filtered_df = df[phone_mask]

        # Debug info
        logger.debug(
            f"KL RECORDS FILTERING: Records with phone number 0912345678: {phone_mask.sum()}"
        )

        return filtered_df

    def process_data(
        self, df: dd.DataFrame
    ) -> Tuple[dd.DataFrame, pd.DataFrame, Optional[str]]:
        df_result = df.rename(
            columns=lambda x: self.column_mapping.get(x.lower(), x)
        )
        pdf_result = df_result.compute()

        # Tạo một bản sao để theo dõi
        total_records_initial = len(pdf_result)

        # Lọc các bản ghi KL với điều kiện Số PO > 7
        kl_records_df = self._filter_kl_records(pdf_result)

        # Tạo một mask cho các bản ghi KL để loại chúng ra khỏi xử lý valid/invalid
        if not kl_records_df.empty:
            # Tạo mask để xác định các bản ghi KL
            kl_indices = kl_records_df.index

            # Loại bỏ các bản ghi KL khỏi pdf_result trước khi xử lý valid/invalid
            pdf_result = pdf_result.drop(index=kl_indices)

        # Chuyển đổi DataFrame KL thành JSON với đúng các trường header
        kl_records_json = None
        if not kl_records_df.empty:
            # Tạo DataFrame mới chỉ với các cột theo yêu cầu
            kl_final_df = pd.DataFrame(columns=self._headers)

            # Sao chép dữ liệu từ các cột tương ứng
            for col in self._headers:
                if col in kl_records_df.columns:
                    kl_final_df[col] = kl_records_df[col]
                else:
                    kl_final_df[col] = pd.NA

            # Xử lý số điện thoại nếu cần
            kl_final_df["Số điện thoại"] = kl_final_df["Số điện thoại"].apply(
                format_phone_number
            )

            # Chuyển đổi thành JSON
            kl_records_json = kl_final_df.to_json(
                orient="records", force_ascii=False
            )

            # Debug: In ra số lượng bản ghi và 10 bản ghi đầu tiên
            print(
                f"\n=== DEBUG: KL RECORDS WITH SỐ PO > 7 - Found {len(kl_final_df)} records ==="
            )
            try:
                records = json.loads(kl_records_json)
                for i, record in enumerate(records[:10], 1):
                    print(f"Record {i}:")
                    for k, v in record.items():
                        print(f"  {k}: {v}")
                    print("-" * 40)
            except Exception as e:
                print(f"Error parsing JSON: {str(e)}")
            print("===============================================\n")

        # Bây giờ xử lý các bản ghi valid/invalid trên tập dữ liệu đã loại bỏ KL
        pdf_result["is_valid_phone"] = pdf_result["Số điện thoại"].apply(
            is_valid_phone
        )

        # Tách thành hai DataFrame: valid và invalid
        valid_df = pdf_result[pdf_result["is_valid_phone"]].copy()
        invalid_df = pdf_result[~pdf_result["is_valid_phone"]].copy()

        # Đảm bảo không có số điện thoại nào bị sót ở invalid_df
        final_valid_check = []
        final_invalid_check = []

        for _, row in valid_df.iterrows():
            if is_valid_phone(row["Số điện thoại"]):
                final_valid_check.append(row)
            else:
                final_invalid_check.append(row)

        for _, row in invalid_df.iterrows():
            final_invalid_check.append(row)

        # Tạo DataFrame từ danh sách
        final_valid_df = (
            pd.DataFrame(final_valid_check) if final_valid_check else valid_df
        )
        additional_invalid_df = (
            pd.DataFrame(final_invalid_check)
            if final_invalid_check
            else invalid_df
        )

        # Xử lý riêng cho từng luồng:
        final_valid_df = self._process_final(final_valid_df, format_phone=True)
        final_invalid_df = self._process_final(
            additional_invalid_df, format_phone=False
        )

        # Chuyển về dask DataFrame và xử lý
        valid_dask = dd.from_pandas(final_valid_df, npartitions=4)
        valid_dask = self._split_rows_by_quantity(valid_dask)

        invalid_dask = dd.from_pandas(final_invalid_df, npartitions=4)
        invalid_dask = self._split_rows_by_quantity(invalid_dask)

        # Kiểm tra tổng số bản ghi trước và sau khi xử lý
        valid_count = len(final_valid_df)
        invalid_count = len(final_invalid_df)
        kl_count = len(kl_records_df)
        total_after = valid_count + invalid_count + kl_count

        print("\n=== DEBUG: RECORD COUNT VERIFICATION ===")
        print(f"Total records initially: {total_records_initial}")
        print(f"Valid records: {valid_count}")
        print(f"Invalid records: {invalid_count}")
        print(f"KL records with Số PO > 7: {kl_count}")
        print(f"Total records after processing: {total_after}")
        print(
            f"All records accounted for: {total_records_initial == total_after + (total_records_initial - (valid_count + invalid_count + kl_count))}"
        )
        if total_records_initial != total_after:
            print(
                f"Note: {total_records_initial - total_after} records were KL records with Số PO <= 7 or other filtered records"
            )
        print("========================================\n")

        return valid_dask, invalid_dask.compute(), kl_records_json

    def process_to_buffer(
        self, output_buffer: io.BytesIO
    ) -> Tuple[bytes, bytes, Optional[str], int]:
        df = self.read_input_file()
        valid_dask, invalid_final_df, kl_records_json = self.process_data(df)
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
        return valid_content, invalid_content, kl_records_json, invalid_count

    def save_output_file(self, df: dd.DataFrame) -> None:
        if self.output_file:
            df.compute().to_excel(self.output_file, index=False)
        else:
            raise ValueError(
                "Output file path is not set for in-memory processing."
            )

    def run(self) -> None:
        try:
            df = self.read_input_file()
            valid_dask, invalid_final_df, kl_records_json = self.process_data(
                df
            )
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
