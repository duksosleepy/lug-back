import dask.dataframe as dd
import pandas as pd


class DaskExcelProcessor:
    def __init__(self, input_file, output_file):
        self._headers = [
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

    def read_input_file(self):
        # Đọc file với các cột cần giữ định dạng string
        df = pd.read_excel(
            self.input_file,
            sheet_name="Sheet1",
            dtype={"Số Ctừ": str, "Số điện thoại": str, "Imei": str},
        )

        # Lọc bỏ các hàng không có Tên vật tư
        df = df[df["Tên vật tư"].notna()]

        # Lọc bỏ các hàng có Mã Ctừ là HDO hoặc TLO
        df = df[~df["Mã Ctừ"].isin(["HDO", "TLO"])]

        # Thay thế giá trị null trong cột Tiền doanh thu bằng 0
        df["Tiền doanh thu"] = df["Tiền doanh thu"].fillna(0)

        # Lọc bỏ các hàng không có Mã Ctừ
        df = df[df["Mã Ctừ"].notna()]

        # Danh sách các từ khóa cần lọc trong tên vật tư
        excluded_keywords = [
            "dịch vụ",
            "online",
            "thương mại điện tử",
            "shopee",
            "lazada",
            "tiktok",
        ]

        # Tạo mask để lọc các hàng có chứa từ khóa cần loại bỏ
        mask = ~df["Tên vật tư"].str.lower().str.contains(
            "|".join(excluded_keywords), case=False, na=False
        )
        df = df[mask]

        # Lọc bỏ các hàng có Mã vật tư là DVVC_ONL
        df = df[df["Mã vật tư"] != "DVVC_ONL"]

        # Điền số 0 cho các giá trị trống trong cột Số lượng
        df["Số lượng"] = df["Số lượng"].fillna(0)

        return dd.from_pandas(df, npartitions=4)

    def split_rows_by_quantity(self, df):
        # Chuyển từ dask DataFrame sang pandas DataFrame để xử lý
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
                for _ in range(int(quantity)):
                    new_row = row.copy()
                    new_row["Số lượng"] = 1
                    new_row["Doanh thu"] = unit_revenue
                    new_rows.append(new_row)

        return dd.from_pandas(pd.DataFrame(new_rows), npartitions=4)

    def create_order_id(self, row):
        # Xử lý từng thành phần, thay thế giá trị null bằng chuỗi rỗng
        ngay_ct = str(row["Ngày Ct"]) if pd.notna(row["Ngày Ct"]) else ""
        ma_ct = str(row["Mã Ct"]) if pd.notna(row["Mã Ct"]) else ""
        so_ct = str(row["Số Ct"]) if pd.notna(row["Số Ct"]) else ""
        ma_bp = str(row["Mã bộ phận"]) if pd.notna(row["Mã bộ phận"]) else ""

        # Nối các thành phần lại với nhau
        return f"{ngay_ct}{ma_ct}{so_ct}{ma_bp}"

    def format_phone_number(self, phone):
        """Định dạng lại số điện thoại để đảm bảo giữ số 0 ở đầu"""
        if pd.isna(phone):
            return None
        # Chuyển về string và đảm bảo số 0 ở đầu
        phone_str = str(phone)
        if (
            phone_str.isdigit() and len(phone_str) == 9
        ):  # Nếu số điện thoại có 9 chữ số
            return f"0{phone_str}"
        return phone_str

    def process_data(self, df):
        mapping = {
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

        # Đổi tên các cột theo mapping
        df_result = df.rename(columns=mapping)

        # Chuyển sang pandas DataFrame để xử lý
        pdf_result = df_result.compute()

        # Định dạng lại số điện thoại
        pdf_result["Số điện thoại"] = pdf_result["Số điện thoại"].apply(
            self.format_phone_number
        )

        # Tạo mã đơn hàng
        pdf_result["Mã đơn hàng"] = pdf_result.apply(
            self.create_order_id, axis=1
        )

        # Đảm bảo Doanh thu không có giá trị null
        pdf_result["Doanh thu"] = pdf_result["Doanh thu"].fillna(0)

        # Tạo DataFrame mới chỉ với các cột cần thiết theo thứ tự trong _headers
        final_df = pd.DataFrame(columns=self._headers)

        # Copy dữ liệu từ các cột tương ứng
        for col in self._headers:
            if col in pdf_result.columns:
                final_df[col] = pdf_result[col]
            else:
                final_df[col] = None

        # Chuyển kết quả thành dask DataFrame
        result_dask = dd.from_pandas(final_df, npartitions=4)

        # Thực hiện tách hàng theo số lượng
        return self.split_rows_by_quantity(result_dask)

    def save_output_file(self, df):
        # Xuất file và giữ định dạng cột số
        df.compute().to_excel(self.output_file, index=False)

    def run(self):
        try:
            df = self.read_input_file()
            df_result = self.process_data(df)
            self.save_output_file(df_result)
            print(f"File {self.output_file} đã được tạo thành công!")
        except Exception as e:
            print(f"Có lỗi xảy ra: {str(e)}")
