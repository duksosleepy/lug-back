import io
import logging
import re
from pathlib import Path
from typing import Dict, List, Tuple, Union

import pandas as pd

logger = logging.getLogger(__name__)


class ProductMappingProcessor:
    """
    Processor thực hiện ánh xạ mã hàng và tên hàng dựa trên file mapping.

    Processor này nhận hai file:
    - File dữ liệu chứa các bản ghi cần xử lý
    - File mapping (mmmc.xlsx) chứa bảng ánh xạ giữa mã hàng/tên hàng cũ và mới
    """

    def __init__(
        self,
        data_file: Union[str, io.BytesIO],
        mapping_file: Union[str, io.BytesIO],
    ):
        """
        Khởi tạo processor với file dữ liệu và file mapping.

        Args:
            data_file: File dữ liệu gốc cần xử lý (định dạng Excel)
            mapping_file: File mapping chứa mã hàng/tên hàng cũ và mới (định dạng Excel)
        """
        # Xử lý file dữ liệu
        if isinstance(data_file, (str, Path)):
            self.data_file = Path(data_file)
            if not self.data_file.exists():
                raise FileNotFoundError(
                    f"Không tìm thấy file dữ liệu {data_file}"
                )
        else:
            self.data_file = data_file

        # Xử lý file mapping
        if isinstance(mapping_file, (str, Path)):
            self.mapping_file = Path(mapping_file)
            if not self.mapping_file.exists():
                raise FileNotFoundError(
                    f"Không tìm thấy file mapping {mapping_file}"
                )
        else:
            self.mapping_file = mapping_file

        # Các mẫu có thể của tên cột
        self.old_code_patterns = [
            r"mã\s*gốc",
            r"ma\s*goc",
            r"old.*code",
            r"code.*old",
            r"mã\s*cũ",
            r"ma\s*cu",
        ]
        self.old_name_patterns = [
            r"tên\s*gốc",
            r"ten\s*goc",
            r"old.*name",
            r"name.*old",
            r"tên\s*cũ",
            r"ten\s*cu",
        ]
        self.new_code_patterns = [
            r"mã\s*mới",
            r"ma\s*moi",
            r"new.*code",
            r"code.*new",
            r"mã\s*thay",
            r"ma\s*thay",
        ]
        self.new_name_patterns = [
            r"tên\s*mới",
            r"ten\s*moi",
            r"new.*name",
            r"name.*new",
            r"tên\s*thay",
            r"ten\s*thay",
        ]

    def _normalize_column_name(self, name: str) -> str:
        """
        Chuẩn hóa tên cột để dễ so sánh.

        Args:
            name: Tên cột cần chuẩn hóa

        Returns:
            Tên cột đã chuẩn hóa (chữ thường, không dấu, không khoảng trắng)
        """
        # Chuyển sang chữ thường
        normalized = name.lower()
        # Loại bỏ dấu tiếng Việt (đơn giản)
        normalized = (
            normalized.replace("đ", "d")
            .replace("ê", "e")
            .replace("ô", "o")
            .replace("ư", "u")
        )
        normalized = re.sub(r"[áàảãạâấầẩẫậăắằẳẵặ]", "a", normalized)
        normalized = re.sub(r"[éèẻẽẹêếềểễệ]", "e", normalized)
        normalized = re.sub(r"[íìỉĩị]", "i", normalized)
        normalized = re.sub(r"[óòỏõọôốồổỗộơớờởỡợ]", "o", normalized)
        normalized = re.sub(r"[úùủũụưứừửữự]", "u", normalized)
        normalized = re.sub(r"[ýỳỷỹỵ]", "y", normalized)
        # Thay thế các ký tự không phải chữ cái/số bằng khoảng trắng
        normalized = re.sub(r"[^a-z0-9]", "", normalized)
        return normalized

    def _find_column_by_pattern(
        self, df: pd.DataFrame, patterns: List[str]
    ) -> str:
        """
        Tìm tên cột phù hợp với các mẫu.

        Args:
            df: DataFrame chứa các cột
            patterns: Danh sách các mẫu regex để tìm kiếm

        Returns:
            Tên cột thực tế nếu tìm thấy, None nếu không tìm thấy
        """
        # Chuẩn hóa tất cả tên cột để dễ so sánh
        normalized_columns = {
            self._normalize_column_name(col): col for col in df.columns
        }

        # Tìm kiếm theo mẫu
        for pattern in patterns:
            pattern = pattern.lower()
            for norm_col, original_col in normalized_columns.items():
                if re.search(pattern, norm_col):
                    return original_col

        # Nếu không tìm thấy theo mẫu, kiểm tra nếu có trong cột
        for pattern in patterns:
            for col in df.columns:
                if pattern.lower() in col.lower():
                    return col

        return None

    def _identify_mapping_columns(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Xác định các cột mapping từ DataFrame.

        Args:
            df: DataFrame chứa dữ liệu mapping

        Returns:
            Dict ánh xạ từ tên cột chuẩn sang tên cột thực tế
        """
        # In ra tất cả tên cột để debug
        logger.info(f"Các cột trong file mapping: {list(df.columns)}")

        # Tìm các cột tương ứng
        old_code_col = self._find_column_by_pattern(df, self.old_code_patterns)
        old_name_col = self._find_column_by_pattern(df, self.old_name_patterns)
        new_code_col = self._find_column_by_pattern(df, self.new_code_patterns)
        new_name_col = self._find_column_by_pattern(df, self.new_name_patterns)

        # Kiểm tra xem đã tìm thấy các cột cần thiết chưa
        missing_cols = []
        if not old_code_col:
            missing_cols.append("Mã gốc")
        if not old_name_col:
            missing_cols.append("Tên gốc")
        if not new_code_col:
            missing_cols.append("Mã mới")
        if not new_name_col:
            missing_cols.append("Tên mới")

        if missing_cols:
            # Nếu không tìm thấy tất cả các cột cần thiết, thử sử dụng 4 cột đầu tiên
            if len(df.columns) >= 4 and not missing_cols:
                logger.warning(
                    f"Không tìm thấy các cột: {missing_cols}. Sử dụng 4 cột đầu tiên..."
                )
                return {
                    "old_code": df.columns[0],
                    "old_name": df.columns[1],
                    "new_code": df.columns[2],
                    "new_name": df.columns[3],
                }
            else:
                # Hiển thị thông báo rõ ràng về cột thiếu và cột có sẵn
                error_msg = f"File mapping thiếu các cột bắt buộc: {', '.join(missing_cols)}. "
                error_msg += f"Các cột hiện có: {', '.join(df.columns)}"
                raise ValueError(error_msg)

        return {
            "old_code": old_code_col,
            "old_name": old_name_col,
            "new_code": new_code_col,
            "new_name": new_name_col,
        }

    def _load_mapping_data(self) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """
        Đọc dữ liệu mapping từ file mapping.

        Returns:
            Tuple[DataFrame chứa dữ liệu mapping, Dict ánh xạ từ tên cột chuẩn sang tên cột thực tế]
        """
        try:
            # Đọc file mapping - đọc tất cả cột dưới dạng chuỗi
            df_mapping = pd.read_excel(
                self.mapping_file,
                sheet_name=0,  # Giả sử sheet đầu tiên chứa dữ liệu mapping
                dtype=str,
            )

            # Làm sạch các giá trị NaN
            df_mapping = df_mapping.fillna("")

            # Xác định các cột mapping
            column_mapping = self._identify_mapping_columns(df_mapping)

            # Log thông tin về các cột đã xác định
            logger.info(f"Đã xác định các cột mapping: {column_mapping}")

            return df_mapping, column_mapping

        except Exception as e:
            logger.error(
                f"Lỗi khi đọc dữ liệu mapping: {str(e)}", exc_info=True
            )
            raise

    def _load_data_file(self) -> pd.DataFrame:
        """
        Đọc file dữ liệu cần xử lý.

        Returns:
            DataFrame chứa dữ liệu cần xử lý
        """
        try:
            df_data = pd.read_excel(
                self.data_file,
                sheet_name=0,  # Giả sử sheet đầu tiên chứa dữ liệu
                dtype=str,
            )

            # Kiểm tra xem cột cần thiết có tồn tại không
            if "Mã hàng" not in df_data.columns:
                logger.warning(
                    "Không tìm thấy cột 'Mã hàng' trong file dữ liệu!"
                )
                # Tìm cột có thể là mã hàng
                code_col = self._find_column_by_pattern(
                    df_data, self.old_code_patterns + ["ma.*hang", "code"]
                )
                if code_col:
                    logger.info(f"Sử dụng cột '{code_col}' làm cột mã hàng.")
                    # Đổi tên cột thành "Mã hàng" để tiếp tục xử lý
                    df_data = df_data.rename(columns={code_col: "Mã hàng"})
                else:
                    raise ValueError(
                        "File dữ liệu phải chứa cột 'Mã hàng' hoặc cột tương tự"
                    )

            # Tương tự cho Tên hàng
            if "Tên hàng" not in df_data.columns:
                name_col = self._find_column_by_pattern(
                    df_data, self.old_name_patterns + ["ten.*hang", "name"]
                )
                if name_col:
                    logger.info(f"Sử dụng cột '{name_col}' làm cột tên hàng.")
                    df_data = df_data.rename(columns={name_col: "Tên hàng"})

            # Log thông tin về các cột đã xác định
            logger.info(f"Các cột trong file dữ liệu: {list(df_data.columns)}")

            return df_data

        except Exception as e:
            logger.error(f"Lỗi khi đọc file dữ liệu: {str(e)}", exc_info=True)
            raise

    def process_to_buffer(self, output_buffer: io.BytesIO) -> None:
        """
        Xử lý file dữ liệu dựa trên file mapping và ghi vào buffer đầu ra.

        Args:
            output_buffer: BytesIO buffer để ghi dữ liệu đã xử lý
        """
        try:
            # Đọc dữ liệu mapping và dữ liệu
            mapping_df, column_mapping = self._load_mapping_data()
            data_df = self._load_data_file()

            # Lấy tên cột thực tế
            old_code_col = column_mapping["old_code"]
            new_code_col = column_mapping["new_code"]
            new_name_col = column_mapping["new_name"]

            # Tạo từ điển mapping để tra cứu nhanh hơn
            code_mapping = dict(
                zip(mapping_df[old_code_col], mapping_df[new_code_col])
            )
            name_mapping = dict(
                zip(mapping_df[old_code_col], mapping_df[new_name_col])
            )

            logger.info(f"Đã tạo mapping cho {len(code_mapping)} mã hàng")

            # Áp dụng mapping vào dữ liệu
            if "Mã hàng" in data_df.columns:
                # Tạo cột tạm chứa các giá trị đã ánh xạ
                data_df["Mã hàng_mới"] = data_df["Mã hàng"].map(code_mapping)
                data_df["Tên hàng_mới"] = data_df["Mã hàng"].map(name_mapping)

                # Đếm số lượng bản ghi được ánh xạ
                mapped_count = data_df["Mã hàng_mới"].notna().sum()
                logger.info(
                    f"Số lượng bản ghi được ánh xạ: {mapped_count}/{len(data_df)}"
                )

                # Thay thế giá trị khi có ánh xạ
                # Chỉ thay thế khi giá trị mới không phải là NaN và không phải chuỗi rỗng
                data_df.loc[
                    data_df["Mã hàng_mới"].notna()
                    & (data_df["Mã hàng_mới"] != ""),
                    "Mã hàng",
                ] = data_df["Mã hàng_mới"]

                if "Tên hàng" in data_df.columns:
                    data_df.loc[
                        data_df["Tên hàng_mới"].notna()
                        & (data_df["Tên hàng_mới"] != ""),
                        "Tên hàng",
                    ] = data_df["Tên hàng_mới"]

                # Xóa các cột tạm thời
                data_df = data_df.drop(columns=["Mã hàng_mới", "Tên hàng_mới"])

            # Ghi dữ liệu đã xử lý vào buffer
            with pd.ExcelWriter(output_buffer, engine="openpyxl") as writer:
                data_df.to_excel(writer, index=False)

            # Đặt lại vị trí buffer về đầu
            output_buffer.seek(0)
            logger.info("Đã hoàn thành xử lý và ghi vào buffer")

        except Exception as e:
            logger.error(f"Lỗi khi xử lý dữ liệu: {str(e)}", exc_info=True)
            raise

    def run(self) -> Path:
        """
        Chạy xử lý trên hệ thống file (khi sử dụng đường dẫn file).

        Returns:
            Đường dẫn đến file đầu ra
        """
        if not isinstance(self.data_file, Path):
            raise ValueError(
                "Không thể chạy với file in-memory. Sử dụng process_to_buffer() để thay thế."
            )

        output_file = (
            self.data_file.parent
            / f"{self.data_file.stem}_mapped{self.data_file.suffix}"
        )

        try:
            # Đọc và xử lý dữ liệu
            mapping_df, column_mapping = self._load_mapping_data()
            data_df = self._load_data_file()

            # Lấy tên cột thực tế
            old_code_col = column_mapping["old_code"]
            new_code_col = column_mapping["new_code"]
            new_name_col = column_mapping["new_name"]

            # Tạo ánh xạ
            code_mapping = dict(
                zip(mapping_df[old_code_col], mapping_df[new_code_col])
            )
            name_mapping = dict(
                zip(mapping_df[old_code_col], mapping_df[new_name_col])
            )

            # Áp dụng ánh xạ
            if "Mã hàng" in data_df.columns:
                # Tạo cột mới chứa giá trị đã ánh xạ
                data_df["Mã hàng_mới"] = data_df["Mã hàng"].map(code_mapping)
                data_df["Tên hàng_mới"] = data_df["Mã hàng"].map(name_mapping)

                # Thay thế giá trị khi có ánh xạ
                data_df.loc[
                    data_df["Mã hàng_mới"].notna()
                    & (data_df["Mã hàng_mới"] != ""),
                    "Mã hàng",
                ] = data_df["Mã hàng_mới"]

                if "Tên hàng" in data_df.columns:
                    data_df.loc[
                        data_df["Tên hàng_mới"].notna()
                        & (data_df["Tên hàng_mới"] != ""),
                        "Tên hàng",
                    ] = data_df["Tên hàng_mới"]

                # Xóa các cột tạm thời
                data_df = data_df.drop(columns=["Mã hàng_mới", "Tên hàng_mới"])

            # Lưu ra file
            data_df.to_excel(output_file, index=False)

            logger.info(f"File ánh xạ đã được lưu tại {output_file}")
            return output_file

        except Exception as e:
            logger.error(f"Lỗi trong quá trình ánh xạ: {str(e)}", exc_info=True)
            raise
