import base64
import io
import logging
import os
import tempfile
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware

# Thiết lập logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI()

origins = [
    "http://localhost:3000",
    "http://localhost",
    "http://45.117.77.126:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # hoặc dùng ["*"] để cho phép tất cả các origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Định nghĩa các extensions được phép
ALLOWED_EXTENSIONS = {".xlsx", ".xls"}

# Email để gửi file lỗi
ERROR_NOTIFICATION_EMAILS = [
    "songkhoi123@gmail.com",
    "nam.nguyen@lug.vn",
    "dang.le@sangtam.com",
    "tan.nguyen@sangtam.com",
]


def validate_excel_file(filename: str) -> None:
    """Kiểm tra file có phải là file Excel không."""
    suffix = Path(filename).suffix
    if suffix not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Chỉ chấp nhận file Excel (.xlsx, .xls). File của bạn có đuôi: {suffix}",
        )


async def send_error_file_email(
    file_content: bytes, original_filename: str, process_type: str
) -> bool:
    """
    Gửi file lỗi qua email.

    Args:
        file_content: Nội dung file lỗi
        original_filename: Tên file gốc
        process_type: Loại xử lý (online/offline)

    Returns:
        bool: True nếu gửi thành công
    """
    try:
        from util.mail_client import EmailClient
        from util.send_email import load_config

        # Tạo tên file đính kèm
        error_filename = (
            f"{Path(original_filename).stem}_invalid_{process_type}.xlsx"
        )

        # Tạo file tạm thời để đính kèm
        with tempfile.NamedTemporaryFile(
            suffix=".xlsx", delete=False
        ) as temp_file:
            temp_file.write(file_content)
            temp_file_path = temp_file.name

        try:
            # Tải cấu hình email
            config = load_config()

            # Tạo client và gửi email
            with EmailClient(**config) as client:
                msg = client.create_message(
                    to=ERROR_NOTIFICATION_EMAILS,
                    subject=f"File số điện thoại không hợp lệ - {original_filename}",
                    body=f"""
                    <p>Kính gửi,</p>
                    <p>Đính kèm là file chứa các số điện thoại không hợp lệ từ quá trình xử lý {process_type}.</p>
                    <p>File gốc: {original_filename}</p>
                    <p>Trân trọng,</p>
                    <p>Hệ thống tự động</p>
                    """,
                    html=True,
                )

                # Đính kèm file
                msg = client.attach_file(msg, temp_file_path)

                # Gửi email
                result = client.send(msg)

                if result:
                    recipients_str = ", ".join(ERROR_NOTIFICATION_EMAILS)
                    logger.info(
                        f"Đã gửi file lỗi '{error_filename}' qua email thành công đến: {recipients_str}"
                    )
                else:
                    logger.error(
                        f"Không thể gửi file lỗi '{error_filename}' qua email"
                    )

                return result
        finally:
            # Xóa file tạm
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    except Exception as e:
        logger.error(f"Lỗi khi gửi email file lỗi: {str(e)}", exc_info=True)
        return False


async def process_excel_file(
    file_content: bytes, filename: str, is_online: bool
) -> dict:
    """
    Xử lý file Excel theo mode online/offline.

    - Với online mode: gọi processors.online_processor, hàm process_to_buffer trả về
      tuple (valid_content, invalid_content, invalid_count).
    - Với offline mode: gọi processors.offline_processor, hàm process_to_buffer ghi dữ liệu vào output_buffer.

    Trả về JSON với các trường:
      - valid_file: base64 string của file hợp lệ.
      - invalid_file: base64 string của file không hợp lệ (nếu có, ngược lại là None).
      - invalid_count: số lượng bản ghi không hợp lệ.
    """
    try:
        if is_online:
            from processors.online_processor import (
                DaskExcelProcessor as Processor,
            )

            process_type = "online"
        else:
            from processors.offline_processor import (
                DaskExcelProcessor as Processor,
            )

            process_type = "offline"

        input_buffer = io.BytesIO(file_content)
        processor = Processor(input_buffer)
        output_buffer = io.BytesIO()

        # Gọi hàm process_to_buffer
        result = processor.process_to_buffer(output_buffer)
        if result is not None:
            # Online mode: process_to_buffer trả về tuple
            valid_content, invalid_content, invalid_count = result
        else:
            # Offline mode: dữ liệu được ghi vào output_buffer
            valid_content = output_buffer.getvalue()
            invalid_content = None
            invalid_count = 0

        # Gửi file lỗi qua email nếu có
        if invalid_content and invalid_count > 0:
            await send_error_file_email(invalid_content, filename, process_type)

        # Mã hóa nội dung file sang base64
        valid_file_b64 = (
            base64.b64encode(valid_content).decode("utf-8")
            if valid_content
            else None
        )
        invalid_file_b64 = (
            base64.b64encode(invalid_content).decode("utf-8")
            if invalid_content
            else None
        )

        return {
            "valid_file": valid_file_b64,
            "invalid_file": invalid_file_b64,
            "invalid_count": invalid_count,
        }

    except Exception as e:
        logger.error(f"Lỗi khi xử lý file: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=400, detail=f"Lỗi khi xử lý file: {str(e)}"
        )


@app.post("/process/online")
async def process_online(file: UploadFile):
    """Endpoint xử lý file theo mode online."""
    logger.info(f"Đang xử lý file online: {file.filename}")
    validate_excel_file(file.filename)
    content = await file.read()
    result = await process_excel_file(content, file.filename, is_online=True)
    return result


@app.post("/process/offline")
async def process_offline(file: UploadFile):
    """Endpoint xử lý file theo mode offline."""
    logger.info(f"Đang xử lý file offline: {file.filename}")
    validate_excel_file(file.filename)
    content = await file.read()
    result = await process_excel_file(content, file.filename, is_online=False)
    return result


@app.post("/process/mapping")
async def process_mapping(
    firstFile: UploadFile = File(...), secondFile: UploadFile = File(...)
):
    """
    Endpoint ánh xạ mã hàng và tên hàng dựa trên file mapping.

    Nhận hai file:
    - firstFile: File dữ liệu cần ánh xạ (raw file từ frontend)
    - secondFile: File chứa bảng ánh xạ mã hàng/tên hàng cũ và mới (warranty file từ frontend)

    Trả về JSON chứa base64 của file đã ánh xạ.
    """
    # Kiểm tra xem đã nhận được các tệp chưa
    if not firstFile:
        raise HTTPException(status_code=400, detail="firstFile là bắt buộc")
    if not secondFile:
        raise HTTPException(status_code=400, detail="secondFile là bắt buộc")

    logger.info(
        f"Đang xử lý ánh xạ mã hàng. File dữ liệu: {firstFile.filename}, File mapping: {secondFile.filename}"
    )

    # Kiểm tra định dạng file
    validate_excel_file(firstFile.filename)
    validate_excel_file(secondFile.filename)

    try:
        from processors.product_mapping_processor import ProductMappingProcessor

        # Đọc nội dung file
        data_content = await firstFile.read()
        mapping_content = await secondFile.read()

        # Đảm bảo nội dung không rỗng
        if not data_content:
            raise HTTPException(status_code=400, detail="File dữ liệu trống")
        if not mapping_content:
            raise HTTPException(status_code=400, detail="File mapping trống")

        # Tạo buffer cho các file đầu vào và đầu ra
        data_buffer = io.BytesIO(data_content)
        mapping_buffer = io.BytesIO(mapping_content)
        output_buffer = io.BytesIO()

        # Xử lý ánh xạ
        processor = ProductMappingProcessor(data_buffer, mapping_buffer)
        process_info = processor.process_to_buffer(output_buffer)

        # Lấy thông tin về số lượng bản ghi đã ánh xạ
        matched_count = (
            process_info.get("matched_count", 0)
            if isinstance(process_info, dict)
            else 0
        )
        total_count = (
            process_info.get("total_count", 0)
            if isinstance(process_info, dict)
            else 0
        )

        # Mã hóa nội dung file sang base64
        output_buffer.seek(0)
        mapped_file_b64 = base64.b64encode(output_buffer.getvalue()).decode(
            "utf-8"
        )

        # Tạo tên file kết quả
        output_filename = f"{Path(firstFile.filename).stem}_mapped.xlsx"

        # Trả về JSON thay vì file trực tiếp
        return {
            "resultFile": mapped_file_b64,
            "filename": output_filename,
            "matchedCount": matched_count,
            "totalCount": total_count,
        }

    except Exception as e:
        logger.error(f"Lỗi khi ánh xạ mã hàng: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=400, detail=f"Lỗi khi ánh xạ mã hàng: {str(e)}"
        )
