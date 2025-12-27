import asyncio
import base64
import io
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path

import httpx
import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from pydantic import BaseModel, Field

from src.accounting.api import router as accounting_router
from src.sapo_sync import SapoSyncRequest, sync_mysapo, sync_mysapogo
from src.settings import app_settings
from src.util import validate_excel_file
from src.util.apprise_notification import send_warranty_match_email
from src.util.logging import setup_logging
from src.util.logging.middleware import setup_fastapi_logging

# Initialize Loguru for this module
setup_logging(log_to_file=False)

# Create FastAPI application with custom title and description
app = FastAPI(
    title="LUG Backend API",
    description="API for processing files and handling warranty registrations",
    version="0.1.0",
)

# Configure FastAPI with Loguru logging middleware
setup_fastapi_logging(app)


@app.on_event("startup")
async def startup_event():
    """Initialize application data on startup."""
    logger.info("Initializing application startup tasks...")

    # Initialize cancel reasons for mysapogo.com sync
    try:
        from sapo_sync.mysapogo_com import initialize_cancel_reasons

        success = await initialize_cancel_reasons()
        if success:
            logger.info("Cancel reasons initialized successfully from API")
        else:
            logger.warning("Cancel reasons initialized using fallback data")
    except Exception as e:
        logger.error(
            f"Failed to initialize cancel reasons: {str(e)}", exc_info=True
        )

    logger.info("Application startup completed")


origins = app_settings.cors_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # hoặc dùng ["*"] để cho phép tất cả các origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include the accounting router
app.include_router(accounting_router)

ALLOWED_EXTENSIONS = app_settings.allowed_extensions
ERROR_NOTIFICATION_EMAILS = app_settings.error_notification_emails
ONLINE_ERROR_NOTIFICATION_EMAILS = app_settings.online_error_notification_emails


# Thêm endpoint này vào file src/api/server.py
@app.post("/sapo/sync")
async def sync_sapo(request_data: SapoSyncRequest):
    """
    Endpoint đồng bộ dữ liệu từ Sapo APIs vào Google Sheets

    Args:
        request_data (SapoSyncRequest): Chứa startDate và endDate với định dạng YYYY-MM-DD

    Returns:
        dict: Trạng thái của quá trình đồng bộ
    """
    try:
        logger.info(
            f"Bắt đầu đồng bộ Sapo từ {request_data.startDate} đến {request_data.endDate}"
        )

        # Gọi các hàm đồng bộ đồng thời
        mysapo_task = sync_mysapo(request_data.startDate, request_data.endDate)
        mysapogo_task = sync_mysapogo(
            request_data.startDate, request_data.endDate
        )

        # Đợi cả hai task hoàn thành
        mysapo_result, mysapogo_result = await asyncio.gather(
            mysapo_task, mysapogo_task
        )

        total_orders_processed = mysapo_result.get(
            "orders_processed", 0
        ) + mysapogo_result.get("orders_processed", 0)

        return {
            "success": True,
            "message": f"Đồng bộ Sapo hoàn tất. Tổng số đơn hàng đã xử lý: {total_orders_processed}",
            "mysapo_net": mysapo_result,
            "mysapogo_com": mysapogo_result,
        }
    except Exception as e:
        logger.error(
            f"Lỗi trong quá trình đồng bộ Sapo: {str(e)}", exc_info=True
        )
        raise HTTPException(
            status_code=500, detail=f"Lỗi khi xử lý đồng bộ Sapo: {str(e)}"
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
        from src.util.mail_client import EmailClient
        from src.util.send_email import load_config

        # Chọn danh sách email nhận thông báo dựa trên loại process
        recipients = (
            ONLINE_ERROR_NOTIFICATION_EMAILS
            if process_type == "online"
            else ERROR_NOTIFICATION_EMAILS
        )

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
                    to=recipients,
                    subject=f"File số điện thoại không hợp lệ - {original_filename}",
                    body=f"""
                    <p>Dear,</p>
                    <p>File chứa các số điện thoại không hợp lệ từ quá trình xử lý {process_type}.</p>
                    <p>File gốc: {original_filename}</p>
                    <p>Hệ thống tự động, vui lòng không reply lại email này.</p>
                    """,
                    html=True,
                )

                # Đính kèm file
                msg = client.attach_file(msg, temp_file_path)

                # Gửi email
                result = client.send(msg)

                if result:
                    recipients_str = ", ".join(recipients)
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
      tuple (valid_content, invalid_content, kl_records_json, invalid_count).
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
        if is_online:
            # Online mode: process_to_buffer trả về tuple (valid_content, invalid_content, kl_records_json, invalid_count)
            valid_content, invalid_content, kl_records_json, invalid_count = (
                processor.process_to_buffer(output_buffer)
            )

            # Gửi dữ liệu KL records tới API nếu có
            if kl_records_json:
                try:
                    # Lấy xc-token từ biến môi trường
                    xc_token = os.environ.get("XC_TOKEN")
                    if not xc_token:
                        logger.warning("XC_TOKEN environment variable not set")
                        xc_token = ""  # Sử dụng token mặc định nếu không có

                    # Parse JSON từ kl_records_json
                    records_data = json.loads(kl_records_json)

                    def format_document_number(value, field_type=None):
                        """
                        Định dạng số chứng từ, giữ nguyên các số 0 ở đầu hoặc thêm số 0 để đạt độ dài nhất định.

                        Args:
                            value: Giá trị cần định dạng
                            field_type: Loại trường (ma_ct, so_ct, etc.) để xử lý đặc biệt
                        """
                        if pd.isna(value) or value is None:
                            return ""

                        # Xử lý thành chuỗi trước, loại bỏ dấu thập phân nếu cần
                        if isinstance(value, float) and value.is_integer():
                            value_str = str(int(value))
                        else:
                            value_str = str(value)

                        # Xử lý đặc biệt cho so_ct: Thêm số 0 phía trước để đạt đủ 4 ký tự
                        if field_type == "so_ct":
                            return value_str.zfill(
                                4
                            )  # Thêm 0 phía trước để đạt đủ 4 ký tự

                        # Các trường khác giữ nguyên
                        return value_str

                    # Hàm định dạng số lượng và doanh thu
                    def format_numeric(value):
                        """Định dạng các trường số, chuyển thành số nguyên nếu có thể."""
                        if pd.isna(value) or value is None:
                            return ""

                        # Nếu là số thập phân với phần thập phân là 0
                        if isinstance(value, float) and value.is_integer():
                            return str(int(value))

                        # Trường hợp còn lại
                        return str(value)

                    # Chuyển đổi định dạng dữ liệu theo yêu cầu
                    transformed_records = []
                    for record in records_data:
                        # Chuyển đổi định dạng ngày từ dd/mm/yyyy sang yyyy-mm-dd
                        formatted_date = record.get("Ngày Ct") or ""
                        if formatted_date and "/" in formatted_date:
                            try:
                                # Parse the date assuming dd/mm/yyyy format
                                date_parts = formatted_date.split("/")
                                if len(date_parts) == 3:
                                    day, month, year = date_parts
                                    # Reformat to yyyy-mm-dd
                                    formatted_date = f"{year}-{month}-{day}"
                            except Exception as e:
                                logger.error(
                                    f"Error formatting date {formatted_date}: {str(e)}"
                                )

                        # Tạo record mới theo định dạng yêu cầu
                        transformed_record = {
                            "ngay_ct": formatted_date,
                            "ma_ct": format_document_number(
                                record.get("Mã Ct")
                            ),
                            "so_ct": format_document_number(
                                record.get("Số Ct"), "so_ct"
                            ),
                            "ma_bo_phan": format_document_number(
                                record.get("Mã bộ phận")
                            ),
                            "ma_don_hang": format_document_number(
                                record.get("Mã đơn hàng")
                            ),
                            "ten_khach_hang": str(
                                record.get("Tên khách hàng") or ""
                            ),
                            "so_dien_thoai": str(
                                record.get("Số điện thoại") or ""
                            ).strip(),
                            "tinh_thanh": str(record.get("Tỉnh thành") or ""),
                            "quan_huyen": str(record.get("Quận huyện") or ""),
                            "phuong_xa": str(record.get("Phường xã") or ""),
                            "dia_chi": str(record.get("Địa chỉ") or ""),
                            "ma_hang": format_document_number(
                                record.get("Mã hàng")
                            ),
                            "ten_hang": str(record.get("Tên hàng") or ""),
                            "imei": str(record.get("Imei") or ""),
                            "so_luong": format_numeric(record.get("Số lượng")),
                            "doanh_thu": format_numeric(
                                record.get("Doanh thu")
                            ),
                            "ghi_chu": str(record.get("Ghi chú") or ""),
                        }
                        transformed_records.append(transformed_record)

                    # Debug: Hiển thị dữ liệu đã chuyển đổi
                    logger.info(
                        f"Đang gửi {len(transformed_records)} bản ghi KL đến API"
                    )
                    print("\n=== DEBUG: TRANSFORMED DATA FORMAT ===")
                    if transformed_records:
                        print(
                            json.dumps(
                                transformed_records[0],
                                indent=2,
                                ensure_ascii=False,
                            )
                        )
                    print("======================================\n")

                    # Gửi dữ liệu đến API
                    async with httpx.AsyncClient(timeout=30.0) as client:
                        response = await client.post(
                            f"{app_settings.api_endpoint}/tables/mtvvlryi3xc0gqd/records",
                            json=transformed_records,
                            headers={
                                "Content-Type": "application/json",
                                "xc-token": xc_token,
                            },
                        )

                        # Ghi log kết quả
                        if response.status_code == 200:
                            logger.info(
                                f"Đã gửi dữ liệu KL records thành công: {response.status_code}"
                            )
                            print("\n=== DEBUG: API RESPONSE ===")
                            print(f"Status code: {response.status_code}")
                            print(
                                f"Response: {response.text[:200]}..."
                                if len(response.text) > 200
                                else f"Response: {response.text}"
                            )
                            print("===========================\n")
                        else:
                            logger.warning(
                                f"Không thể gửi dữ liệu KL records, mã lỗi: {response.status_code}"
                            )
                            print("\n=== DEBUG: API ERROR ===")
                            print(f"Status code: {response.status_code}")
                            print(f"Response: {response.text}")
                            print("=======================\n")
                except Exception as e:
                    # Chỉ ghi log lỗi, không làm gián đoạn quá trình xử lý chính
                    logger.error(
                        f"Lỗi khi gửi dữ liệu KL records: {str(e)}",
                        exc_info=True,
                    )
        else:
            # Offline mode: dữ liệu được ghi vào output_buffer
            result = processor.process_to_buffer(output_buffer)
            if result is not None:
                valid_content, invalid_content, invalid_count = result
            else:
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


class WarrantyRequest(BaseModel):
    name: str
    phone: str
    order_code: str
    purchase_platform: str = Field(default="")
    captchaToken: str


@app.post("/warranty")
async def submit_warranty(request: WarrantyRequest):
    """
    Process warranty registration form submissions.

    Frontend validation ensures phone number is valid using EXACT same regex as backend.
    No need for backend phone validation since frontend uses identical logic.

    - Kiểm tra xem mã đơn hàng đã đăng ký chưa
    - Kiểm tra có trong bảng mtvvlryi3xc0gqd không
    - Tìm kiếm trong bảng mydoap8edbr206g để phát hiện đăng ký trùng
    """
    logger.info(
        f"Received warranty registration for {request.name} with order code {request.order_code}"
    )

    try:
        # Lấy xc-token từ biến môi trường
        xc_token = app_settings.xc_token

        # Phone number is already validated by frontend using EXACT same regex
        # No need to validate again since frontend uses identical validation logic
        formatted_phone = request.phone
        # First, check if the order code has already been registered
        order_code = request.order_code
        check_url = f"{app_settings.api_endpoint}/tables/miyw4f4yeojamv6/records?where=(ma_don_hang%2Ceq%2C{order_code})&limit=1"

        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "xc-token": xc_token,
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            # Check if order code already exists in the warranty registration table
            check_response = await client.get(check_url, headers=headers)

            if check_response.status_code != 200:
                logger.error(
                    f"Error checking order registration: {check_response.status_code} - {check_response.text}"
                )
                return {
                    "success": False,
                    "message": f"Lỗi khi kiểm tra đơn hàng: {check_response.status_code}",
                }

            check_data = check_response.json()
            existing_records = check_data.get("list", [])

            # If the order code already exists, return a specific response
            if existing_records:
                logger.info(
                    f"Order code {order_code} has already been registered for warranty"
                )
                return {
                    "success": False,
                    "message": "Mã đơn hàng này đã được đăng ký bảo hành trước đó.",
                    "already_registered": True,
                }

            # Gọi API để tìm thông tin đơn hàng dựa trên mã đơn hàng
            search_url = f"{app_settings.api_endpoint}/tables/mtvvlryi3xc0gqd/records?where=(ma_don_hang%2Ceq%2C{order_code})&limit=1000&shuffle=0&offset=0"

            logger.info(f"Searching for order with code: {order_code}")
            search_response = await client.get(search_url, headers=headers)

            if search_response.status_code != 200:
                logger.error(
                    f"Error searching for order: {search_response.status_code} - {search_response.text}"
                )
                return {
                    "success": False,
                    "message": f"Lỗi khi tìm kiếm đơn hàng: {search_response.status_code}",
                }

            search_data = search_response.json()
            records = search_data.get("list", [])

            # THÊM MỚI: Kiểm tra xem đơn hàng có trong bảng tạm không
            if not records:
                logger.info(
                    f"Checking if order {order_code} exists in pending table"
                )
                pending_url = f"{app_settings.api_endpoint}/tables/mydoap8edbr206g/records?where=(ma_don_hang%2Ceq%2C{order_code})&limit=1"

                pending_response = await client.get(
                    pending_url, headers=headers
                )
                if pending_response.status_code == 200:
                    pending_data = pending_response.json()
                    pending_records = pending_data.get("list", [])

                    if pending_records:
                        logger.info(
                            f"Order code {order_code} found in pending registrations"
                        )
                        # Cập nhật thông tin liên hệ cho bản ghi chờ
                        pending_id = pending_records[0].get("Id")
                        update_url = f"{app_settings.api_endpoint}/tables/mydoap8edbr206g/records/{pending_id}"

                        update_data = {
                            "ho_ten": request.name,
                            "so_dien_thoai": formatted_phone,
                            "noi_mua": request.purchase_platform or "website",
                        }

                        update_response = await client.patch(
                            update_url, headers=headers, json=update_data
                        )
                        if update_response.status_code == 200:
                            logger.info(
                                f"Updated pending registration for order {order_code}"
                            )
                        else:
                            logger.warning(
                                f"Failed to update pending registration: {update_response.status_code}"
                            )

                        # Kích hoạt task Celery ngay lập tức
                        from src.tasks.worker import sync_pending_registrations

                        sync_pending_registrations.delay()

                        return {
                            "success": True,
                            "message": "Đơn hàng của bạn đã được ghi nhận, chúng tôi sẽ xử lý sớm nhất có thể.",
                            "pending": True,
                        }

            # Nếu không tìm thấy đơn hàng, lưu thông tin vào bảng mydoap8edbr206g
            if not records:
                logger.warning(f"No records found for order code: {order_code}")

                # THAY ĐỔI: Thay vì trả về lỗi, lưu thông tin vào bảng mydoap8edbr206g
                registration_data_new_table = {
                    "ho_ten": request.name,
                    "so_dien_thoai": formatted_phone,
                    "noi_mua": request.purchase_platform or "website",
                    "ma_don_hang": request.order_code,
                    "ghi_chu": "Đăng ký không có mã đơn hàng trong hệ thống",
                }

                # Lưu vào bảng mới
                new_table_url = f"{app_settings.api_endpoint}/tables/mydoap8edbr206g/records"

                try:
                    new_table_response = await client.post(
                        new_table_url,
                        headers=headers,
                        json=registration_data_new_table,
                    )

                    if new_table_response.status_code not in (200, 201):
                        logger.warning(
                            f"Error saving to new table: {new_table_response.status_code} - {new_table_response.text}"
                        )
                except Exception as e:
                    logger.error(f"Error posting to new table: {str(e)}")

                # Lưu thông tin vào bảng theo dõi
                registration_url = f"{app_settings.api_endpoint}/tables/miyw4f4yeojamv6/records"

                registration_data = {
                    "ho_ten": request.name,
                    "so_dien_thoai": formatted_phone,
                    "noi_mua": request.purchase_platform or "website",
                    "ma_don_hang": request.order_code,
                }

                try:
                    registration_response = await client.post(
                        registration_url,
                        headers=headers,
                        json=registration_data,
                    )

                    if registration_response.status_code not in (200, 201):
                        logger.warning(
                            f"Error saving registration info: {registration_response.status_code} - {registration_response.text}"
                        )
                except Exception as e:
                    logger.error(
                        f"Error posting to registration table: {str(e)}"
                    )

                # Trả về thành công thay vì lỗi
                return {
                    "success": True,
                    "message": "Đăng ký bảo hành thành công!",
                    "order_found": False,
                }

            logger.info(
                f"Found {len(records)} items for order code {order_code}"
            )

            # Bước 2: Chuẩn bị dữ liệu để sao chép sang bảng khác
            # Cập nhật tên khách hàng và số điện thoại từ form đăng ký
            records_to_copy = []
            record_ids = []

            for record in records:
                # Lưu ID để xóa sau này
                record_ids.append({"Id": record["Id"]})

                # Tạo bản ghi mới với thông tin cập nhật từ form
                new_record = {
                    "ngay_ct": record["ngay_ct"],
                    "ma_ct": record["ma_ct"],
                    "so_ct": record["so_ct"],
                    "ma_bo_phan": record["ma_bo_phan"],
                    "ma_don_hang": record["ma_don_hang"],
                    "ten_khach_hang": request.name,  # Cập nhật tên từ form
                    "so_dien_thoai": formatted_phone,  # Cập nhật số điện thoại đã format
                    "tinh_thanh": record["tinh_thanh"],
                    "quan_huyen": record["quan_huyen"],
                    "phuong_xa": record["phuong_xa"],
                    "dia_chi": record["dia_chi"],
                    "ma_hang": record["ma_hang"],
                    "ten_hang": record["ten_hang"],
                    "imei": record["imei"],
                    "so_luong": record["so_luong"],
                    "doanh_thu": record["doanh_thu"],
                    "ghi_chu": f"Đăng ký bảo hành qua {request.purchase_platform or 'website'}",  # Thêm ghi chú
                }
                records_to_copy.append(new_record)

            # Bước 3: Sao chép dữ liệu sang bảng đích
            copy_url = (
                f"{app_settings.api_endpoint}/tables/mffwo1asni22n9z/records"
            )

            logger.info(
                f"Copying {len(records_to_copy)} records to warranty table"
            )
            copy_response = await client.post(
                copy_url, headers=headers, json=records_to_copy
            )

            if copy_response.status_code not in (200, 201):
                logger.error(
                    f"Error copying records: {copy_response.status_code} - {copy_response.text}"
                )
                return {
                    "success": False,
                    "message": "Lỗi khi lưu thông tin bảo hành",
                }

            # Bước 3.1: Gửi toàn bộ dữ liệu matched records đến CRM
            try:
                crm_target_url = app_settings.get_env("CRM_TARGET_URL")
                crm_api_key = app_settings.get_env("CRM_API_KEY")

                if crm_target_url and crm_api_key:
                    # Chuẩn bị dữ liệu CRM từ complete matched records từ 127.0.0.1:8081
                    # Group all products under ONE master record for the order

                    # Use first record for master data (all records have same order info)
                    first_record = records[0]

                    # Extract date part only from datetime string (YYYY-MM-DD)
                    date_str = first_record.get("ngay_ct", "")
                    if date_str and len(date_str) >= 10:
                        date_str = date_str[:10]

                    # Handle null/empty values - use None instead of defaults to match CRM example
                    tinh_thanh = first_record.get("tinh_thanh") or None
                    quan_huyen = first_record.get("quan_huyen") or None
                    phuong_xa = first_record.get("phuong_xa") or None
                    dia_chi = first_record.get("dia_chi") or None

                    # Build detail array with all products
                    detail_items = []
                    for record in records:
                        # Ensure numeric fields are properly formatted
                        so_luong = record.get("so_luong", "1")
                        try:
                            so_luong = int(float(so_luong)) if so_luong else 1
                        except (ValueError, TypeError):
                            so_luong = 1

                        doanh_thu = record.get("doanh_thu", "0")
                        try:
                            doanh_thu = float(doanh_thu) if doanh_thu else 0
                        except (ValueError, TypeError):
                            doanh_thu = 0

                        detail_item = {
                            "maHang": record.get("ma_hang", ""),
                            "tenHang": record.get("ten_hang", ""),
                            "imei": record.get("imei", ""),
                            "soLuong": so_luong,
                            "doanhThu": doanh_thu,
                        }
                        detail_items.append(detail_item)

                    # Create single CRM record with all products in detail array
                    crm_record = {
                        "master": {
                            "ngayCT": date_str,
                            "maCT": first_record.get("ma_ct", ""),
                            "soCT": first_record.get("so_ct", "").zfill(4)
                            if first_record.get("so_ct")
                            else "0001",  # Zero-pad to 4 digits
                            "maBoPhan": first_record.get("ma_bo_phan", ""),
                            "maDonHang": first_record.get("ma_don_hang", ""),
                            "tenKhachHang": request.name,  # Use registered name from warranty form
                            "soDienThoai": formatted_phone,  # Use registered phone from warranty form
                            "tinhThanh": tinh_thanh,
                            "quanHuyen": quan_huyen,
                            "phuongXa": phuong_xa,
                            "diaChi": dia_chi,
                        },
                        "detail": detail_items,
                    }

                    # Prepare CRM payload with single record containing all products
                    crm_payload = {
                        "apikey": crm_api_key,
                        "data": [crm_record],  # Single record with all products
                    }

                    logger.info(
                        f"Sending 1 order with {len(detail_items)} product(s) to CRM: {crm_target_url}"
                    )

                    # Debug: Log the CRM record being sent
                    logger.info(
                        f"CRM record: {json.dumps(crm_record, ensure_ascii=False, indent=2)}"
                    )

                    crm_response = await client.post(
                        crm_target_url,
                        headers={"Content-Type": "application/json"},
                        json=crm_payload,
                        timeout=30.0,  # Reasonable timeout for CRM call
                    )

                    crm_response_text = crm_response.text
                    logger.info(
                        f"CRM Response Status: {crm_response.status_code}"
                    )
                    logger.info(f"CRM Response Body: {crm_response_text}")

                    if crm_response.status_code in (200, 201):
                        logger.info("Successfully sent warranty order to CRM")
                        # Try to parse CRM response
                        try:
                            crm_result = crm_response.json()
                            if crm_result.get("status") == 1:
                                logger.info("CRM confirmed successful import")
                            else:
                                logger.warning(
                                    f"CRM import had issues: {crm_result}"
                                )
                        except Exception as parse_e:
                            logger.warning(
                                f"Could not parse CRM response: {parse_e}"
                            )
                    else:
                        logger.warning(
                            f"CRM integration failed: {crm_response.status_code} - {crm_response_text}"
                        )
                else:
                    logger.info(
                        "CRM_TARGET_URL or CRM_API_KEY not configured, skipping CRM integration"
                    )
            except Exception as crm_e:
                # CRM integration failure should not affect warranty registration
                logger.error(
                    f"CRM integration error (non-blocking): {str(crm_e)}",
                    exc_info=True,
                )
                # Continue with warranty process even if CRM fails

            # Bước 3.2: Send matched customer data via email using Apprise
            try:
                logger.info(
                    "Sending matched warranty data via email using Apprise"
                )
                email_sent = send_warranty_match_email(records_to_copy)
                if email_sent:
                    logger.info(
                        "Successfully sent matched warranty data via email"
                    )
                else:
                    logger.warning(
                        "Failed to send matched warranty data via email"
                    )
            except Exception as email_e:
                # Email notification failure should not affect warranty registration
                logger.error(
                    f"Email notification error (non-blocking): {str(email_e)}",
                    exc_info=True,
                )
                # Continue with warranty process even if email fails

            # Bước 4: Xóa bản ghi gốc
            delete_url = (
                f"{app_settings.api_endpoint}/tables/mtvvlryi3xc0gqd/records"
            )

            logger.info(f"Deleting {len(record_ids)} original records")
            delete_response = await client.request(
                method="DELETE",
                url=delete_url,
                headers=headers,
                json=record_ids,
            )

            if delete_response.status_code != 200:
                logger.warning(
                    f"Error deleting original records: {delete_response.status_code} - {delete_response.text}"
                )
                # Không trả về lỗi ở đây, vì thông tin đã được sao chép thành công

            # Bước 5: Lưu thông tin người đăng ký vào bảng theo dõi
            registration_url = (
                f"{app_settings.api_endpoint}/tables/miyw4f4yeojamv6/records"
            )

            registration_data = {
                "ho_ten": request.name,
                "so_dien_thoai": formatted_phone,
                "noi_mua": request.purchase_platform or "website",
                "ma_don_hang": request.order_code,
            }

            logger.info(f"Saving registration info: {registration_data}")
            registration_response = await client.post(
                registration_url, headers=headers, json=registration_data
            )

            if registration_response.status_code not in (200, 201):
                logger.warning(
                    f"Error saving registration info: {registration_response.status_code} - {registration_response.text}"
                )
                # Không trả về lỗi ở đây, vì các bước chính đã hoàn thành

            return {
                "success": True,
                "message": "Đăng ký bảo hành thành công! Vui lòng kiểm tra app lugID",
                "items_processed": len(records_to_copy),
                "order_found": True,
            }

    except httpx.HTTPStatusError as e:
        logger.error(
            f"External API error: {e.response.status_code} - {e.response.text}"
        )
        return {
            "success": False,
            "message": "Không thể xử lý đăng ký bảo hành. Vui lòng thử lại sau.",
        }
    except Exception as e:
        logger.error(
            f"Error processing warranty request: {str(e)}", exc_info=True
        )
        return {
            "success": False,
            "message": "Đã xảy ra lỗi khi xử lý đăng ký bảo hành.",
        }


@app.get("/")
async def root():
    """
    Root endpoint - API information
    """
    return {
        "name": app_settings.app_name,
        "version": "0.1.0",
        "status": "running",
        "endpoints": {
            "online_processing": "/process/online",
            "offline_processing": "/process/offline",
            "product_mapping": "/process/product-mapping",
            "accounting": "/accounting",
            "accounting_online": "/accounting/process/online",
            "accounting_offline": "/accounting/process/offline",
        },
    }


@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    try:
        # Test Redis connection
        redis_status = "ok"
        try:
            bank_processor.processor.redis_cache.redis_client.ping()
        except Exception as e:
            redis_status = f"error: {str(e)}"

        # Check accounting processor database connection
        accounting_status = "ok"
        try:
            from src.accounting.api import processor

            if processor.connect():
                processor.close()
            else:
                accounting_status = (
                    "error: could not connect to accounting database"
                )
        except Exception as e:
            accounting_status = f"error: {str(e)}"

        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {
                "redis": redis_status,
                "database": "ok",  # Add database check if needed
                "accounting": accounting_status,
            },
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat(),
        }
