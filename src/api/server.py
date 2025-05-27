import asyncio
import base64
import io
import json
import os
import tempfile
from pathlib import Path

import httpx
import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from pydantic import BaseModel, Field

from sapo_sync import SapoSyncRequest, sync_mysapo, sync_mysapogo
from settings import app_settings
from util import format_phone_number, validate_excel_file
from util.logging import setup_logging
from util.logging.middleware import setup_fastapi_logging

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

origins = app_settings.cors_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # hoặc dùng ["*"] để cho phép tất cả các origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
        from util.mail_client import EmailClient
        from util.send_email import load_config

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

        # Format số điện thoại
        formatted_phone = format_phone_number(request.phone)

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


# ============================================================================
# NEW BANK STATEMENT PROCESSING ENDPOINTS
# ============================================================================


@app.post("/process/bank-statement")
async def process_bank_statement(file: UploadFile = File(...)):
    """
    Process single bank statement file and return Excel stream
    Similar to /process/online and /process/offline
    """
    try:
        # Validate file
        validate_excel_file(file.filename, {".ods", ".xlsx", ".xls"})

        # Read file content
        content = await file.read()
        input_buffer = io.BytesIO(content)

        logger.info(f"Processing bank statement: {file.filename}")

        # Process the file
        result = await _process_bank_statement_stream(input_buffer)

        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["error"])

        # Return Excel stream
        output_filename = f"saoke_{file.filename.split('.')[0]}.xlsx"

        return StreamingResponse(
            io.BytesIO(result["excel_data"]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={output_filename}"
            },
        )

    except Exception as e:
        logger.error(
            f"Error processing bank statement: {str(e)}", exc_info=True
        )
        raise HTTPException(
            status_code=500, detail=f"Processing error: {str(e)}"
        )


@app.post("/process/bank-statement-multiple")
async def process_multiple_bank_statements(
    files: List[UploadFile] = File(...), strategy: str = Form("smart")
):
    """
    Process multiple bank statement files and return ZIP with Excel files
    """
    try:
        if len(files) > 20:  # Reasonable limit
            raise HTTPException(
                status_code=400, detail="Maximum 20 files allowed"
            )

        # Validate all files
        for file in files:
            validate_excel_file(file.filename, {".ods", ".xlsx", ".xls"})

        logger.info(f"Processing {len(files)} bank statement files")

        # Process all files
        processed_files = []
        failed_files = []

        for file in files:
            try:
                content = await file.read()
                input_buffer = io.BytesIO(content)

                result = await _process_bank_statement_stream(input_buffer)

                if result["success"]:
                    processed_files.append(
                        {
                            "filename": f"saoke_{file.filename.split('.')[0]}.xlsx",
                            "data": result["excel_data"],
                            "transactions": result["transactions"],
                        }
                    )
                else:
                    failed_files.append(
                        {"filename": file.filename, "error": result["error"]}
                    )

            except Exception as e:
                failed_files.append(
                    {"filename": file.filename, "error": str(e)}
                )

        if not processed_files:
            raise HTTPException(
                status_code=500, detail="No files were processed successfully"
            )

        # Create ZIP with all Excel files
        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            # Add processed Excel files
            for pf in processed_files:
                zip_file.writestr(pf["filename"], pf["data"])

            # Add summary report if there were failures
            if failed_files or len(processed_files) > 1:
                summary_data = []
                for pf in processed_files:
                    summary_data.append(
                        {
                            "File": pf["filename"],
                            "Status": "Success",
                            "Transactions": pf["transactions"],
                            "Error": "",
                        }
                    )

                for ff in failed_files:
                    summary_data.append(
                        {
                            "File": ff["filename"],
                            "Status": "Failed",
                            "Transactions": 0,
                            "Error": ff["error"],
                        }
                    )

                summary_df = pd.DataFrame(summary_data)
                summary_buffer = io.BytesIO()
                summary_df.to_excel(summary_buffer, index=False)
                zip_file.writestr(
                    "processing_summary.xlsx", summary_buffer.getvalue()
                )

        zip_buffer.seek(0)

        return StreamingResponse(
            io.BytesIO(zip_buffer.getvalue()),
            media_type="application/zip",
            headers={
                "Content-Disposition": "attachment; filename=bank_statements_processed.zip"
            },
        )

    except Exception as e:
        logger.error(
            f"Error processing multiple files: {str(e)}", exc_info=True
        )
        raise HTTPException(
            status_code=500, detail=f"Processing error: {str(e)}"
        )


@app.post("/process/bank-statement-async")
async def process_bank_statement_async(
    background_tasks: BackgroundTasks, file: UploadFile = File(...)
):
    """
    Start async processing and return status endpoint for polling
    """
    try:
        # Validate file
        validate_excel_file(file.filename, {".ods", ".xlsx", ".xls"})

        # Save file temporarily
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=".ods"
        ) as tmp_file:
            content = await file.read()
            tmp_file.write(content)
            tmp_file_path = tmp_file.name

        # Create session ID
        session_id = f"stmt_{uuid.uuid4().hex[:8]}"

        # Start background processing
        background_tasks.add_task(
            _process_bank_statement_background,
            session_id,
            tmp_file_path,
            file.filename,
        )

        return ProcessingResult(
            success=True, message="Processing started", statement_id=session_id
        )

    except Exception as e:
        logger.error(f"Error starting async processing: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/process/bank-statement-status/{session_id}")
async def get_bank_statement_status(session_id: str):
    """
    Get processing status for async processing
    """
    try:
        # Check Redis for status
        status_key = f"bank_statement:{session_id}:status"
        status_data = bank_processor.processor.redis_cache.redis_client.hgetall(
            status_key
        )

        if not status_data:
            raise HTTPException(status_code=404, detail="Session not found")

        return ProcessingStatusResponse(
            success=True,
            message=status_data.get("message", ""),
            statement_id=session_id,
            total_transactions=int(status_data.get("total_transactions", 0)),
            processed_transactions=int(
                status_data.get("processed_transactions", 0)
            ),
            processing_time=float(status_data.get("processing_time", 0))
            if status_data.get("processing_time")
            else None,
        )

    except Exception as e:
        logger.error(f"Error getting status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/process/bank-statement-download/{session_id}")
async def download_bank_statement_result(session_id: str):
    """
    Download processed result for async processing
    """
    try:
        # Check if result exists
        result_key = f"bank_statement:{session_id}:result"
        result_data = bank_processor.processor.redis_cache.redis_client.get(
            result_key
        )

        if not result_data:
            raise HTTPException(
                status_code=404, detail="Result not found or expired"
            )

        import base64

        excel_data = base64.b64decode(result_data)

        # Get original filename
        meta_key = f"bank_statement:{session_id}:meta"
        meta_data = bank_processor.processor.redis_cache.redis_client.hgetall(
            meta_key
        )
        original_filename = meta_data.get("filename", "statement")

        output_filename = f"saoke_{original_filename.split('.')[0]}.xlsx"

        return StreamingResponse(
            io.BytesIO(excel_data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={output_filename}"
            },
        )

    except Exception as e:
        logger.error(f"Error downloading result: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/process/bank-statement-cleanup/{session_id}")
async def cleanup_bank_statement_session(session_id: str):
    """
    Cleanup processing session data
    """
    try:
        # Clean up Redis keys
        keys_to_delete = [
            f"bank_statement:{session_id}:status",
            f"bank_statement:{session_id}:result",
            f"bank_statement:{session_id}:meta",
        ]

        bank_processor.processor.redis_cache.redis_client.delete(
            *keys_to_delete
        )

        return {"message": f"Session {session_id} cleaned up successfully"}

    except Exception as e:
        logger.error(f"Error cleaning up session: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


async def _process_bank_statement_stream(input_buffer: io.BytesIO) -> dict:
    """
    Process bank statement and return Excel data as bytes
    """
    start_time = time.time()

    try:
        # Process using the enhanced reader
        reader = BankStatementReader()
        df = reader.read_bank_statement(input_buffer, debug=False)

        if df.empty:
            return {
                "success": False,
                "error": "No transaction data found in bank statement",
            }

        # Convert to processed format (simplified)
        processed_data = []
        for _, row in df.iterrows():
            # Apply basic processing rules
            is_credit = row.get("credit", 0) > 0
            amount = row.get("credit", 0) if is_credit else row.get("debit", 0)

            processed_transaction = {
                "document_type": "BC" if is_credit else "BN",
                "date": row.get("date", "").strftime("%d/%m/%Y")
                if pd.notna(row.get("date"))
                else "",
                "description": str(row.get("description", "")),
                "amount": amount,
                "debit_account": "1121" if is_credit else "6278",
                "credit_account": "1311" if is_credit else "1121",
                "reference": str(row.get("reference", "")),
                "balance": row.get("balance", 0),
            }
            processed_data.append(processed_transaction)

        # Create Excel output
        output_df = pd.DataFrame(processed_data)

        # Generate Excel in memory
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
            output_df.to_excel(writer, sheet_name="Saoke", index=False)

            # Add summary sheet
            summary_data = {
                "Metric": [
                    "Total Transactions",
                    "Total Credit",
                    "Total Debit",
                    "Processing Time",
                ],
                "Value": [
                    len(output_df),
                    output_df[output_df["document_type"] == "BC"][
                        "amount"
                    ].sum(),
                    output_df[output_df["document_type"] == "BN"][
                        "amount"
                    ].sum(),
                    f"{time.time() - start_time:.2f}s",
                ],
            }
            pd.DataFrame(summary_data).to_excel(
                writer, sheet_name="Summary", index=False
            )

        excel_buffer.seek(0)

        return {
            "success": True,
            "excel_data": excel_buffer.getvalue(),
            "transactions": len(processed_data),
            "processing_time": time.time() - start_time,
        }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def _process_bank_statement_background(
    session_id: str, file_path: str, filename: str
):
    """
    Background task to process bank statement
    """
    import base64

    try:
        # Update status to processing
        status_key = f"bank_statement:{session_id}:status"
        bank_processor.processor.redis_cache.redis_client.hset(
            status_key,
            mapping={
                "message": "Processing",
                "start_time": datetime.now().isoformat(),
                "total_transactions": 0,
                "processed_transactions": 0,
            },
        )

        # Store metadata
        meta_key = f"bank_statement:{session_id}:meta"
        bank_processor.processor.redis_cache.redis_client.hset(
            meta_key, mapping={"filename": filename}
        )

        # Process the file
        with open(file_path, "rb") as f:
            input_buffer = io.BytesIO(f.read())

        result = await _process_bank_statement_stream(input_buffer)

        if result["success"]:
            # Store result in Redis (with expiry)
            result_key = f"bank_statement:{session_id}:result"
            excel_b64 = base64.b64encode(result["excel_data"]).decode("utf-8")
            bank_processor.processor.redis_cache.redis_client.setex(
                result_key,
                3600,  # 1 hour expiry
                excel_b64,
            )

            # Update final status
            bank_processor.processor.redis_cache.redis_client.hset(
                status_key,
                mapping={
                    "message": "Completed",
                    "end_time": datetime.now().isoformat(),
                    "total_transactions": result["transactions"],
                    "processed_transactions": result["transactions"],
                    "processing_time": result["processing_time"],
                },
            )

            logger.info(f"✅ Background processing completed for {filename}")
        else:
            # Update error status
            bank_processor.processor.redis_cache.redis_client.hset(
                status_key,
                mapping={
                    "message": f"Failed: {result['error']}",
                    "end_time": datetime.now().isoformat(),
                },
            )

            logger.error(
                f"❌ Background processing failed for {filename}: {result['error']}"
            )

        # Cleanup temporary file
        if os.path.exists(file_path):
            os.unlink(file_path)

    except Exception as e:
        # Update error status
        bank_processor.processor.redis_cache.redis_client.hset(
            status_key,
            mapping={
                "message": f"Error: {str(e)}",
                "end_time": datetime.now().isoformat(),
            },
        )

        logger.error(f"Background processing error: {str(e)}", exc_info=True)


# ============================================================================
# HEALTH CHECK AND INFO ENDPOINTS
# ============================================================================


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
            "bank_statement": "/process/bank-statement",
            "bank_statement_multiple": "/process/bank-statement-multiple",
            "bank_statement_async": "/process/bank-statement-async",
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

        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {
                "redis": redis_status,
                "database": "ok",  # Add database check if needed
            },
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat(),
        }
