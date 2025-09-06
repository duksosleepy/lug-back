"""
Module tasks.worker.py - Quản lý các tác vụ nền cho đồng bộ dữ liệu đăng ký bảo hành.
"""

import asyncio

import httpx
from celery import Celery
from celery.schedules import crontab
from loguru import logger

# Move imports to module level
from src.sapo_sync import SapoSyncRequest, sync_mysapo, sync_mysapogo
from src.settings import app_settings
from src.util.phone_utils import format_phone_number

# === PHẦN 1: KHỞI TẠO CELERY APP ===

# Lấy Redis URL từ biến môi trường hoặc sử dụng giá trị mặc định
REDIS_URL = app_settings.get_env("REDIS_URL", "redis://localhost:6379")

# Khởi tạo ứng dụng Celery
celery_app = Celery(
    app_settings.app_name,
    broker=f"{REDIS_URL}/0",
    backend=f"{REDIS_URL}/1",
    include=["src.tasks.worker", "src.crm.tasks"],
)

# Cấu hình Celery
celery_app.conf.update(
    # Cấu hình cho các task định kỳ
    beat_schedule={
        "sync-pending-registrations": {
            "task": "src.tasks.worker.sync_pending_registrations",
            "schedule": 3600.0,  # Chạy mỗi giờ (3600s)
            "args": (),
        },
        "daily-sapo-sync": {
            "task": "src.tasks.worker.daily_sapo_sync",
            "schedule": crontab(hour=0, minute=30),
            "args": (),
        },
        "crm-data-sync": {
            "task": "src.crm.tasks.sync_crm_data",
            "schedule": crontab(hour=10, minute=0),  # Run at 5:00 AM daily
            "args": (),
        },
    },
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    enable_utc=True,
    timezone="Asia/Ho_Chi_Minh",
    worker_concurrency=4,
    task_time_limit=1800,  # 30 minutes
    task_soft_time_limit=1500,  # 25 minutes
    task_routes={
        "src.tasks.worker.*": {"queue": "celery"},
    },
)


# === PHẦN 2: CÁC HÀM HỖ TRỢ ===


def get_headers():
    """Tạo headers cho API requests."""
    return {
        "accept": "application/json",
        "content-type": "application/json",
        "xc-token": app_settings.xc_token,
    }


def fetch_pending_registrations(client):
    """Lấy tất cả đăng ký đang chờ xử lý từ bảng tạm."""
    pending_url = f"{app_settings.api_endpoint}/tables/mydoap8edbr206g/records"
    pending_response = client.get(pending_url, headers=get_headers())

    if pending_response.status_code != 200:
        logger.error(
            f"Không thể lấy dữ liệu từ bảng tạm: {pending_response.status_code} - {pending_response.text}"
        )
        return None

    pending_data = pending_response.json()
    pending_records = pending_data.get("list", [])

    return pending_records


def check_order_exists(client, order_code):
    """Kiểm tra xem mã đơn hàng đã có trong bảng chính chưa."""
    search_url = f"{app_settings.api_endpoint}/tables/mtvvlryi3xc0gqd/records?where=(ma_don_hang%2Ceq%2C{order_code})&limit=1000"
    search_response = client.get(search_url, headers=get_headers())

    if search_response.status_code != 200:
        logger.error(
            f"Lỗi khi tìm kiếm đơn hàng: {search_response.status_code} - {search_response.text}"
        )
        return None

    search_data = search_response.json()
    order_records = search_data.get("list", [])

    return order_records


def prepare_records_to_copy(
    order_records, customer_name, formatted_phone, purchase_platform
):
    """Chuẩn bị các bản ghi để sao chép từ bảng chính sang bảng đích."""
    records_to_copy = []
    record_ids = []

    for order_record in order_records:
        # Lưu ID để xóa sau này
        record_ids.append({"Id": order_record["Id"]})

        # Tạo bản ghi mới với thông tin cập nhật từ form
        new_record = {
            "ngay_ct": order_record.get("ngay_ct", ""),
            "ma_ct": order_record.get("ma_ct", ""),
            "so_ct": order_record.get("so_ct", ""),
            "ma_bo_phan": order_record.get("ma_bo_phan", ""),
            "ma_don_hang": order_record.get("ma_don_hang", ""),
            "ten_khach_hang": customer_name,  # Cập nhật tên từ form
            "so_dien_thoai": formatted_phone,  # Cập nhật số điện thoại đã format
            "tinh_thanh": order_record.get("tinh_thanh", ""),
            "quan_huyen": order_record.get("quan_huyen", ""),
            "phuong_xa": order_record.get("phuong_xa", ""),
            "dia_chi": order_record.get("dia_chi", ""),
            "ma_hang": order_record.get("ma_hang", ""),
            "ten_hang": order_record.get("ten_hang", ""),
            "imei": order_record.get("imei", ""),
            "so_luong": order_record.get("so_luong", ""),
            "doanh_thu": order_record.get("doanh_thu", ""),
            "ghi_chu": f"Đăng ký bảo hành qua {purchase_platform} (từ hàng đợi)",
        }
        records_to_copy.append(new_record)

    return records_to_copy, record_ids


def copy_records_to_warranty(client, records_to_copy):
    """Sao chép bản ghi sang bảng bảo hành."""
    copy_url = f"{app_settings.api_endpoint}/tables/mffwo1asni22n9z/records"

    copy_response = client.post(
        copy_url, headers=get_headers(), json=records_to_copy
    )

    if copy_response.status_code not in (200, 201):
        logger.error(
            f"Lỗi khi sao chép bản ghi: {copy_response.status_code} - {copy_response.text}"
        )
        return False

    return True


def delete_original_records(client, record_ids):
    """Xóa bản ghi gốc từ bảng chính."""
    delete_url = f"{app_settings.api_endpoint}/tables/mtvvlryi3xc0gqd/records"

    delete_response = client.request(
        method="DELETE",
        url=delete_url,
        headers=get_headers(),
        json=record_ids,
    )

    if delete_response.status_code != 200:
        logger.warning(
            f"Lỗi khi xóa bản ghi gốc: {delete_response.status_code} - {delete_response.text}"
        )
        return False

    return True


def delete_pending_record(client, record_id):
    """Xóa bản ghi từ bảng tạm."""
    delete_temp_url = f"{app_settings.api_endpoint}/tables/mydoap8edbr206g/records/{record_id}"

    delete_temp_response = client.request(
        method="DELETE",
        url=delete_temp_url,
        headers=get_headers(),
    )

    if delete_temp_response.status_code != 200:
        logger.warning(
            f"Lỗi khi xóa bản ghi tạm: {delete_temp_response.status_code} - {delete_temp_response.text}"
        )
        return False

    return True


def add_to_warranty_tracking(
    client, customer_name, formatted_phone, purchase_platform, order_code
):
    """Thêm vào bảng theo dõi bảo hành."""
    registration_url = (
        f"{app_settings.api_endpoint}/tables/miyw4f4yeojamv6/records"
    )
    registration_data = {
        "ho_ten": customer_name,
        "so_dien_thoai": formatted_phone,
        "noi_mua": purchase_platform,
        "ma_don_hang": order_code,
    }

    registration_response = client.post(
        registration_url, headers=get_headers(), json=registration_data
    )

    if registration_response.status_code not in (200, 201):
        logger.warning(
            f"Lỗi khi lưu thông tin đăng ký: {registration_response.status_code}"
        )
        return False

    return True


# === PHẦN 3: HELPER FUNCTION FOR ASYNC SYNC ===


def run_async_sync(start_date: str, end_date: str) -> dict:
    """
    Helper function to run async sync functions in a synchronous context.
    This function creates a new event loop to run the async tasks.

    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format

    Returns:
        dict: Results from both sync operations
    """

    async def _run_sync():
        """Internal async function to run both sync operations."""
        try:
            # Create request data
            request_data = SapoSyncRequest(
                startDate=start_date, endDate=end_date
            )

            # Run both sync operations concurrently
            mysapo_task = sync_mysapo(
                request_data.startDate, request_data.endDate
            )
            mysapogo_task = sync_mysapogo(
                request_data.startDate, request_data.endDate
            )

            # Wait for both tasks to complete
            mysapo_result, mysapogo_result = await asyncio.gather(
                mysapo_task, mysapogo_task
            )

            return mysapo_result, mysapogo_result

        except Exception as e:
            logger.error(
                f"Error in async sync operations: {str(e)}", exc_info=True
            )
            raise

    # Check if there's already an event loop running
    try:
        loop = asyncio.get_running_loop()
        # If we're in an async context, we need to use a different approach
        logger.warning(
            "Already in async context, using asyncio.run_coroutine_threadsafe"
        )
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(asyncio.run, _run_sync())
            return future.result(timeout=1800)  # 30 minute timeout
    except RuntimeError:
        # No event loop running, safe to use asyncio.run
        return asyncio.run(_run_sync())


# === PHẦN 4: TASK ĐỒNG BỘ CHÍNH ===


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def daily_sapo_sync(self):
    """
    Task chạy hàng ngày vào 00:30 AM để đồng bộ dữ liệu từ Sapo APIs.
    Đồng bộ từ ngày 31/12 của năm trước đến ngày hiện tại.
    """
    from datetime import datetime

    try:
        # Lấy ngày 31/12 của năm trước và ngày hiện tại để đồng bộ (format: YYYY-MM-DD)
        today = datetime.now()
        start_date = f"{today.year - 1}-12-31"
        end_date = today.strftime("%Y-%m-%d")

        logger.info(
            f"Bắt đầu đồng bộ Sapo hàng ngày từ {start_date} đến {end_date}"
        )

        mysapo_result, mysapogo_result = run_async_sync(start_date, end_date)

        total_orders_processed = mysapo_result.get(
            "orders_processed", 0
        ) + mysapogo_result.get("orders_processed", 0)

        logger.info(
            f"Đồng bộ Sapo hàng ngày hoàn tất. Tổng số đơn hàng đã xử lý: {total_orders_processed}"
        )

        return {
            "success": True,
            "message": "Đồng bộ Sapo hàng ngày hoàn tất",
            "mysapo_net": mysapo_result,
            "mysapogo_com": mysapogo_result,
            "total_orders_processed": total_orders_processed,
        }
    except Exception as e:
        logger.error(
            f"Lỗi trong quá trình đồng bộ Sapo hàng ngày: {str(e)}",
            exc_info=True,
        )
        # Retry task nếu xảy ra lỗi
        self.retry(exc=e)
        return {
            "success": False,
            "message": f"Lỗi khi đồng bộ Sapo hàng ngày: {str(e)}",
        }


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def sync_pending_registrations(self):
    """
    Task đồng bộ giữa bảng đăng ký tạm (mydoap8edbr206g) và bảng chính (mtvvlryi3xc0gqd).

    Quy trình:
    1. Lấy tất cả bản ghi từ bảng tạm
    2. Với mỗi bản ghi, kiểm tra xem mã đơn hàng đã có trong bảng chính chưa
    3. Nếu có, chuyển thông tin sang bảng bảo hành và xóa bản ghi tạm
    """
    logger.info("Bắt đầu đồng bộ đăng ký bảo hành chờ xử lý")

    try:
        # Sử dụng httpx để gửi request đồng bộ trong task
        with httpx.Client(timeout=30.0) as client:
            # 1. Lấy tất cả đăng ký chờ xử lý
            pending_records = fetch_pending_registrations(client)

            if pending_records is None:
                return {
                    "success": False,
                    "message": "Không thể lấy dữ liệu từ bảng tạm",
                }

            if not pending_records:
                logger.info("Không có đăng ký chờ xử lý")
                return {
                    "success": True,
                    "processed": 0,
                    "message": "Không có đăng ký chờ xử lý",
                }

            logger.info(f"Tìm thấy {len(pending_records)} đăng ký chờ xử lý")
            processed_count = 0

            # 2. Xử lý từng bản ghi
            for record in pending_records:
                # Lấy thông tin chính
                customer_name = record.get("ho_ten", "")
                phone_number = record.get("so_dien_thoai", "")
                order_code = record.get("ma_don_hang", "")
                purchase_platform = record.get("noi_mua", "website")
                record_id = record.get("Id")

                if not order_code:
                    logger.warning(
                        f"Bỏ qua bản ghi không có mã đơn hàng, ID: {record_id}"
                    )
                    continue

                # Format số điện thoại
                formatted_phone = (
                    format_phone_number(phone_number) or phone_number
                )

                # 3. Kiểm tra xem đơn hàng đã có trong bảng chính chưa
                order_records = check_order_exists(client, order_code)

                if order_records is None:
                    continue

                if not order_records:
                    # Đơn hàng vẫn chưa có trong hệ thống
                    logger.info(
                        f"Mã đơn hàng {order_code} vẫn chưa có trong hệ thống"
                    )
                    continue

                # 4. Đơn hàng đã có, tiến hành xử lý
                logger.info(
                    f"Đã tìm thấy đơn hàng {order_code} với {len(order_records)} sản phẩm"
                )

                # Chuẩn bị dữ liệu để sao chép
                records_to_copy, record_ids = prepare_records_to_copy(
                    order_records,
                    customer_name,
                    formatted_phone,
                    purchase_platform,
                )

                try:
                    # 5. Sao chép dữ liệu sang bảng đích
                    if not copy_records_to_warranty(client, records_to_copy):
                        continue

                    # 6. Xóa bản ghi gốc từ bảng chính
                    delete_original_records(client, record_ids)

                    # 7. Xóa bản ghi từ bảng tạm
                    delete_pending_record(client, record_id)

                    # 8. Lưu thông tin vào bảng theo dõi bảo hành
                    add_to_warranty_tracking(
                        client,
                        customer_name,
                        formatted_phone,
                        purchase_platform,
                        order_code,
                    )

                    processed_count += 1
                    logger.info(f"Đã xử lý thành công đơn hàng {order_code}")

                except Exception as e:
                    logger.error(
                        f"Lỗi khi xử lý đơn hàng {order_code}: {str(e)}"
                    )

            return {
                "success": True,
                "processed": processed_count,
                "message": f"Đã xử lý {processed_count}/{len(pending_records)} đơn hàng",
            }

    except Exception as e:
        logger.error(f"Lỗi trong quá trình đồng bộ: {str(e)}", exc_info=True)
        # Retry task nếu xảy ra lỗi
        self.retry(exc=e)
        return {"success": False, "message": f"Lỗi khi đồng bộ: {str(e)}"}
