import json
import re
from collections import OrderedDict

import httpx

from settings import sapo_settings
from util import (
    convert_to_gmt7,
    get_adjusted_dates,
    get_sheets_service,
    update_sheet_with_retry,
)
from util.logging import get_logger

logger = get_logger(__name__)

# Lấy cấu hình từ config_manager thay vì hardcode
SPREADSHEET_ID = sapo_settings.spreadsheet_id
RANGE_NAME = sapo_settings.data_range


def create_lookup_tables():
    "Create efficient lookup tables from inline static data instead of files."
    # Inline order sources data instead of loading from file
    order_sources_lookup = {
        8700401: "Recall",
        8700400: "Đổi hàng Online",
        8700399: "Đổi hàng Tiki",
        8700398: "Đổi hàng LZD",
        8700397: "Đổi hàng Shopee",
        8700396: "Bảo Hành",
        8650849: "Lug ID",
        8153380: "App",
        8088564: "Live Stream",
        7797684: "Hotline",
        6490836: "TiktokShop",
        4922568: "GrabMart",
        4563501: "WebOrder",
        4390031: "Instagram",
        2064035: "Sendo",
        850822: "Khác",
        850821: "Shopee",
        850820: "Pos",
        850819: "Tiki",
        850818: "Lazada",
        850817: "Zalo",
        850816: "Facebook",
        850815: "Web",
    }

    # Inline accounts data instead of loading from file
    accounts_lookup = {
        1227257: "Khôi IT LUG",
        1226958: "Dương Duyên",
        1226404: "Phương Nhi",
        1222086: "null Nguyễn Anh",
        1214227: "Trường Chinh",
        1213641: "Duyên",
        1212709: "Trúc Tuyền",
        1209686: "Ngọc Yến",
        1208310: "null Hương",
        1199950: "Phúc Kho",
        1194871: "Thúy Hoa",
        1184440: "Đăng Kho",
        1182031: "Sơn tiktok",
        1181637: "Dũng",
        1179953: "Kế Toán",
        1179761: "Kiệt",
        1179364: "Chuyên",
        1179204: "Nguyễn Phương",
        1171594: "Trinh",
        1080747: "CHT KVC",
        1071288: "Chị Lam",
        1056112: "Trúc Tuyền",
        1035703: "Kim Liễu",
        1030008: "Khánh_KVC",
        1027483: "Lê Dũng",
        1026890: "Hoài Trinh",
        1025361: "Nam KVC",
        1010245: "Thảo Như",
        914229: "Ngọc Bích",
        910896: "Ánh Tuyết",
        797625: "Thảo",
        429880: "Nhựt Đăng",
        394673: "Hoàng Long (Call)",
        146761: "LUG",
    }

    return order_sources_lookup, accounts_lookup


def extract_branch(detail_json):
    """Extract branch name (Chi nhánh) from shipment detail JSON string."""
    if not detail_json:
        return ""

    try:
        detail = json.loads(detail_json)
        sender_name = detail.get("sender_name", "")
        sender_full_name = detail.get("sender_full_name", "")

        if sender_name and "Kho" in sender_name:
            return sender_name

        if sender_full_name:
            match = re.search(r"\((Kho[^)]*)\)", sender_full_name) or re.search(
                r"(Kho[^,)]*)", sender_full_name
            )
            if match:
                return match.group(1)

        sender_address = detail.get("sender_address", "")
        if sender_address:
            match = re.search(r"\((Kho[^)]*)\)", sender_address) or re.search(
                r"(Kho[^,)]*)", sender_address
            )
            if match:
                return match.group(1)

        return sender_name or sender_full_name or sender_address

    except (json.JSONDecodeError, TypeError):
        return ""


async def fetch_and_process_orders(start_date, end_date):
    """Fetch orders from the API and process them according to requirements."""
    adjusted_start_date, adjusted_end_date = get_adjusted_dates(
        start_date, end_date, format="iso"
    )
    logger.info(
        f"Bắt đầu đồng bộ mysapogo.com từ {adjusted_start_date} đến {adjusted_end_date}"
    )

    order_sources_lookup, accounts_lookup = create_lookup_tables()
    status_mapping = {
        "draft": "Đặt hàng",
        "finalized": "Đang giao dịch",
        "completed": "Hoàn thành",
        "cancelled": "Đã hủy",
        "finished": "Kết thúc",
    }

    base_url = "https://congtysangtam.mysapogo.com/admin/orders.json"
    params = {
        "created_on_max": adjusted_end_date,
        "created_on_min": adjusted_start_date,
        "limit": 250,
    }

    # Lấy headers từ config_manager
    headers = sapo_settings.get_mysapogo_com_headers()

    all_processed_orders = []
    current_page = 1
    total_pages = None

    async with httpx.AsyncClient() as client:
        while total_pages is None or current_page <= total_pages:
            params["page"] = current_page
            try:
                response = await client.get(
                    base_url, params=params, headers=headers
                )
                response.raise_for_status()
                order_data = response.json()
                if "orders" not in order_data:
                    logger.warning(
                        f"Warning: 'orders' key missing in response for page {current_page}"
                    )
                    order_data["orders"] = []
            except httpx.RequestError as e:
                logger.error(f"Error fetching page {current_page}: {e}")
                break
            except json.JSONDecodeError:
                logger.error(
                    f"Error parsing JSON response for page {current_page}"
                )
                break

            if total_pages is None and "metadata" in order_data:
                metadata = order_data.get("metadata", {})
                total_items = metadata.get("total", 0)
                items_per_page = metadata.get("limit", 250)
                total_pages = (
                    (total_items + items_per_page - 1) // items_per_page
                    if items_per_page > 0
                    else 1
                )

            processed_orders = process_page_data(
                order_data,
                order_sources_lookup,
                accounts_lookup,
                status_mapping,
            )
            all_processed_orders.extend(processed_orders)
            logger.info(
                f"Processed page {current_page} of {total_pages if total_pages is not None else 'unknown'}"
            )
            current_page += 1

    return all_processed_orders


def process_page_data(
    order_data, order_sources_lookup, accounts_lookup, status_mapping
):
    """Process a single page of order data."""
    result = []
    orders = order_data.get("orders", []) or []

    for order in orders:
        if not order:
            continue

        chi_nhanh = ""
        for fulfillment in order.get("fulfillments", []) or []:
            if not fulfillment:
                continue
            shipment = fulfillment.get("shipment")
            if shipment and shipment.get("detail"):
                extracted_branch = extract_branch(shipment.get("detail"))
                if extracted_branch:
                    chi_nhanh = extracted_branch
                    break

        # Phần này vẫn giữ lại để tính total_sales cho trường hợp không có line_items
        total_sales = 0
        customer_data = order.get("customer_data")
        if customer_data and isinstance(customer_data, dict):
            sale_order = customer_data.get("sale_order")
            if sale_order and isinstance(sale_order, dict):
                sales_value = sale_order.get("total_sales")
                if sales_value is not None:
                    total_sales = int(round(float(sales_value)))

        line_items = order.get("order_line_items", []) or []
        if not line_items:
            item_info = OrderedDict(
                [
                    ("Mã ĐH", order.get("code", "")),
                    (
                        "Ngày chứng từ",
                        convert_to_gmt7(order.get("created_on", "")),
                    ),
                    ("Chi nhánh", chi_nhanh),
                    (
                        "Nguồn bán hàng",
                        order_sources_lookup.get(order.get("source_id"), ""),
                    ),
                    (
                        "Nhân viên tạo đơn",
                        accounts_lookup.get(order.get("account_id"), ""),
                    ),
                    (
                        "Trạng thái đơn hàng",
                        status_mapping.get(
                            order.get("status", ""), order.get("status", "")
                        ),
                    ),
                    (
                        "Tên khách hàng",
                        (order.get("customer_data") or {}).get("name", ""),
                    ),
                    ("Mã sản phẩm", ""),
                    ("Tên sản phẩm", ""),
                    # THAY ĐỔI: Sử dụng chuỗi rỗng "" thay vì total_sales
                    ("Tổng tiền hàng", ""),
                    ("Ghi chú đơn", order.get("note", "")),
                    (
                        "Điện thoại KH",
                        order.get("phone_number", "").lstrip("0")
                        if order.get("phone_number")
                        else "",
                    ),
                    ("Số lượng", 0),
                    ("Lý do hủy đơn", order.get("reason_cancel_id", "")),
                    (
                        "Ngày hủy đơn",
                        convert_to_gmt7(order.get("cancelled_on", "")),
                    ),
                ]
            )
            result.append(item_info)
        else:
            for line_item in line_items:
                if not line_item:
                    continue
                item_info = OrderedDict(
                    [
                        ("Mã ĐH", order.get("code", "")),
                        (
                            "Ngày chứng từ",
                            convert_to_gmt7(order.get("created_on", "")),
                        ),
                        ("Chi nhánh", chi_nhanh),
                        (
                            "Nguồn bán hàng",
                            order_sources_lookup.get(
                                order.get("source_id"), ""
                            ),
                        ),
                        (
                            "Nhân viên tạo đơn",
                            accounts_lookup.get(order.get("account_id"), ""),
                        ),
                        (
                            "Trạng thái đơn hàng",
                            status_mapping.get(
                                order.get("status", ""), order.get("status", "")
                            ),
                        ),
                        (
                            "Tên khách hàng",
                            (order.get("customer_data") or {}).get("name", ""),
                        ),
                        ("Mã sản phẩm", line_item.get("sku", "")),
                        ("Tên sản phẩm", line_item.get("product_name", "")),
                        # THAY ĐỔI: Sử dụng line_amount
                        ("Tổng tiền hàng", line_item.get("line_amount", "")),
                        ("Ghi chú đơn", order.get("note", "")),
                        (
                            "Điện thoại KH",
                            order.get("phone_number", "").lstrip("0")
                            if order.get("phone_number")
                            else "",
                        ),
                        ("Số lượng", line_item.get("quantity", 0)),
                        ("Lý do hủy đơn", order.get("reason_cancel_id", "")),
                        (
                            "Ngày hủy đơn",
                            convert_to_gmt7(order.get("cancelled_on", "")),
                        ),
                    ]
                )
                result.append(item_info)

    return result


def update_google_sheet(processed_orders):
    """
    Chuyển đổi processed_orders thành mảng giá trị và cập nhật vào Google Sheet.
    """
    if not processed_orders:
        logger.warning("No processed orders to update to Google Sheet.")
        return 0

    service = get_sheets_service()
    # Chuyển từng OrderedDict thành list theo thứ tự các cột
    values = []
    for item in processed_orders:
        row = list(item.values())
        values.append(row)

    # Tạo body cho batchUpdate
    body = {
        "valueInputOption": "USER_ENTERED",
        "data": [{"range": RANGE_NAME, "values": values}],
    }

    result = update_sheet_with_retry(service, SPREADSHEET_ID, body)
    logger.info(
        f"{result.get('totalUpdatedCells')} cells updated in Google Sheet."
    )
    return result.get("totalUpdatedCells", 0)


async def sync(start_date: str, end_date: str) -> dict:
    """
    Đồng bộ dữ liệu từ mysapogo.com API

    Args:
        start_date (str): Ngày bắt đầu định dạng YYYY-MM-DD
        end_date (str): Ngày kết thúc định dạng YYYY-MM-DD

    Returns:
        dict: Thông tin chi tiết về quá trình đồng bộ
    """
    try:
        # Kiểm tra thông tin xác thực trước khi thực hiện
        if not sapo_settings.access_token:
            return {
                "status": "error",
                "message": "Thiếu thông tin xác thực API Sapo (SAPO_ACCESS_TOKEN)",
                "orders_processed": 0,
            }

        processed_orders = await fetch_and_process_orders(start_date, end_date)

        if not processed_orders:
            return {
                "status": "success",
                "message": "Không có dữ liệu đơn hàng để xử lý.",
                "orders_processed": 0,
            }

        # Cập nhật Google Sheet
        cells_updated = update_google_sheet(processed_orders)

        return {
            "status": "success",
            "message": "Đồng bộ dữ liệu thành công",
            "source": "mysapogo.com",
            "orders_processed": len(processed_orders),
            "cells_updated": cells_updated,
        }
    except Exception as e:
        logger.error(
            f"Lỗi trong quá trình đồng bộ mysapogo.com: {str(e)}", exc_info=True
        )
        return {
            "status": "error",
            "source": "mysapogo.com",
            "message": str(e),
            "orders_processed": 0,
        }
