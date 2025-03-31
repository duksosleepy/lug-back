import json
import logging
from collections import OrderedDict

import httpx

from .config_manager import sapo_config
from .utils import (
    convert_to_gmt7,
    get_adjusted_dates,
    get_sheets_service,
    update_sheet_with_retry,
)

logger = logging.getLogger(__name__)

# Lấy cấu hình từ config_manager thay vì hardcode
SPREADSHEET_ID = sapo_config.spreadsheet_id
RANGE_NAME = sapo_config.landing_site_range


async def fetch_orders(start_date, end_date):
    """Fetch orders from the Sapo API theo phân trang."""
    adjusted_start_date, adjusted_end_date = get_adjusted_dates(
        start_date, end_date
    )
    logger.info(
        f"Bắt đầu đồng bộ mysapo.net từ {adjusted_start_date} đến {adjusted_end_date}"
    )

    all_orders = []
    page = 1

    # Sử dụng base_url từ config_manager
    base_url = sapo_config.get_mysapo_net_base_url()

    while True:
        url = f"{base_url}/orders.json?page={page}&limit=250&created_on_min={adjusted_start_date}&created_on_max={adjusted_end_date}"
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                orders = data.get("orders", [])
                if not orders:  # Nếu không có order nào, đã hết dữ liệu
                    logger.info(f"Đã hết dữ liệu tại page {page}.")
                    break
                all_orders.extend(orders)
                logger.info(f"Page {page}: Lấy được {len(orders)} orders.")
                page += 1
        except httpx.RequestError as e:
            logger.error(f"Error fetching orders at page {page}: {e}")
            break
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response at page {page}: {e}")
            break
    # Trả về dữ liệu theo cấu trúc dictionary với key "orders"
    return {"orders": all_orders}


def process_orders(orders_data):
    """Process orders data according to the specified field mappings."""
    processed_orders = []
    orders = []
    if isinstance(orders_data, dict):
        if "orders" in orders_data:
            orders_list = orders_data.get("orders", [])
            orders = (
                orders_list if isinstance(orders_list, list) else [orders_list]
            )
        else:
            orders = [orders_data]
    elif isinstance(orders_data, list):
        orders = orders_data

    for order in orders:
        if not isinstance(order, dict):
            logger.warning(f"Warning: Order is not a dictionary: {order}")
            continue
        line_items = order.get("line_items", [])
        if not isinstance(line_items, list):
            line_items = [line_items] if line_items else []
        if not line_items:
            processed_order = extract_order_data(order, None)
            processed_orders.append(processed_order)
        else:
            for item in line_items:
                processed_order = extract_order_data(order, item)
                processed_orders.append(processed_order)
    return processed_orders


def extract_order_data(order, line_item):
    """Extract data from an order and line item according to the specified field mappings."""
    billing_address = order.get("billing_address", {}) or {}
    financial_status = "Chưa thanh toán"
    if order.get("financial_status") == "paid":
        financial_status = "Đã thanh toán"
    fulfillment_status = "Chưa chuyển"
    if order.get("fulfillment_status") == "fulfilled":
        fulfillment_status = "Chuyển toàn bộ"
    customer = order.get("customer", {}) or {}
    accepts_marketing = "Không"
    if customer and customer.get("accepts_marketing"):
        accepts_marketing = "Có"
    cancelled_on = order.get("cancelled_on", "")
    fulfillments = order.get("fulfillments", [])
    if fulfillments and isinstance(fulfillments, list):
        for fulfillment in fulfillments:
            if fulfillment.get("cancelled_on"):
                cancelled_on = fulfillment.get("cancelled_on")
                break

    item_fulfillment_status = "Chưa chuyển"
    if line_item and line_item.get("fulfillment_status") == "fulfilled":
        item_fulfillment_status = "Chuyển toàn bộ"
    requires_shipping = "Không"
    if line_item and line_item.get("requires_shipping"):
        requires_shipping = "Có"
    taxable = "Không"
    if line_item and line_item.get("taxable"):
        taxable = "Có"
    properties = ""
    if line_item:
        properties = line_item.get("properties", "")
        if properties is None:
            properties = ""
        elif isinstance(properties, (list, dict)):
            try:
                properties = json.dumps(properties, ensure_ascii=False)
            except:
                properties = str(properties)

    payment_method = order.get("gateway", "")
    if payment_method is None:
        payment_method = ""
    elif payment_method == "fundiin":
        payment_method = "Trả sau"
    elif payment_method == "cod":
        payment_method = "Thanh toán khi giao hàng (COD)"
    elif payment_method == "visa":
        payment_method = (
            "OnePay - Thanh toán online qua thẻ quốc tế (Visa/Master Card)"
        )
    elif payment_method == "vnpay":
        payment_method = "Thanh toán qua VNPAY"

    order_status = order.get("status", "")
    if order_status is None:
        order_status = ""
    elif order_status == "open":
        order_status = "Mở"
    elif order_status == "cancelled":
        order_status = "Hủy"

    note_attributes = order.get("note_attributes", "")
    if note_attributes is None:
        note_attributes = ""
    elif isinstance(note_attributes, (list, dict)):
        try:
            note_attributes = json.dumps(note_attributes, ensure_ascii=False)
        except:
            note_attributes = str(note_attributes)

    # Hàm an toàn để lấy giá trị với logic kiểm tra list rỗng
    def safe_get(obj, key, default=""):
        value = obj.get(key, default)
        if isinstance(value, list) and not value:
            return ""
        return default if value is None else value

    data = OrderedDict(
        [
            ("Id đơn hàng", safe_get(order, "id")),
            ("Mã đơn hàng", safe_get(order, "name")),
            ("Email", safe_get(order, "email")),
            ("Trạng thái thanh toán (Finacial Status)", financial_status),
            ("Trạng thái giao hàng (Fulfillment Status)", fulfillment_status),
            ("Nhận quảng cáo (Accepts Marketing)", accepts_marketing),
            ("Tiền tệ (Currency)", safe_get(order, "currency")),
            ("Tạm tính (Subtotal)", safe_get(order, "subtotal_price", 0) or 0),
            (
                "Phí ship (Shipping)",
                safe_get(order, "total_shipping_price", 0) or 0,
            ),
            ("Thuế (Taxes)", safe_get(order, "total_tax", 0) or 0),
            ("Tổng tiền (Total)", safe_get(order, "total_price", 0) or 0),
            (
                "Mã khuyến mãi (Discount Code)",
                safe_get(line_item, "discount_code") if line_item else "",
            ),
            (
                "Tiền khuyến mãi (Discount Amount)",
                safe_get(line_item, "total_discount", 0) or 0
                if line_item
                else 0,
            ),
            (
                "Tạo lúc (Created at)",
                convert_to_gmt7(safe_get(order, "created_on")),
            ),
            (
                "Số lượng sản phẩm (Lineitem quantity)",
                safe_get(line_item, "quantity") if line_item else "",
            ),
            (
                "Tên sản phẩm (Lineitem name)",
                safe_get(line_item, "name") if line_item else "",
            ),
            ("Thuộc tính (Properties)", properties),
            (
                "Giá sản phẩm (Lineitem price)",
                safe_get(line_item, "price") if line_item else "",
            ),
            (
                "Mã SKU (Lineitem sku)",
                safe_get(line_item, "sku") if line_item else "",
            ),
            (
                "SP yêu cầu vận chuyển (Lineitem requires shipping)",
                requires_shipping,
            ),
            ("Có thuế (Lineitem taxable)", taxable),
            (
                "Trạng thái giao hàng của sản phẩm (Lineitem fulfillment status)",
                item_fulfillment_status,
            ),
            (
                "Tên người thanh toán (Billing Name)",
                safe_get(billing_address, "name"),
            ),
            ("Địa chỉ thanh toán (Billing Street)", ""),
            ("Billing Address1", safe_get(billing_address, "address1")),
            ("Billing Address2", safe_get(billing_address, "address2")),
            (
                "Đơn vị thanh toán (Billing Company)",
                safe_get(billing_address, "company"),
            ),
            ("Thành phố (Billing City)", safe_get(billing_address, "city")),
            ("Billing Zip", safe_get(billing_address, "zip")),
            ("Tỉnh (Billing Province)", safe_get(billing_address, "province")),
            (
                "Quận huyện (Billing District)",
                safe_get(billing_address, "district"),
            ),
            ("Phường xã (Billing Ward)", safe_get(billing_address, "ward")),
            (
                "Quốc gia (Billing Country)",
                safe_get(billing_address, "country"),
            ),
            ("Billing Phone", safe_get(billing_address, "phone")),
            ("Ghi chú (Notes)", safe_get(order, "note")),
            ("Chú thích (Note Attributes)", note_attributes),
            (
                "Hủy đơn hàng lúc (Cancelled at)",
                convert_to_gmt7(cancelled_on) if cancelled_on else "",
            ),
            ("Phương thức thanh toán (Payment Method)", payment_method),
            (
                "Tiền hoàn trả (Refunded Amount)",
                safe_get(order, "total_refunded", 0) or 0,
            ),
            (
                "Nhà sản xuất (Vendor)",
                safe_get(line_item, "vendor") if line_item else "",
            ),
            ("Nguồn (Source)", safe_get(order, "source_name")),
            (
                "Giảm giá trên sản phẩm (Lineitem discount)",
                safe_get(line_item, "total_discount", 0) or 0
                if line_item
                else 0,
            ),
            (
                "Giá sản phẩm sau khuyến mại (discounted_unit_price)",
                safe_get(line_item, "discounted_unit_price")
                if line_item
                else "",
            ),
            ("Tags", safe_get(order, "tags")),
            ("Trạng thái đơn hàng", order_status),
            ("Landing Site", safe_get(order, "landing_site")),
            ("Referring Site", safe_get(order, "referring_site")),
        ]
    )
    return data


def update_google_sheet(processed_orders):
    """Chuyển đổi processed_orders thành mảng giá trị và cập nhật vào Google Sheet."""
    service = get_sheets_service()
    values = []
    for order in processed_orders:
        # Chuyển mỗi OrderedDict thành danh sách theo thứ tự các cột
        row = list(order.values())
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
    Đồng bộ dữ liệu từ mysapo.net API

    Args:
        start_date (str): Ngày bắt đầu định dạng YYYY-MM-DD
        end_date (str): Ngày kết thúc định dạng YYYY-MM-DD

    Returns:
        dict: Thông tin chi tiết về quá trình đồng bộ
    """
    try:
        # Kiểm tra thông tin xác thực trước khi thực hiện
        if not sapo_config.api_key or not sapo_config.secret_key:
            return {
                "status": "error",
                "message": "Thiếu thông tin xác thực API Sapo (SAPO_API_KEY hoặc SAPO_SECRET_KEY)",
                "orders_processed": 0,
            }

        # Lấy dữ liệu orders
        orders_data = await fetch_orders(start_date, end_date)
        if not orders_data or not orders_data.get("orders"):
            return {
                "status": "success",
                "message": "Không có dữ liệu đơn hàng.",
                "orders_processed": 0,
            }

        # Xử lý orders
        processed_orders = process_orders(orders_data)
        if not processed_orders:
            return {
                "status": "success",
                "message": "Không có dữ liệu đơn hàng xử lý được.",
                "orders_processed": 0,
            }

        # Cập nhật Google Sheet
        cells_updated = update_google_sheet(processed_orders)

        return {
            "status": "success",
            "message": "Đồng bộ dữ liệu thành công",
            "source": "mysapo.net",
            "orders_processed": len(processed_orders),
            "cells_updated": cells_updated,
        }
    except Exception as e:
        logger.error(
            f"Lỗi trong quá trình đồng bộ mysapo.net: {str(e)}", exc_info=True
        )
        return {
            "status": "error",
            "source": "mysapo.net",
            "message": str(e),
            "orders_processed": 0,
        }
