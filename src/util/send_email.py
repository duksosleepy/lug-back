import os

from util.logging import get_logger

from .mail_client import EmailClient

logger = get_logger(__name__)


def load_config():
    """
    Tải cấu hình email từ file hoặc biến môi trường

    Returns:
        dict: Thông tin cấu hình email
    """
    # Lazy import to avoid circular dependency
    from settings import email_settings

    # Ưu tiên đọc từ biến môi trường
    return email_settings.get_config_dict()


def send_notification_email(
    to, subject, body, attachment_path=None, html=False
):
    """
    Gửi email thông báo

    Args:
        to (str or list): Người nhận
        subject (str): Tiêu đề
        body (str): Nội dung
        attachment_path (str, optional): Đường dẫn đến file đính kèm
        html (bool, optional): Nếu True, body sẽ được hiểu là HTML

    Returns:
        bool: True nếu gửi thành công
    """
    try:
        config = load_config()

        # Sử dụng context manager để tự động connect và disconnect
        with EmailClient(**config) as client:
            # Tạo email
            msg = client.create_message(to, subject, body, html=html)

            # Đính kèm file nếu có
            if attachment_path and os.path.exists(attachment_path):
                msg = client.attach_file(msg, attachment_path)

            # Gửi email
            result = client.send(msg)

            if result:
                logger.info(f"Đã gửi email thành công đến {to}")
            else:
                logger.warning(f"Không thể gửi email đến {to}")

            return result

    except Exception as e:
        logger.error(f"Có lỗi xảy ra khi gửi email: {str(e)}")
        return False
