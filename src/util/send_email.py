import configparser
import logging
import os

from .mail_client import EmailClient

logger = logging.getLogger(__name__)


def load_config():
    """
    Tải cấu hình email từ file hoặc biến môi trường

    Returns:
        dict: Thông tin cấu hình email
    """
    # Ưu tiên đọc từ biến môi trường
    smtp_server = os.environ.get("SMTP_SERVER")
    smtp_port = os.environ.get("SMTP_PORT")
    email_address = os.environ.get("EMAIL_ADDRESS")
    password = os.environ.get("EMAIL_PASSWORD")
    # Nếu không có trong biến môi trường, thử đọc từ file cấu hình
    if not all([smtp_server, smtp_port, email_address, password]):
        try:
            config = configparser.ConfigParser()
            config_path = os.environ.get(
                "EMAIL_CONFIG_PATH", "/etc/lug-back/email_config.ini"
            )

            if os.path.exists(config_path):
                config.read(config_path)

                smtp_server = smtp_server or config.get(
                    "EMAIL", "SMTP_SERVER", fallback=None
                )
                smtp_port = smtp_port or config.get(
                    "EMAIL", "SMTP_PORT", fallback=None
                )
                email_address = email_address or config.get(
                    "EMAIL", "EMAIL_ADDRESS", fallback=None
                )
                password = password or config.get(
                    "EMAIL", "PASSWORD", fallback=None
                )
            else:
                logger.warning(f"File cấu hình {config_path} không tồn tại")
        except (configparser.Error, FileNotFoundError) as e:
            logger.error(f"Không thể đọc file cấu hình: {str(e)}")

    # Kiểm tra xem đã có đủ thông tin cấu hình chưa
    if not all([smtp_server, smtp_port, email_address, password]):
        raise ValueError(
            "Thiếu thông tin cấu hình email. Vui lòng kiểm tra lại biến môi trường hoặc file cấu hình."
        )

    return {
        "smtp_server": smtp_server,
        "smtp_port": int(smtp_port),
        "email_address": email_address,
        "password": password,
    }


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
