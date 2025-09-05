import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from util.logging import get_logger

logger = get_logger(__name__)


class EmailClient:
    def __init__(self, smtp_server, smtp_port, email_address, password):
        """
        Khởi tạo client email với thông tin xác thực

        Args:
            smtp_server (str): Địa chỉ SMTP server
            smtp_port (int): Cổng SMTP server
            email_address (str): Địa chỉ email người gửi
            password (str): Mật khẩu hoặc application password
        """
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.email_address = email_address
        self.password = password
        self.server = None

    def connect(self):
        """Kết nối đến SMTP server"""
        try:
            self.server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            self.server.starttls()  # Bảo mật kết nối bằng TLS
            self.server.login(self.email_address, self.password)
            logger.info(
                f"Đã kết nối thành công đến SMTP server {self.smtp_server}"
            )
            return self
        except Exception as e:
            logger.error(f"Lỗi kết nối đến SMTP server: {str(e)}")
            raise

    def disconnect(self):
        """Ngắt kết nối khỏi SMTP server"""
        if self.server:
            try:
                self.server.quit()
                logger.info("Đã ngắt kết nối SMTP server")
            except Exception as e:
                logger.warning(f"Lỗi khi ngắt kết nối SMTP server: {str(e)}")
            finally:
                self.server = None

    def create_message(self, to, subject, body, cc=None, bcc=None, html=False):
        """
        Tạo một email mới

        Args:
            to (str or list): Người nhận hoặc danh sách người nhận
            subject (str): Tiêu đề email
            body (str): Nội dung email
            cc (str or list, optional): CC hoặc danh sách CC
            bcc (str or list, optional): BCC hoặc danh sách BCC
            html (bool, optional): Nếu True, body sẽ được hiểu là HTML

        Returns:
            MIMEMultipart: Đối tượng email đã tạo
        """
        msg = MIMEMultipart()
        msg["From"] = self.email_address

        # Xử lý danh sách người nhận
        if isinstance(to, list):
            msg["To"] = ", ".join(to)
        else:
            msg["To"] = to

        msg["Subject"] = subject

        # Xử lý CC và BCC
        if cc:
            if isinstance(cc, list):
                msg["Cc"] = ", ".join(cc)
            else:
                msg["Cc"] = cc

        if bcc:
            if isinstance(bcc, list):
                msg["Bcc"] = ", ".join(bcc)
            else:
                msg["Bcc"] = bcc

        # Đính kèm body
        content_type = "html" if html else "plain"
        msg.attach(MIMEText(body, content_type))

        return msg

    def attach_file(self, msg, file_path):
        """
        Đính kèm file vào email

        Args:
            msg (MIMEMultipart): Đối tượng email
            file_path (str): Đường dẫn đến file cần đính kèm

        Returns:
            MIMEMultipart: Email với file đã đính kèm
        """
        try:
            with open(file_path, "rb") as file:
                attachment = MIMEApplication(
                    file.read(), Name=os.path.basename(file_path)
                )

            attachment["Content-Disposition"] = (
                f'attachment; filename="{os.path.basename(file_path)}"'
            )
            msg.attach(attachment)

            logger.info(f"Đã đính kèm file {file_path}")
            return msg
        except Exception as e:
            logger.error(f"Lỗi khi đính kèm file {file_path}: {str(e)}")
            raise

    def attach_files(self, msg, file_paths):
        """
        Đính kèm nhiều file vào email

        Args:
            msg (MIMEMultipart): Đối tượng email
            file_paths (list): Danh sách đường dẫn đến các file cần đính kèm

        Returns:
            MIMEMultipart: Email với các file đã đính kèm
        """
        for file_path in file_paths:
            if file_path and os.path.exists(file_path):
                msg = self.attach_file(msg, file_path)
        return msg

    def send(self, msg, to=None, cc=None, bcc=None):
        """
        Gửi email

        Args:
            msg (MIMEMultipart): Email cần gửi
            to (str or list, optional): Ghi đè người nhận từ msg
            cc (str or list, optional): Ghi đè CC từ msg
            bcc (str or list, optional): Ghi đè BCC từ msg

        Returns:
            bool: True nếu gửi thành công
        """
        if not self.server:
            raise ConnectionError(
                "Chưa kết nối đến SMTP server. Hãy gọi connect() trước."
            )

        # Lấy danh sách người nhận từ msg nếu không được ghi đè
        recipients = []
        if to:
            if isinstance(to, list):
                recipients.extend(to)
            else:
                recipients.append(to)
        elif "To" in msg:
            recipients.extend([addr.strip() for addr in msg["To"].split(",")])

        # Thêm CC
        cc_list = []
        if cc:
            if isinstance(cc, list):
                cc_list.extend(cc)
            else:
                cc_list.append(cc)
        elif "Cc" in msg:
            cc_list.extend([addr.strip() for addr in msg["Cc"].split(",")])
        recipients.extend(cc_list)

        # Thêm BCC
        bcc_list = []
        if bcc:
            if isinstance(bcc, list):
                bcc_list.extend(bcc)
            else:
                bcc_list.append(bcc)
        elif "Bcc" in msg:
            bcc_list.extend([addr.strip() for addr in msg["Bcc"].split(",")])
        recipients.extend(bcc_list)

        # Gửi email
        try:
            self.server.sendmail(
                self.email_address, recipients, msg.as_string()
            )
            logger.info(f"Đã gửi email thành công đến {', '.join(recipients)}")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi gửi email: {str(e)}")
            return False

    def __enter__(self):
        """Hỗ trợ sử dụng với context manager (with statement)"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Đảm bảo ngắt kết nối khi kết thúc context manager"""
        self.disconnect()
