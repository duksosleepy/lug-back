"""
Tiện ích bổ sung cho hệ thống logging.
"""

from loguru import logger


def get_logger(name=None):
    """
    Tạo và trả về một logger đã được cấu hình, với module name được thiết lập.

    Args:
        name: Tên của module, thường là __name__

    Returns:
        logger: Đối tượng logger đã được cấu hình
    """
    if name:
        return logger.bind(module=name)
    return logger


def log_request_info(logger, request):
    """
    Log thông tin chi tiết về request, an toàn với mọi loại content-type.

    Args:
        logger: Logger instance
        request: FastAPI Request object
    """
    try:
        # Log thông tin cơ bản
        log_data = {
            "method": request.method,
            "url": str(request.url),
            "client": request.client.host if request.client else "unknown",
        }

        logger.debug(f"Request details: {log_data}")
    except Exception as e:
        logger.warning(f"Error logging request info: {str(e)}")


def log_error_with_context(logger, error, context=None):
    """
    Log lỗi với bối cảnh bổ sung và format nhất quán.

    Args:
        logger: Logger instance
        error: Exception object
        context: Dict với thông tin bổ sung
    """
    error_info = {
        "error_type": type(error).__name__,
        "error_message": str(error),
        "context": context or {},
    }

    logger.exception(f"Error details: {error_info}")
