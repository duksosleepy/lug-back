"""
Tiện ích cho HTTP requests với port fallback mechanism.
"""

import json
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse, urlunparse

import httpx

from src.util.logging import get_logger

logger = get_logger(__name__)


def _replace_port(url: str, port: int) -> str:
    """
    Replace the port in a URL with a new port.

    Args:
        url: The original URL
        port: The new port number

    Returns:
        str: URL with the new port
    """
    parsed = urlparse(url)
    # Replace the netloc (host:port) with new port
    host_only = parsed.hostname or "localhost"
    new_netloc = f"{host_only}:{port}"
    return urlunparse((parsed.scheme, new_netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))


async def post_json_data_with_port_fallback(
    endpoint: str,
    data: Union[Dict[str, Any], List[Dict[str, Any]]],
    token: Optional[str] = None,
    timeout: float = 30.0,
    fallback_ports: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """
    Gửi dữ liệu JSON đến API endpoint với cơ chế fallback port.

    Nếu yêu cầu thất bại với cổng hiện tại, sẽ thử các cổng fallback khác.
    Rất hữu ích cho các dịch vụ chạy trong cụm (cluster) nơi cổng có thể thay đổi.

    Args:
        endpoint: URL endpoint của API
        data: Dữ liệu JSON cần gửi
        token: Token xác thực (mặc định lấy từ settings)
        timeout: Thời gian timeout tính bằng giây
        fallback_ports: Danh sách cổng fallback để thử [8080, 28080, 18080]

    Returns:
        Dict[str, Any]: Kết quả trả về từ API
    """
    # Lazy import to avoid circular dependency
    if token is None:
        from src.settings import app_settings
        token = app_settings.xc_token

    if not endpoint.startswith("http"):
        # Nếu endpoint không có scheme, thêm base URL từ settings
        from src.settings import app_settings
        full_url = f"{app_settings.api_endpoint}/{endpoint.lstrip('/')}"
    else:
        full_url = endpoint

    headers = {"Content-Type": "application/json", "xc-token": token}

    # Default fallback ports from settings or parameter
    if fallback_ports is None:
        from src.settings import app_settings
        fallback_ports = app_settings.get_crm_batch_fallback_ports()

    # Extract the primary port from the URL
    parsed_url = urlparse(full_url)
    primary_port = parsed_url.port
    ports_to_try = [primary_port] + fallback_ports if primary_port else fallback_ports

    last_error = None

    for i, port in enumerate(ports_to_try):
        try:
            url_with_port = _replace_port(full_url, port)
            logger.info(f"Attempting POST to {url_with_port} (attempt {i + 1}/{len(ports_to_try)})")

            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(url_with_port, json=data, headers=headers)
                response.raise_for_status()

                logger.info(f"POST thành công đến {url_with_port}")
                try:
                    return response.json()
                except json.JSONDecodeError:
                    logger.warning(
                        f"Không thể parse JSON từ response. Status code: {response.status_code}"
                    )
                    return {"status": "success", "text": response.text}

        except (httpx.HTTPError, httpx.ConnectError, httpx.TimeoutException) as e:
            last_error = e
            logger.warning(
                f"Request failed to port {port}: {type(e).__name__} - {str(e)}. "
                f"Trying next port..." if i < len(ports_to_try) - 1 else f"No more ports to try."
            )
            continue

    # If all ports failed
    logger.error(
        f"Tất cả cổng fallback đều thất bại. Last error: {last_error}"
    )
    return {
        "status": "error",
        "message": f"Không thể kết nối đến bất kỳ cổng nào: {str(last_error)}",
        "ports_tried": ports_to_try,
    }


async def post_json_data(
    endpoint: str,
    data: Union[Dict[str, Any], List[Dict[str, Any]]],
    token: Optional[str] = None,
    timeout: float = 30.0,
) -> Dict[str, Any]:
    """
    Gửi dữ liệu JSON đến API endpoint.

    Args:
        endpoint: URL endpoint của API
        data: Dữ liệu JSON cần gửi
        token: Token xác thực (mặc định lấy từ settings)
        timeout: Thời gian timeout tính bằng giây

    Returns:
        Dict[str, Any]: Kết quả trả về từ API
    """
    # Lazy import to avoid circular dependency
    if token is None:
        from src.settings import app_settings

        token = app_settings.xc_token

    if not endpoint.startswith("http"):
        # Nếu endpoint không có scheme, thêm base URL từ settings
        from src.settings import app_settings

        full_url = f"{app_settings.api_endpoint}/{endpoint.lstrip('/')}"
    else:
        full_url = endpoint

    headers = {"Content-Type": "application/json", "xc-token": token}

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(full_url, json=data, headers=headers)

            response.raise_for_status()
            logger.info(f"POST thành công đến {full_url}")

            try:
                return response.json()
            except json.JSONDecodeError:
                logger.warning(
                    f"Không thể parse JSON từ response. Status code: {response.status_code}"
                )
                return {"status": "success", "text": response.text}

    except httpx.HTTPStatusError as e:
        logger.error(
            f"HTTP error: {e.response.status_code} - {e.response.text}"
        )
        return {
            "status": "error",
            "status_code": e.response.status_code,
            "message": str(e),
        }
    except Exception as e:
        logger.error(f"Lỗi khi gửi request POST: {str(e)}")
        return {"status": "error", "message": str(e)}


async def get_json_data(
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
    token: Optional[str] = None,
    timeout: float = 30.0,
) -> Dict[str, Any]:
    """
    Lấy dữ liệu JSON từ API endpoint.

    Args:
        endpoint: URL endpoint của API
        params: Các tham số query string
        token: Token xác thực (mặc định lấy từ settings)
        timeout: Thời gian timeout tính bằng giây

    Returns:
        Dict[str, Any]: Dữ liệu JSON từ API
    """
    # Lazy import to avoid circular dependency
    if token is None:
        from src.settings import app_settings

        token = app_settings.xc_token

    if not endpoint.startswith("http"):
        # Nếu endpoint không có scheme, thêm base URL từ settings
        from src.settings import app_settings

        full_url = f"{app_settings.api_endpoint}/{endpoint.lstrip('/')}"
    else:
        full_url = endpoint

    headers = {"accept": "application/json", "xc-token": token}

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(
                full_url, params=params, headers=headers
            )

            response.raise_for_status()
            logger.info(f"GET thành công từ {full_url}")

            try:
                return response.json()
            except json.JSONDecodeError:
                logger.warning(
                    f"Không thể parse JSON từ response. Status code: {response.status_code}"
                )
                return {"status": "success", "text": response.text}

    except httpx.HTTPStatusError as e:
        logger.error(
            f"HTTP error: {e.response.status_code} - {e.response.text}"
        )
        return {
            "status": "error",
            "status_code": e.response.status_code,
            "message": str(e),
        }
    except Exception as e:
        logger.error(f"Lỗi khi gửi request GET: {str(e)}")
        return {"status": "error", "message": str(e)}


def post_json_data_sync_with_port_fallback(
    endpoint: str,
    data: Union[Dict[str, Any], List[Dict[str, Any]]],
    token: Optional[str] = None,
    timeout: float = 30.0,
    fallback_ports: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """
    Gửi dữ liệu JSON đến API endpoint (đồng bộ) với cơ chế fallback port.

    Nếu yêu cầu thất bại với cổng hiện tại, sẽ thử các cổng fallback khác.
    Rất hữu ích cho các dịch vụ chạy trong cụm (cluster) nơi cổng có thể thay đổi.

    Args:
        endpoint: URL endpoint của API
        data: Dữ liệu JSON cần gửi
        token: Token xác thực (mặc định lấy từ settings)
        timeout: Thời gian timeout tính bằng giây
        fallback_ports: Danh sách cổng fallback để thử [8080, 28080, 18080]

    Returns:
        Dict[str, Any]: Kết quả trả về từ API
    """
    # Lazy import to avoid circular dependency
    if token is None:
        from src.settings import app_settings
        token = app_settings.xc_token

    if not endpoint.startswith("http"):
        # Nếu endpoint không có scheme, thêm base URL từ settings
        from src.settings import app_settings
        full_url = f"{app_settings.api_endpoint}/{endpoint.lstrip('/')}"
    else:
        full_url = endpoint

    headers = {"Content-Type": "application/json"}

    # Default fallback ports from settings or parameter
    if fallback_ports is None:
        from src.settings import app_settings
        fallback_ports = app_settings.get_crm_batch_fallback_ports()

    # Extract the primary port from the URL
    parsed_url = urlparse(full_url)
    primary_port = parsed_url.port
    ports_to_try = [primary_port] + fallback_ports if primary_port else fallback_ports

    last_error = None

    for i, port in enumerate(ports_to_try):
        try:
            url_with_port = _replace_port(full_url, port)
            logger.info(f"Attempting POST to {url_with_port} (attempt {i + 1}/{len(ports_to_try)})")

            with httpx.Client(timeout=timeout) as client:
                response = client.post(url_with_port, json=data, headers=headers)
                response.raise_for_status()

                logger.info(f"POST thành công đến {url_with_port}")
                try:
                    return response.json()
                except json.JSONDecodeError:
                    logger.warning(
                        f"Không thể parse JSON từ response. Status code: {response.status_code}"
                    )
                    return {"status": "success", "text": response.text}

        except (httpx.HTTPError, httpx.ConnectError, httpx.TimeoutException) as e:
            last_error = e
            logger.warning(
                f"Request failed to port {port}: {type(e).__name__} - {str(e)}. "
                f"Trying next port..." if i < len(ports_to_try) - 1 else f"No more ports to try."
            )
            continue

    # If all ports failed
    logger.error(
        f"Tất cả cổng fallback đều thất bại. Last error: {last_error}"
    )
    return {
        "status": "error",
        "message": f"Không thể kết nối đến bất kỳ cổng nào: {str(last_error)}",
        "ports_tried": ports_to_try,
    }
