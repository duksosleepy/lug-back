"""
Tiện ích cho HTTP requests.
"""

import json
from typing import Any, Dict, List, Optional, Union

import httpx

from src.util.logging import get_logger

logger = get_logger(__name__)


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
