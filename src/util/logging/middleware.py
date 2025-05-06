"""
FastAPI middleware for Loguru logging
"""

import time
import traceback
from typing import Callable

from fastapi import FastAPI, Request, Response
from fastapi.routing import APIRoute
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware


class LoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware để ghi log thông tin request và response.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()

        # Log request info
        logger.info(
            f"Request started: {request.method} {request.url.path} - Client: {request.client.host}"
        )

        # Process the request and catch any exceptions
        try:
            response = await call_next(request)
            process_time = time.time() - start_time

            # Log response info
            logger.info(
                f"Request completed: {request.method} {request.url.path} - Status: {response.status_code} - Duration: {process_time:.3f}s"
            )

            return response
        except Exception as e:
            # Log exceptions not caught elsewhere
            process_time = time.time() - start_time
            logger.error(
                f"Request failed: {request.method} {request.url.path} - Error: {str(e)} - Duration: {process_time:.3f}s"
            )
            logger.error(f"Exception details: {traceback.format_exc()}")
            raise  # Re-raise the exception


class LoggingRoute(APIRoute):
    """
    Custom route class that logs request and response details.
    More detailed than the middleware for capturing API endpoint specific info.
    """

    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            req_body = await request.body()

            # Log detailed request info
            logger.debug(
                f"API request: {request.method} {request.url.path}\n"
                f"Headers: {request.headers}\n"
                f"Query params: {request.query_params}\n"
                f"Body: {req_body.decode() if req_body else None}"
            )

            start_time = time.time()

            try:
                # Execute the actual endpoint function
                response = await original_route_handler(request)

                # Calculate processing time
                process_time = time.time() - start_time

                # Log success response
                logger.debug(
                    f"API response: {request.method} {request.url.path} - Status: {response.status_code}\n"
                    f"Duration: {process_time:.3f}s"
                )

                return response
            except Exception as e:
                # Calculate processing time
                process_time = time.time() - start_time

                # Log detailed error info
                logger.exception(
                    f"API error: {request.method} {request.url.path}\n"
                    f"Duration: {process_time:.3f}s\n"
                    f"Error: {str(e)}"
                )

                raise

        return custom_route_handler


def setup_fastapi_logging(app: FastAPI) -> FastAPI:
    """
    Thiết lập logging cho ứng dụng FastAPI, bao gồm middleware và custom route.

    Args:
        app: FastAPI application instance

    Returns:
        FastAPI application với logging đã được cấu hình
    """
    # Add logging middleware
    app.add_middleware(LoggingMiddleware)

    # Use custom route class for detailed endpoint logging if needed
    app.router.route_class = LoggingRoute

    return app
