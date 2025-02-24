import logging
from enum import Enum
from typing import Annotated

from litestar import Controller, Litestar, Response, post
from litestar.datastructures import UploadFile
from litestar.enums import RequestEncodingType
from litestar.exceptions import HTTPException
from litestar.params import Body
from litestar.status_codes import HTTP_400_BAD_REQUEST

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class ProcessorType(str, Enum):
    ONLINE = "online"
    OFFLINE = "offline"


class ExcelAPI(Controller):
    path = "/"
    ALLOWED_EXTENSIONS = {".xlsx", ".xls"}

    @staticmethod
    def _validate_excel_file(filename: str) -> None:
        from pathlib import Path

        suffix = Path(filename).suffix
        if suffix not in ExcelAPI.ALLOWED_EXTENSIONS:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=f"Chỉ chấp nhận file Excel (.xlsx, .xls). File của bạn có đuôi: {suffix}",  # noqa: E501
            )

    @staticmethod
    async def _process_excel_in_memory(
        file_content: bytes, processor_type: ProcessorType
    ) -> bytes:
        import io

        from processors.offline_processor import (
            DaskExcelProcessor as OfflineProcessor,
        )
        from processors.online_processor import (
            DaskExcelProcessor as OnlineProcessor,
        )

        try:
            input_buffer = io.BytesIO(file_content)
            output_buffer = io.BytesIO()

            processor_class = (
                OnlineProcessor
                if processor_type == ProcessorType.ONLINE
                else OfflineProcessor
            )
            processor = processor_class(input_buffer)
            processor.process_to_buffer(output_buffer)
            return output_buffer.getvalue()
        except Exception as e:
            logger.error(f"Error processing file: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=f"Lỗi khi xử lý file: {str(e)}",
            )

    @post(
        path="/process/{processor_type:str}",
        status_code=200,
    )
    async def process_excel(
        self,
        processor_type: ProcessorType,
        file: Annotated[
            UploadFile,
            Body(
                media_type=RequestEncodingType.MULTI_PART,
                title="Excel file to process",
                description="Upload an Excel file (.xlsx or .xls)",
            ),
        ],
    ) -> Response:
        logger.info(f"Received request for processor_type: {processor_type}")
        logger.info(f"Received file: {file.filename if file else 'No file'}")

        if not file or not file.filename:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="Không tìm thấy file trong request",
            )

        self._validate_excel_file(file.filename)

        try:
            file_content = await file.read()
            processed_content = await self._process_excel_in_memory(
                file_content, processor_type
            )

            return Response(
                content=processed_content,
                media_type="application/vnd.ms-excel",
                headers={
                    "Content-Disposition": f'attachment; filename="{file.filename}"'  # noqa: E501
                },
            )
        except Exception as e:
            logger.error(f"Error in process_excel: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=str(e),
            )


app = Litestar(
    route_handlers=[ExcelAPI],
    request_max_body_size=20 * 1024 * 1024,  # 20MB
)
