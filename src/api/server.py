import io
import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

# Thiết lập logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI()

origins = [
    "http://localhost:3000",
    "http://localhost",
    "http://45.117.77.126:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # hoặc dùng ["*"] để cho phép tất cả các origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Định nghĩa các extensions được phép
ALLOWED_EXTENSIONS = {".xlsx", ".xls"}


def validate_excel_file(filename: str) -> None:
    """Kiểm tra file có phải là file Excel không."""
    suffix = Path(filename).suffix
    if suffix not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Chỉ chấp nhận file Excel (.xlsx, .xls). File của bạn có đuôi: {suffix}",
        )


async def process_excel_file(file_content: bytes, is_online: bool) -> bytes:
    """Xử lý file Excel dựa vào mode (online/offline)."""
    try:
        # Import các processor
        if is_online:
            from processors.online_processor import (
                DaskExcelProcessor as Processor,
            )
        else:
            from processors.offline_processor import (
                DaskExcelProcessor as Processor,
            )

        # Xử lý file
        input_buffer = io.BytesIO(file_content)
        output_buffer = io.BytesIO()

        processor = Processor(input_buffer)
        processor.process_to_buffer(output_buffer)

        return output_buffer.getvalue()
    except Exception as e:
        logger.error(f"Lỗi khi xử lý file: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=400, detail=f"Lỗi khi xử lý file: {str(e)}"
        )


@app.post("/process/online")
async def process_online(file: UploadFile):
    """Endpoint xử lý file theo mode online."""
    logger.info(f"Đang xử lý file online: {file.filename}")

    # Validate file
    validate_excel_file(file.filename)

    # Đọc và xử lý file
    content = await file.read()
    processed_content = await process_excel_file(content, is_online=True)

    # Trả về file đã xử lý
    return StreamingResponse(
        io.BytesIO(processed_content),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{file.filename}"'
        },
    )


@app.post("/process/offline")
async def process_offline(file: UploadFile):
    """Endpoint xử lý file theo mode offline."""
    logger.info(f"Đang xử lý file offline: {file.filename}")

    # Validate file
    validate_excel_file(file.filename)

    # Đọc và xử lý file
    content = await file.read()
    processed_content = await process_excel_file(content, is_online=False)

    # Trả về file đã xử lý
    return StreamingResponse(
        io.BytesIO(processed_content),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{file.filename}"'
        },
    )
