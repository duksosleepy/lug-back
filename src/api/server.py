import tempfile
import uuid
from enum import Enum
from pathlib import Path
from typing import Annotated

from litestar import Litestar, post
from litestar.datastructures import UploadFile
from litestar.exceptions import HTTPException
from litestar.response import FileResponse
from litestar.status_codes import HTTP_400_BAD_REQUEST

from processors.offline_processor import DaskExcelProcessor as OfflineProcessor
from processors.online_processor import DaskExcelProcessor as OnlineProcessor


class ProcessorType(str, Enum):
    ONLINE = "online"
    OFFLINE = "offline"


class ExcelAPI:
    ALLOWED_EXTENSIONS = {".xlsx", ".xls"}

    @staticmethod
    async def _save_upload_file(file: UploadFile) -> Path:
        """Lưu file tạm thời và trả về đường dẫn"""
        suffix = Path(file.filename).suffix
        if suffix not in ExcelAPI.ALLOWED_EXTENSIONS:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="Chỉ chấp nhận file Excel (.xlsx, .xls)",
            )

        temp_dir = Path(tempfile.gettempdir())
        temp_path = temp_dir / f"{uuid.uuid4()}{suffix}"

        content = await file.read()
        temp_path.write_bytes(content)
        return temp_path

    @staticmethod
    def _process_excel(
        input_path: Path, output_path: Path, processor_type: ProcessorType
    ) -> None:
        """Xử lý file Excel theo loại processor được chọn"""
        try:
            processor_class = (
                OnlineProcessor
                if processor_type == ProcessorType.ONLINE
                else OfflineProcessor
            )
            processor = processor_class(str(input_path), str(output_path))
            processor.run()
        except Exception as e:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=f"Lỗi khi xử lý file: {str(e)}",
            )

    @post("/process/{processor_type:str}")
    async def process_excel(
        self,
        processor_type: ProcessorType,
        file: Annotated[UploadFile, "Excel file to process"],
    ) -> FileResponse:
        """
        Endpoint xử lý file Excel.

        Args:
            processor_type: Loại xử lý (online/offline)
            file: File Excel cần xử lý

        Returns:
            FileResponse chứa file Excel đã xử lý
        """
        # Tạo temporary files
        input_path = await self._save_upload_file(file)
        output_path = input_path.parent / f"processed_{input_path.name}"

        try:
            # Xử lý file
            self._process_excel(input_path, output_path, processor_type)

            # Trả về file đã xử lý
            return FileResponse(
                path=output_path,
                filename=f"processed_{file.filename}",
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        finally:
            # Cleanup temporary files
            for path in [input_path, output_path]:
                if path.exists():
                    path.unlink()


app = Litestar(route_handlers=[ExcelAPI])
