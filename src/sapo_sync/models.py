from datetime import datetime

from pydantic import BaseModel, field_validator


class SapoSyncRequest(BaseModel):
    startDate: str
    endDate: str

    @field_validator("startDate", "endDate")
    def validate_date_format(cls, v):
        try:
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError:
            raise ValueError(f"Ngày {v} phải có định dạng YYYY-MM-DD")
