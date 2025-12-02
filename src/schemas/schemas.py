from pydantic import BaseModel


class OriginDateFilter(BaseModel):
    start_date: str
    end_date: str

class YearRangeFilter(BaseModel):
    start_year: int
    end_year: int
