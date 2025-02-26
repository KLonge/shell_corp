from pydantic import BaseModel


class ComparisonResult(BaseModel):
    model_name: str | None = None
    passed: bool
    comparison_sql: str
    failed_row_perc: float
    total_rows: int
    failed_columns: dict[str, int] | None
    value_tolerance: float
    row_tolerance: float
