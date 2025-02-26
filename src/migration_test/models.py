from pydantic import BaseModel


class ComparisonResult(BaseModel):
    """
    Represents the result of comparing two tables.

    This class contains information about the comparison between two tables,
    including whether the comparison passed, the percentage of rows that failed,
    and which columns had differences.

    Attributes:
        model_name: Name of the model/table being compared
        passed: Whether the comparison passed based on the tolerance
        comparison_sql: SQL query used for the comparison
        failed_row_perc: Percentage of rows that failed the comparison (0.0 to 1.0)
        total_rows: Total number of rows compared
        failed_columns: Dictionary mapping column names to the number of failures
        value_tolerance: Tolerance used for numeric comparisons
        row_tolerance: Maximum percentage of rows that can fail
    """

    model_name: str | None = None
    passed: bool
    comparison_sql: str
    failed_row_perc: float
    total_rows: int
    failed_columns: dict[str, int] | None
    value_tolerance: float
    row_tolerance: float
