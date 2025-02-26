import os

import pandas as pd

from src.migration_test.models import ComparisonResult


def comparison_results_to_csv(
    folder_path: str, comparison_results: list[ComparisonResult]
) -> tuple[str, str | None]:
    """
    Save comparison results to CSV files.

    Takes a list of comparison results and saves them to CSV files in the
    'comparison_results' directory. Creates two files:
    1. A complete results file
    2. A failures-only file (if there are any failures)

    Args:
        folder_path: Path of the folder being processed
        comparison_results: List of ComparisonResult objects containing test results

    Returns:
        tuple[str, str | None]: Tuple containing:
            - Path to the complete results CSV file
            - Path to the failures CSV file (None if no failures)
    """
    results_df = pd.DataFrame(
        [
            {
                "folder_path": folder_path,
                "model_name": result.model_name,
                "passed": result.passed,
                "failed_row_percentage": result.failed_row_perc * 100,
                "total_rows": result.total_rows,
                "failed_columns": str(result.failed_columns),
                "value_tolerance": result.value_tolerance,
                "row_tolerance": result.row_tolerance,
            }
            for result in comparison_results
        ]
    )

    base_dir = os.path.dirname(os.path.abspath(__file__))
    results_dir = os.path.join(base_dir, "comparison_results")
    failed_dir = os.path.join(results_dir, "failed")
    os.makedirs(results_dir, exist_ok=True)
    os.makedirs(failed_dir, exist_ok=True)

    csv_filename = f"{folder_path.replace('/', '__')}_test_results.csv"
    csv_path = os.path.join(results_dir, csv_filename)
    results_df.to_csv(csv_path, index=False)

    failures_df = results_df[~results_df["passed"]]
    failed_csv_path = None
    if not failures_df.empty:
        failed_csv_path = os.path.join(failed_dir, csv_filename)
        failures_df.to_csv(failed_csv_path, index=False)

    return csv_path, failed_csv_path
