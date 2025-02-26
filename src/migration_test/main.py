import datetime as dt
import os
from pathlib import Path

import pandas as pd

from src.clients.duckdb import DuckDBClient
from src.migration_test.models import ComparisonResult


def compare_duckdb_tables(
    duckdb_client: DuckDBClient,
    table1_name: str,
    table2_name: str,
    primary_key: list[str],
    value_tolerance: float = 0.05,
    row_tolerance: float = 0.05,
    exclude_columns: list[str] | None = None,
    timestamp_range_exclude: tuple[dt.datetime, dt.datetime] | None = None,
    trim_strings: bool = False,
) -> ComparisonResult:
    """
    Compare two tables in DuckDB and return a comparison result.

    This function compares two tables in DuckDB based on a primary key and returns
    a ComparisonResult object with information about the comparison.

    Args:
        duckdb_client: DuckDB client to use for the comparison
        table1_name: Name of the first table to compare
        table2_name: Name of the second table to compare
        primary_key: List of column names that form the primary key
        value_tolerance: Tolerance for numeric comparisons (0.05 = 5%)
        row_tolerance: Maximum percentage of rows that can fail (0.05 = 5%)
        exclude_columns: List of columns to exclude from comparison
        timestamp_range_exclude: Tuple of (start_time, end_time) to exclude from timestamp comparisons
        trim_strings: Whether to trim string values before comparison

    Returns:
        ComparisonResult: Object containing comparison results
    """
    # Get metadata for both tables
    meta1_query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{table1_name.split(".")[-1]}'
    AND table_schema = '{table1_name.split(".")[0]}'
    """

    meta2_query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{table2_name.split(".")[-1]}'
    AND table_schema = '{table2_name.split(".")[0]}'
    """

    meta1 = duckdb_client.query(meta1_query)
    meta2 = duckdb_client.query(meta2_query)

    # Convert to dictionaries for easier lookup
    meta1_dict = dict(zip(meta1["column_name"], meta1["data_type"]))
    meta2_dict = dict(zip(meta2["column_name"], meta2["data_type"]))

    print(f"meta1_dict: {meta1_dict}")
    print(f"meta2_dict: {meta2_dict}")

    # Determine columns to compare
    all_columns = set(meta1_dict.keys()) & set(meta2_dict.keys())
    if exclude_columns:
        all_columns = all_columns - set(exclude_columns)

    valid_columns = list(all_columns)

    # Build column comparisons
    column_comparisons = {}
    for col in valid_columns:
        data_type = meta1_dict[col].lower()

        if "timestamp" in data_type:
            t1_col = f"t1_{col}"
            t2_col = f"t2_{col}"

            if timestamp_range_exclude:
                start_time, end_time = timestamp_range_exclude
                column_comparisons[col] = f"""
                    ({t1_col} = {t2_col} OR ({t1_col} IS NULL AND {t2_col} IS NULL))
                    OR (
                        {t1_col} BETWEEN '{start_time}' AND '{end_time}'
                        OR {t2_col} BETWEEN '{start_time}' AND '{end_time}'
                    )
                """
            else:
                column_comparisons[col] = (
                    f"{t1_col} = {t2_col} OR ({t1_col} IS NULL AND {t2_col} IS NULL)"
                )

        elif any(
            num_type in data_type
            for num_type in ["number", "decimal", "numeric", "float", "int"]
        ):
            column_comparisons[col] = f"""
                ABS(COALESCE(t1_{col}, 0) - COALESCE(t2_{col}, 0)) <= 
                {value_tolerance} * GREATEST(ABS(COALESCE(t1_{col}, 0)), 
                ABS(COALESCE(t2_{col}, 0)), 1) 
                OR (t1_{col} IS NULL AND t2_{col} IS NULL)
            """
        elif any(
            str_type in data_type for str_type in ["string", "varchar", "char", "text"]
        ):
            t1_col = f"t1_{col}"
            t2_col = f"t2_{col}"
            if trim_strings:
                t1_col = f"TRIM({t1_col})"
                t2_col = f"TRIM({t2_col})"
            column_comparisons[col] = (
                f"{t1_col} = {t2_col} OR ({t1_col} IS NULL AND {t2_col} IS NULL)"
            )
        else:
            # For any other data types, use direct comparison
            column_comparisons[col] = (
                f"t1_{col} = t2_{col} OR (t1_{col} IS NULL AND t2_{col} IS NULL)"
            )

    # Build the comparison query
    pk_join_condition = " AND ".join([f"t1.{pk} = t2.{pk}" for pk in primary_key])

    # Select columns for the base query
    select_columns = []
    for col in primary_key:
        select_columns.append(f"t1.{col}")

    for col in valid_columns:
        select_columns.append(f"t1.{col} AS t1_{col}")
        select_columns.append(f"t2.{col} AS t2_{col}")

    select_clause = ", ".join(select_columns)

    # Build the main comparison query
    comparison_query = f"""
    WITH base_keys AS (
        SELECT {select_clause}
        FROM {table1_name} t1
        INNER JOIN {table2_name} t2
        ON {pk_join_condition}
    ),
    test_data AS (
        SELECT *,
        CASE WHEN {" AND ".join([f"({comp})" for comp in column_comparisons.values()])}
            THEN TRUE ELSE FALSE END AS row_passed
        FROM base_keys
    )
    SELECT 
        COUNT(*) AS total_rows,
        SUM(CASE WHEN NOT row_passed THEN 1 ELSE 0 END) AS failed_rows,
        {", ".join([f"SUM(CASE WHEN NOT ({comp}) THEN 1 ELSE 0 END) AS {col}_fails" for col, comp in column_comparisons.items()])}
    FROM test_data
    """

    # Execute the comparison query
    print(comparison_query)

    result = duckdb_client.query(comparison_query)

    # Extract results
    total_rows = int(result["total_rows"].iloc[0])
    failed_rows = int(result["failed_rows"].iloc[0])

    # Calculate failed columns
    failed_columns = {}
    for col in valid_columns:
        col_fails = int(result[f"{col}_fails"].iloc[0])
        if col_fails > 0:
            failed_columns[col] = col_fails

    # Calculate failed row percentage
    failed_row_perc = failed_rows / total_rows if total_rows > 0 else 0

    # Determine if the comparison passed
    passed = failed_row_perc <= row_tolerance

    # Create and return the comparison result
    return ComparisonResult(
        passed=passed,
        comparison_sql=comparison_query,
        failed_row_perc=failed_row_perc,
        total_rows=total_rows,
        failed_columns=failed_columns,
        value_tolerance=value_tolerance,
        row_tolerance=row_tolerance,
    )


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


def main(folder_path: str) -> None:
    """
    Main function to compare tables between prod and legacy_prod schemas.

    Args:
        folder_path: Path to save results
    """
    count: int = 0
    comparison_results: list[ComparisonResult] = []

    # Define the tables to test with their primary keys
    tables_to_test = {
        "derived_a": ["player_id", "contract_end_date"],
        # "derived_b": ["user_id", "event_date"],
        # "derived_c": ["session_id"],
    }

    total_model_count = len(tables_to_test)

    # Create DuckDB client
    duckdb_client = DuckDBClient(database_path=DATABASE_PATH)

    for table_name, primary_key in tables_to_test.items():
        comparison_result = compare_duckdb_tables(
            duckdb_client=duckdb_client,
            table1_name=f"prod.{table_name}",
            table2_name=f"raw.{table_name}",
            primary_key=primary_key,
            value_tolerance=0.20,
            row_tolerance=0.01,
            exclude_columns=[],
            timestamp_range_exclude=(
                dt.datetime(2024, 12, 1),
                dt.datetime(2024, 12, 31),
            ),
            trim_strings=True,
        )
        comparison_result.model_name = table_name
        comparison_results.append(comparison_result)
        count += 1
        print(f"\n{count}/{total_model_count} TABLES COMPARED")

    comparison_results_to_csv(
        folder_path=folder_path, comparison_results=comparison_results
    )

    failed_models: list[ComparisonResult] = [
        result for result in comparison_results if not result.passed
    ]

    if failed_models:
        print(f"\n⚠️ FAILED MODELS: {len(failed_models)}/{total_model_count}")

        for failed_model in failed_models:
            print(f"⚠️ {failed_model.model_name}")
            print("\n")
            print(f"📊 {failed_model.failed_row_perc * 100:.2f}% OF ROWS FAILED")
            print("\n")
            print("-" * 100)
            print("\n")
    else:
        print("\n✅ ALL MODELS PASSED")


if __name__ == "__main__":
    # Define the folder path for saving results
    FOLDER_PATH: str = "migration_test_results"
    DATABASE_PATH: Path = (
        Path(__file__).parent.parent.parent / "database/new/transferroom.duckdb"
    )

    # Run the main function
    main(folder_path=FOLDER_PATH)
