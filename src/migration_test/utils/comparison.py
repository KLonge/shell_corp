import datetime as dt

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
