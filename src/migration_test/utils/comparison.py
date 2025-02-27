import datetime as dt

import pandas as pd

from src.clients.duckdb import DuckDBClient
from src.migration_test.models import ComparisonResult


def _get_table_metadata(duckdb_client: DuckDBClient, table_name: str) -> dict[str, str]:
    """Get column metadata for a table.

    Retrieves column names and data types from the information schema.

    Args:
        duckdb_client: DuckDB client to use for the query
        table_name: Name of the table to get metadata for

    Returns:
        dict: Dictionary mapping column names to data types
    """
    meta_query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{table_name.split(".")[-1]}'
    AND table_schema = '{table_name.split(".")[0]}'
    """
    meta = duckdb_client.query(meta_query)
    return dict(zip(meta["column_name"], meta["data_type"]))


def _build_column_comparisons(
    valid_columns: list[str],
    meta1_dict: dict[str, str],
    value_tolerance: float,
    timestamp_range_exclude: tuple[dt.datetime, dt.datetime] | None,
    trim_strings: bool,
) -> dict[str, str]:
    """Build SQL comparison expressions for each column.

    Creates appropriate SQL expressions for comparing columns based on their data types.

    Args:
        valid_columns: List of columns to compare
        meta1_dict: Dictionary mapping column names to data types
        value_tolerance: Tolerance for numeric comparisons
        timestamp_range_exclude: Tuple of (start_time, end_time) to exclude from timestamp comparisons
        trim_strings: Whether to trim string values before comparison

    Returns:
        dict: Dictionary mapping column names to SQL comparison expressions
    """
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
            for num_type in ["number", "decimal", "numeric", "float", "int", "double"]
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

    return column_comparisons


def _get_row_counts(
    duckdb_client: DuckDBClient, table1_name: str, table2_name: str
) -> tuple[int, int]:
    """Get row counts for both tables.

    Args:
        duckdb_client: DuckDB client to use for the query
        table1_name: Name of the first table
        table2_name: Name of the second table

    Returns:
        tuple: (table1_count, table2_count)
    """
    total_rows_query = f"""
    SELECT 
        (SELECT COUNT(*) FROM {table1_name}) as table1_count,
        (SELECT COUNT(*) FROM {table2_name}) as table2_count
    """
    row_counts = duckdb_client.query(total_rows_query).iloc[0]
    return int(row_counts["table1_count"]), int(row_counts["table2_count"])


def _build_missing_rows_query(
    table_name: str, other_table_name: str, primary_key: list[str]
) -> str:
    """Build a query to find rows missing from one table.

    Args:
        table_name: Name of the table to check for rows
        other_table_name: Name of the table to check against
        primary_key: List of column names that form the primary key

    Returns:
        str: SQL query to find missing rows
    """
    pk_columns_str = ", ".join(primary_key)
    return f"""
    WITH table1_keys AS (
        SELECT {pk_columns_str} FROM {other_table_name}
    ),
    table2_keys AS (
        SELECT {pk_columns_str} FROM {table_name}
    )
    SELECT t2.* 
    FROM {table_name} t2
    WHERE NOT EXISTS (
        SELECT 1 FROM table1_keys t1
        WHERE {" AND ".join([f"t2.{pk} = t1.{pk}" for pk in primary_key])}
    )
    LIMIT 5
    """


def _print_missing_rows(
    missing_rows: pd.DataFrame,
    table_name: str,
    other_table_name: str,
    primary_key: list[str],
) -> None:
    """Print information about rows missing from a table.

    Args:
        missing_rows: DataFrame containing the missing rows
        table_name: Name of the table containing the rows
        other_table_name: Name of the table missing the rows
        primary_key: List of column names that form the primary key
    """
    print(f"\n🔍 Rows in {table_name} but missing from {other_table_name} (up to 5):")
    if missing_rows.empty:
        print("  None found")
    else:
        for idx, row in enumerate(missing_rows.itertuples()):
            print(f"\n  --- Missing Row {idx + 1} ---")
            # Print primary key values prominently
            pk_values = ", ".join([f"{pk}={getattr(row, pk)}" for pk in primary_key])
            print(f"  Primary Key: {pk_values}")

            # Print all non-primary key columns
            print("  Column values:")
            non_pk_cols = [
                col for col in missing_rows.columns if col not in primary_key
            ]
            for col in non_pk_cols:
                value = getattr(row, col)
                if pd.isna(value):
                    print(f"    {col}: NULL")
                else:
                    print(f"    {col}: {value}")


def _handle_row_count_mismatch(
    duckdb_client: DuckDBClient,
    table1_name: str,
    table2_name: str,
    primary_key: list[str],
    total_rows_table1: int,
    total_rows_table2: int,
    row_tolerance: float,
) -> tuple[bool, float, list[dict] | None, str]:
    """Handle row count mismatches between tables.

    Args:
        duckdb_client: DuckDB client to use for queries
        table1_name: Name of the first table
        table2_name: Name of the second table
        primary_key: List of column names that form the primary key
        total_rows_table1: Total rows in the first table
        total_rows_table2: Total rows in the second table
        row_tolerance: Maximum percentage of rows that can fail

    Returns:
        tuple: (passed, failed_row_perc, sample_failed_rows, comparison_sql)
    """
    row_diff = abs(total_rows_table1 - total_rows_table2)
    row_diff_perc = row_diff / max(total_rows_table1, total_rows_table2)

    print("\n⚠️ Row count mismatch between tables:")
    print(f"Table 1 ({table1_name}): {total_rows_table1:,} rows")
    print(f"Table 2 ({table2_name}): {total_rows_table2:,} rows")
    print(f"Difference: {row_diff:,} rows ({row_diff_perc:.2%})")

    # Find rows that exist in one table but not the other
    missing_from_table2_query = _build_missing_rows_query(
        table1_name, table2_name, primary_key
    )
    missing_from_table1_query = _build_missing_rows_query(
        table2_name, table1_name, primary_key
    )

    # Execute the queries separately
    sample_failed_rows = []

    try:
        missing_from_table2 = duckdb_client.query(missing_from_table2_query)
        _print_missing_rows(missing_from_table2, table1_name, table2_name, primary_key)

        # Add rows to sample_failed_rows
        if not missing_from_table2.empty:
            for row in missing_from_table2.to_dict(orient="records"):
                row_dict = row.copy()
                row_dict["diff_type"] = "missing_from_table2"
                sample_failed_rows.append(row_dict)
    except Exception as e:
        print(f"  Error querying missing rows from {table2_name}: {e!s}")

    try:
        missing_from_table1 = duckdb_client.query(missing_from_table1_query)
        _print_missing_rows(missing_from_table1, table2_name, table1_name, primary_key)

        # Add rows to sample_failed_rows
        if not missing_from_table1.empty:
            for row in missing_from_table1.to_dict(orient="records"):
                row_dict = row.copy()
                row_dict["diff_type"] = "missing_from_table1"
                sample_failed_rows.append(row_dict)
    except Exception as e:
        print(f"  Error querying missing rows from {table1_name}: {e!s}")

    passed = row_diff_perc <= row_tolerance

    if not passed:
        print(
            f"❌ Row count difference ({row_diff_perc:.2%}) exceeds tolerance ({row_tolerance:.2%})"
        )
    else:
        print(
            f"✅ Row count difference ({row_diff_perc:.2%}) within tolerance ({row_tolerance:.2%})"
        )

    comparison_sql = f"-- Missing from table2 query:\n{missing_from_table2_query}\n\n-- Missing from table1 query:\n{missing_from_table1_query}"

    return (
        passed,
        row_diff_perc,
        sample_failed_rows if sample_failed_rows else None,
        comparison_sql,
    )


def _build_comparison_query(
    table1_name: str,
    table2_name: str,
    primary_key: list[str],
    valid_columns: list[str],
    column_comparisons: dict[str, str],
) -> str:
    """Build the main comparison query.

    Args:
        table1_name: Name of the first table
        table2_name: Name of the second table
        primary_key: List of column names that form the primary key
        valid_columns: List of columns to compare
        column_comparisons: Dictionary mapping column names to SQL comparison expressions

    Returns:
        str: SQL query for comparing the tables
    """
    pk_join_condition = " AND ".join([f"t1.{pk} = t2.{pk}" for pk in primary_key])

    # Select columns for the base query
    select_columns = []
    for col in primary_key:
        select_columns.append(f"t1.{col}")

    for col in valid_columns:
        select_columns.append(f"t1.{col} AS t1_{col}")
        select_columns.append(f"t2.{col} AS t2_{col}")

    select_clause = ", ".join(select_columns)

    return f"""
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


def _build_sample_failed_rows_query(
    table1_name: str,
    table2_name: str,
    primary_key: list[str],
    valid_columns: list[str],
    column_comparisons: dict[str, str],
) -> str:
    """Build a query to get sample failed rows.

    Args:
        table1_name: Name of the first table
        table2_name: Name of the second table
        primary_key: List of column names that form the primary key
        valid_columns: List of columns to compare
        column_comparisons: Dictionary mapping column names to SQL comparison expressions

    Returns:
        str: SQL query for getting sample failed rows
    """
    pk_join_condition = " AND ".join([f"t1.{pk} = t2.{pk}" for pk in primary_key])

    # Select columns for the base query
    select_columns = []
    for col in primary_key:
        select_columns.append(f"t1.{col}")

    for col in valid_columns:
        select_columns.append(f"t1.{col} AS t1_{col}")
        select_columns.append(f"t2.{col} AS t2_{col}")

    select_clause = ", ".join(select_columns)

    # Create paired column selections for the final output
    paired_columns = []
    for col in valid_columns:
        # Only show values when they're different, otherwise null
        paired_columns.append(
            f"CASE WHEN t1_{col} = t2_{col} OR (t1_{col} IS NULL AND t2_{col} IS NULL) THEN NULL ELSE t1_{col} END AS {col}_1"
        )
        paired_columns.append(
            f"CASE WHEN t1_{col} = t2_{col} OR (t1_{col} IS NULL AND t2_{col} IS NULL) THEN NULL ELSE t2_{col} END AS {col}_2"
        )

    return f"""
    WITH base_keys AS (
        SELECT {select_clause}
        FROM {table1_name} t1
        INNER JOIN {table2_name} t2
        ON {pk_join_condition}
    ),
    test_data AS (
        SELECT *,
        CASE WHEN {" AND ".join([f"({comp})" for comp in column_comparisons.values()])}
            THEN TRUE ELSE FALSE END AS row_passed,
        {", ".join([f"CASE WHEN NOT ({comp}) THEN FALSE ELSE TRUE END AS {col}_passed" for col, comp in column_comparisons.items()])}
        FROM base_keys
    )
    SELECT 
        {", ".join([f"{pk}" for pk in primary_key])},
        {", ".join(paired_columns)},
        {", ".join([f"NOT {col}_passed AS {col}_failed" for col in valid_columns])},
        row_passed
    FROM test_data
    WHERE NOT row_passed
    ORDER BY {primary_key[0]}
    LIMIT 5
    """


def _process_sample_failed_rows(
    sample_failed_rows: list[dict], primary_key: list[str], valid_columns: list[str]
) -> None:
    """Process and print sample failed rows.

    Args:
        sample_failed_rows: List of dictionaries containing sample failed rows
        primary_key: List of column names that form the primary key
        valid_columns: List of columns to compare
    """
    print("\nShowing sample differences:")
    for i, row in enumerate(sample_failed_rows):
        print(f"\n--- Sample {i + 1} ---")
        # Print primary key values
        pk_values = ", ".join([f"{pk}={row.get(pk)}" for pk in primary_key])
        print(f"Primary Key: {pk_values}")

        # Add a list of failed columns for easier reference
        row["failed_columns"] = [
            col for col in valid_columns if row.get(f"{col}_failed", False)
        ]

        print("Failed columns:")
        for col in row["failed_columns"]:
            source_val = row.get(f"{col}_1")
            target_val = row.get(f"{col}_2")
            print(f"  - {col}: {source_val} vs {target_val}")

        # Add a dictionary of value differences for failed columns
        row["value_differences"] = {}
        for col in row["failed_columns"]:
            source_val = row.get(f"{col}_1")
            target_val = row.get(f"{col}_2")

            # Add additional information for numeric differences
            diff_info = {}
            if isinstance(source_val, int | float) and isinstance(
                target_val, int | float
            ):
                diff_info["diff"] = target_val - source_val
                diff_info["diff_pct"] = (
                    (diff_info["diff"] / source_val * 100)
                    if source_val != 0
                    else float("inf")
                )
                print(
                    f"    Numeric diff: {diff_info['diff']} ({diff_info['diff_pct']:.2f}%)"
                )

            row["value_differences"][col] = {
                "source": source_val,
                "target": target_val,
                **diff_info,
            }


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
    print(f"\nCOMPARING TABLES: {table1_name} AND {table2_name}")

    # Get metadata for both tables
    meta1_dict = _get_table_metadata(duckdb_client, table1_name)
    meta2_dict = _get_table_metadata(duckdb_client, table2_name)

    print(f"meta1_dict: {meta1_dict}")
    print(f"meta2_dict: {meta2_dict}")

    # Determine columns to compare
    all_columns = set(meta1_dict.keys()) & set(meta2_dict.keys())
    if exclude_columns:
        all_columns = all_columns - set(exclude_columns)

    valid_columns = list(all_columns)

    print(f"\nColumns to compare: {len(valid_columns)}")
    if exclude_columns:
        print(f"Columns excluded: {len(exclude_columns)}")

    # Build column comparisons
    column_comparisons = _build_column_comparisons(
        valid_columns,
        meta1_dict,
        value_tolerance,
        timestamp_range_exclude,
        trim_strings,
    )

    # Get total row counts for both tables
    total_rows_table1, total_rows_table2 = _get_row_counts(
        duckdb_client, table1_name, table2_name
    )

    # Check for row count mismatches
    if total_rows_table1 != total_rows_table2:
        passed, failed_row_perc, row_mismatch_samples, comparison_sql = (
            _handle_row_count_mismatch(
                duckdb_client,
                table1_name,
                table2_name,
                primary_key,
                total_rows_table1,
                total_rows_table2,
                row_tolerance,
            )
        )

        if not passed:
            return ComparisonResult(
                passed=False,
                comparison_sql=comparison_sql,
                failed_row_perc=failed_row_perc,
                total_rows=max(total_rows_table1, total_rows_table2),
                failed_columns={},
                value_tolerance=value_tolerance,
                row_tolerance=row_tolerance,
                sample_failed_rows=row_mismatch_samples,
            )

    # Build the main comparison query
    comparison_query = _build_comparison_query(
        table1_name, table2_name, primary_key, valid_columns, column_comparisons
    )

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

    # Print comparison results
    print("\nComparison Results:")
    print(f"Total rows compared: {total_rows:,}")
    print(f"Failed rows: {failed_rows:,} ({failed_row_perc:.2%})")
    print(f"Row tolerance: {row_tolerance:.2%}")

    if passed:
        print(
            f"\n✅ Differences ({failed_row_perc:.2%}) within acceptable row tolerance ({row_tolerance:.2%})"
        )
    else:
        print(
            f"\n❌ Differences ({failed_row_perc:.2%}) exceed row tolerance ({row_tolerance:.2%})"
        )

    if failed_columns:
        print("\nColumns with differences:")
        for col, count in failed_columns.items():
            print(f"  - {col}: {count:,} rows ({count / total_rows:.2%})")

    # If there are failed rows, get a sample of them
    value_mismatch_samples: list[dict] | None = None
    if failed_rows > 0:
        # Query to get sample failed rows
        sample_query = _build_sample_failed_rows_query(
            table1_name, table2_name, primary_key, valid_columns, column_comparisons
        )

        print("\nSample query for failed rows:")
        print(sample_query)

        sample_failed_rows_df = duckdb_client.query(sample_query)
        value_mismatch_samples = sample_failed_rows_df.to_dict(orient="records")
        print(f"Found {len(value_mismatch_samples)} sample failed rows")

        # Print sample of failed rows
        if value_mismatch_samples:
            _process_sample_failed_rows(
                value_mismatch_samples, primary_key, valid_columns
            )

    # Create and return the comparison result
    return ComparisonResult(
        passed=passed,
        comparison_sql=comparison_query,
        failed_row_perc=failed_row_perc,
        total_rows=total_rows,
        failed_columns=failed_columns,
        value_tolerance=value_tolerance,
        row_tolerance=row_tolerance,
        sample_failed_rows=value_mismatch_samples,
    )
